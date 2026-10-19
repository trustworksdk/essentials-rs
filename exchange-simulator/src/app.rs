//! Assembles the service from configuration: database, recovered exchange, exchange thread, webhook delivery worker
//! and HTTP router.

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use tokio::sync::{Notify, watch};

use crate::api::{self, AppState};
use crate::clock::VirtualClock;
use crate::config::{CheckpointMode, ClockMode, Config, PersistenceMode, SettlementMode};
use crate::engine::{Dispatcher, Exchange, ExchangeOptions, SystemClock};
use crate::persistence::{self, Store};
use crate::webhooks::DeliveryWorker;

/// A started service. Serve [`App::router`]; once serving ends, call [`App::shutdown`] so the background tasks
/// (webhook delivery, settlement ticks) stop and the exchange thread finishes the commands it already accepted.
/// Must be started inside a Tokio runtime.
pub struct App {
    pub router: Router,
    /// Where the database is: the configured file, or the name of the in-memory database.
    location: String,
    exchange_thread: JoinHandle<()>,
    background: Vec<tokio::task::JoinHandle<()>>,
    stop: watch::Sender<bool>,
}

impl App {
    pub fn start(config: &Config) -> anyhow::Result<Self> {
        crate::clock::validate_speed(config.clock.speed).context("clock.speed")?;
        let clearing = config.clearing_policy().map_err(anyhow::Error::msg)?;
        if config.clock.mode == ClockMode::Follow {
            anyhow::bail!(
                "clock.mode: follow is for the payment gateway; the exchange keeps its own clock"
            );
        }
        if config.clock.mode == ClockMode::System && config.clock.speed != 1.0 {
            anyhow::bail!(
                "clock.speed needs clock.mode: virtual; the system clock runs at real time"
            );
        }
        let busy_timeout = Duration::from_millis(config.database.busy_timeout_ms);
        let mode = config.database.mode;
        let location = match mode {
            PersistenceMode::Memory => persistence::memory_location(),
            PersistenceMode::Durable | PersistenceMode::Fast => config.database.path.clone(),
        };
        let path = &location;
        let writer = persistence::open_mode(
            path,
            mode,
            busy_timeout,
            config.database.memory_max_mib.saturating_mul(1 << 20),
        )
        .with_context(|| format!("opening database {path}"))?;
        match mode {
            PersistenceMode::Durable => {}
            PersistenceMode::Fast => tracing::warn!(
                "persistence mode fast: commits are synced to disk only at checkpoints; a host crash or power loss \
                 can lose the last ones"
            ),
            PersistenceMode::Memory => tracing::warn!(
                "persistence mode memory: all state (orders, trades, settlements, webhooks) is lost when the \
                 process stops"
            ),
        }
        // Install the recorder first: anything recorded before it is lost.
        let metrics = crate::metrics::install();
        crate::metrics::set_persistence_mode(mode);
        let delivery_signal = Arc::new(Notify::new());
        let mut store = Store::new(writer);
        if config.database.checkpoint == CheckpointMode::Background
            && mode != PersistenceMode::Memory
        {
            store
                .checkpoint_in_background(busy_timeout, config.database.checkpoint_pages)
                .context("starting background checkpoints")?;
        }
        let exchange = Exchange::recover(
            store,
            ExchangeOptions {
                identifier_policy: config.exchange.identifier_policy,
                ids: config.ids.scheme(),
                clock: Box::new(SystemClock),
                virtual_clock: (config.clock.mode == ClockMode::Virtual).then(|| {
                    VirtualClock::with_speed(
                        config.clock.start.unwrap_or_else(chrono::Utc::now),
                        config.clock.running,
                        config.clock.speed,
                    )
                }),
                simulation_id: config.events.simulation_id.clone(),
                delivery_signal: Some(delivery_signal.clone()),
                settlement: config.settlement.simulator_policy().map(|mut policy| {
                    policy.clearing = clearing.as_ref().map(|c| c.calendar);
                    policy
                }),
                clearing: clearing.clone(),
                external_outcomes: config.settlement.mode != SettlementMode::Simulated,
                webhooks: config
                    .webhooks
                    .declared()
                    .context("reading webhooks.subscriptions")?,
                webhook_secret_grace: chrono::Duration::seconds(
                    i64::try_from(config.webhooks.secret_grace_seconds).unwrap_or(i64::MAX / 1000),
                ),
                instruments: config.declared_instruments().map_err(anyhow::Error::msg)?,
                price_sources: config.price_sources.sources().map_err(anyhow::Error::msg)?,
                business_days: Some(config.business_calendar().map_err(anyhow::Error::msg)?),
                liquidity: Some(config.liquidity.policy().map_err(anyhow::Error::msg)?),
            },
        )
        .context("recovering order books")?;
        let instruments = exchange.store().instruments(&Default::default())?;
        let resting_orders: usize = instruments
            .iter()
            .filter_map(|i| exchange.book(&i.id))
            .map(|b| b.len())
            .sum();
        tracing::info!(
            path = %path,
            mode = mode.as_str(),
            instruments = instruments.len(),
            resting_orders,
            last_sequence = exchange.last_sequence(),
            "database ready, books recovered"
        );

        let reader =
            persistence::open_reader(path, busy_timeout).context("opening read connection")?;
        let (dispatcher, exchange_thread) = Dispatcher::spawn(
            exchange,
            config.exchange.command_queue_capacity,
            config.exchange.group_commit.policy(),
        )
        .context("starting exchange thread")?;
        let (stop, stopped) = watch::channel(false);
        let mut background = vec![DeliveryWorker::spawn(
            dispatcher.clone(),
            delivery_signal,
            config.webhooks.worker(),
            stopped.clone(),
        )];
        // Business days close on domain time whatever else runs, so the ticker always does.
        let interval = Duration::from_millis(config.settlement.tick_interval_ms.max(1));
        background.push(spawn_domain_ticker(dispatcher.clone(), interval, stopped));
        if config.settlement.simulator_policy().is_some() {
            tracing::info!(mode = ?config.settlement.mode, "settlement simulator running");
        }
        let state = AppState::new(dispatcher, Store::new(reader), metrics, mode);
        let mut router = api::router(state.clone()).merge(simulator_kit::api::clock::router(
            state.clone(),
            config.clock.operator_control,
        ));
        if config.clock.operator_control {
            tracing::info!("clock operator control enabled under /api/clock");
        }
        if config.test_control.enabled {
            tracing::warn!(
                "test-control API enabled under /test; never expose this outside test deployments"
            );
            router = router.merge(api::test_control::router(state, Arc::new(config.clone())));
        }
        let router = api::mark_persistence(router, mode);
        Ok(Self {
            location,
            router,
            exchange_thread,
            background,
            stop,
        })
    }

    /// The database's location: its file path, or in `memory` mode a SQLite URI that other connections in this process
    /// can open while the service runs.
    pub fn database_location(&self) -> &str {
        &self.location
    }

    /// Stops the delivery worker (after its current batch), drops the router (the last command senders) and waits for
    /// the exchange thread to drain its queue.
    pub async fn shutdown(self) {
        let _ = self.stop.send(true);
        for task in self.background {
            if let Err(error) = task.await {
                tracing::error!(%error, "background task failed");
            }
        }
        drop(self.router);
        let thread = self.exchange_thread;
        if !matches!(
            tokio::task::spawn_blocking(move || thread.join()).await,
            Ok(Ok(()))
        ) {
            tracing::error!("exchange thread panicked");
        }
    }
}

/// Runs everything due on domain time (settlement, business days, clearing) every `interval` until `stop` turns true or the exchange stops. A full command queue just
/// skips a beat.
fn spawn_domain_ticker(
    dispatcher: crate::engine::Dispatcher,
    interval: Duration,
    mut stop: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stop.changed() => break,
                () = tokio::time::sleep(interval) => {}
            }
            if *stop.borrow() {
                break;
            }
            match dispatcher.tick().await {
                Ok(_) | Err(crate::engine::DispatchError::Overloaded) => {}
                Err(crate::engine::DispatchError::Stopped) => break,
                Err(error) => {
                    tracing::error!(%error, "settlement or clearing step failed; retrying next tick")
                }
            }
        }
    })
}
