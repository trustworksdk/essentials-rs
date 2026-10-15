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
use crate::config::{ClockMode, Config, SettlementMode};
use crate::engine::{Dispatcher, Exchange, ExchangeOptions, SystemClock};
use crate::persistence::{self, Store};
use crate::webhooks::DeliveryWorker;

/// A started service. Serve [`App::router`]; once serving ends, call [`App::shutdown`] so the background tasks
/// (webhook delivery, settlement ticks) stop and the exchange thread finishes the commands it already accepted.
/// Must be started inside a Tokio runtime.
pub struct App {
    pub router: Router,
    exchange_thread: JoinHandle<()>,
    background: Vec<tokio::task::JoinHandle<()>>,
    stop: watch::Sender<bool>,
}

impl App {
    pub fn start(config: &Config) -> anyhow::Result<Self> {
        let busy_timeout = Duration::from_millis(config.database.busy_timeout_ms);
        let path = &config.database.path;
        let writer = persistence::open(path, busy_timeout)
            .with_context(|| format!("opening database {path}"))?;
        let delivery_signal = Arc::new(Notify::new());
        let exchange = Exchange::recover(
            Store::new(writer),
            ExchangeOptions {
                identifier_policy: config.exchange.identifier_policy,
                ids: config.ids.scheme(),
                clock: Box::new(SystemClock),
                virtual_clock: (config.clock.mode == ClockMode::Virtual).then(|| {
                    VirtualClock::new(
                        config.clock.start.unwrap_or_else(chrono::Utc::now),
                        config.clock.running,
                    )
                }),
                simulation_id: config.events.simulation_id.clone(),
                delivery_signal: Some(delivery_signal.clone()),
                settlement: config.settlement.simulator_policy(),
                external_outcomes: config.settlement.mode != SettlementMode::Simulated,
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
            instruments = instruments.len(),
            resting_orders,
            last_sequence = exchange.last_sequence(),
            "database ready, books recovered"
        );

        let reader =
            persistence::open_reader(path, busy_timeout).context("opening read connection")?;
        let (dispatcher, exchange_thread) =
            Dispatcher::spawn(exchange, config.exchange.command_queue_capacity)
                .context("starting exchange thread")?;
        let (stop, stopped) = watch::channel(false);
        let mut background = vec![DeliveryWorker::spawn(
            dispatcher.clone(),
            delivery_signal,
            config.webhooks.worker(),
            stopped.clone(),
        )];
        if config.settlement.simulator_policy().is_some() {
            let interval = Duration::from_millis(config.settlement.tick_interval_ms.max(1));
            background.push(spawn_settlement_ticker(
                dispatcher.clone(),
                interval,
                stopped,
            ));
            tracing::info!(mode = ?config.settlement.mode, "settlement simulator running");
        }
        let state = AppState::new(dispatcher, Store::new(reader), crate::metrics::install());
        let mut router = api::router(state.clone());
        if config.test_control.enabled {
            tracing::warn!(
                "test-control API enabled under /test; never expose this outside test deployments"
            );
            router = router.merge(api::test_control::router(state, Arc::new(config.clone())));
        }
        Ok(Self {
            router,
            exchange_thread,
            background,
            stop,
        })
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

/// Runs a settlement step every `interval` until `stop` turns true or the exchange stops. A full command queue just
/// skips a beat.
fn spawn_settlement_ticker(
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
            match dispatcher.settlement_tick().await {
                Ok(_) | Err(crate::engine::DispatchError::Overloaded) => {}
                Err(crate::engine::DispatchError::Stopped) => break,
                Err(error) => tracing::error!(%error, "settlement step failed; retrying next tick"),
            }
        }
    })
}
