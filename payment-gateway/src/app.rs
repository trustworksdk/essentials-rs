//! Assembles the service from configuration: database, recovered gateway, webhook delivery worker, ticker and router.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use simulator_kit::clock::{ClockMode, SystemClock, VirtualClock};
use simulator_kit::persistence::{self, PersistenceMode};
use simulator_kit::webhooks::DeliveryWorker;
use tokio::sync::{Notify, watch};

use crate::api::{self, AppState, Writer};
use crate::config::Config;
use crate::gateway::{Gateway, GatewayOptions};
use crate::store::MIGRATIONS;

/// A started service. Serve [`App::router`]; once serving ends, call [`App::shutdown`]. Must be started inside a
/// Tokio runtime.
pub struct App {
    pub router: Router,
    location: String,
    background: Vec<tokio::task::JoinHandle<()>>,
    stop: watch::Sender<bool>,
}

impl App {
    pub fn start(config: &Config) -> anyhow::Result<Self> {
        simulator_kit::clock::validate_speed(config.clock.speed).context("clock.speed")?;
        if config.clock.mode == ClockMode::System && config.clock.speed != 1.0 {
            anyhow::bail!(
                "clock.speed needs clock.mode: virtual; the system clock runs at real time"
            );
        }
        let currencies = config.currencies().map_err(anyhow::Error::msg)?;
        let payments = config.payment_policy().map_err(anyhow::Error::msg)?;
        let statements = config.statement_policy().map_err(anyhow::Error::msg)?;
        let busy_timeout = Duration::from_millis(config.database.busy_timeout_ms);
        let mode = config.database.mode;
        let location = match mode {
            PersistenceMode::Memory => persistence::memory_location("payment-gateway"),
            PersistenceMode::Durable | PersistenceMode::Fast => config.database.path.clone(),
        };
        let writer = persistence::open_mode(
            &location,
            mode,
            busy_timeout,
            config.database.memory_max_mib.saturating_mul(1 << 20),
            MIGRATIONS,
        )
        .with_context(|| format!("opening database {location}"))?;
        if mode != PersistenceMode::Durable {
            tracing::warn!(mode = mode.as_str(), "persistence mode is not durable");
        }
        let metrics = crate::metrics::install();
        crate::metrics::set_persistence_mode(mode);
        let delivery_signal = Arc::new(Notify::new());
        let gateway = Gateway::recover(
            writer,
            GatewayOptions {
                ids: config.ids.scheme(),
                clock: Box::new(SystemClock),
                virtual_clock: (config.clock.mode == ClockMode::Virtual).then(|| {
                    VirtualClock::with_speed(
                        config.clock.start.unwrap_or_else(chrono::Utc::now),
                        config.clock.running,
                        config.clock.speed,
                    )
                }),
                currencies,
                payments,
                statements,
                simulation_id: config.events.simulation_id.clone(),
                delivery_signal: Some(delivery_signal.clone()),
                webhooks: config
                    .webhooks
                    .declared()
                    .context("reading webhooks.subscriptions")?,
                webhook_secret_grace: chrono::Duration::seconds(
                    i64::try_from(config.webhooks.secret_grace_seconds).unwrap_or(i64::MAX / 1000),
                ),
            },
        )
        .context("recovering the gateway")?;
        tracing::info!(location = %location, mode = mode.as_str(), "database ready");
        let reader =
            persistence::open_reader(&location, busy_timeout).context("opening read connection")?;
        let writer = Writer::new(gateway);
        let (stop, stopped) = watch::channel(false);
        let background = vec![
            DeliveryWorker::spawn(
                writer.clone(),
                delivery_signal,
                config.webhooks.worker(),
                stopped.clone(),
            ),
            spawn_ticker(
                writer.clone(),
                Duration::from_millis(config.statement.tick_interval_ms.max(1)),
                stopped,
            ),
        ];
        let state = AppState::new(writer, reader, metrics, mode);
        let mut router = api::router(state.clone());
        if config.test_control.enabled {
            tracing::warn!(
                "test-control API enabled under /test; never expose this outside test deployments"
            );
            router = router.merge(api::test_router(state));
        }
        Ok(Self {
            router,
            location,
            background,
            stop,
        })
    }

    /// The database's location: its file path, or in `memory` mode a SQLite URI.
    pub fn database_location(&self) -> &str {
        &self.location
    }

    /// Stops the background tasks (the delivery worker after its current batch).
    pub async fn shutdown(self) {
        let _ = self.stop.send(true);
        for task in self.background {
            if let Err(error) = task.await {
                tracing::error!(%error, "background task failed");
            }
        }
    }
}

/// Runs everything due on domain time (payment transitions, statements) every `interval` until `stop` turns true.
fn spawn_ticker(
    writer: Writer,
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
            if let Err(error) = writer.run(Gateway::tick).await {
                tracing::error!(%error, "payment or statement step failed; retrying next tick");
            }
        }
    })
}
