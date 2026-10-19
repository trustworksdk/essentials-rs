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
        let follow = match (config.clock.mode, &config.clock.follow) {
            (ClockMode::Follow, Some(follow)) => {
                reqwest::Url::parse(&follow.url).context("clock.follow.url")?;
                Some(follow.clone())
            }
            (ClockMode::Follow, None) => anyhow::bail!("clock.mode: follow needs clock.follow.url"),
            _ => None,
        };
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
                // A following clock starts frozen at real-time speed; the first read of the followed clock sets it.
                virtual_clock: match config.clock.mode {
                    ClockMode::System => None,
                    ClockMode::Virtual => Some(VirtualClock::with_speed(
                        config.clock.start.unwrap_or_else(chrono::Utc::now),
                        config.clock.running,
                        config.clock.speed,
                    )),
                    ClockMode::Follow => Some(VirtualClock::new(
                        config.clock.start.unwrap_or_else(chrono::Utc::now),
                        false,
                    )),
                },
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
                follow: follow.is_some(),
            },
        )
        .context("recovering the gateway")?;
        tracing::info!(location = %location, mode = mode.as_str(), "database ready");
        let reader =
            persistence::open_reader(&location, busy_timeout).context("opening read connection")?;
        let writer = Writer::new(gateway);
        let (stop, stopped) = watch::channel(false);
        let mut background = vec![
            DeliveryWorker::spawn(
                writer.clone(),
                delivery_signal,
                config.webhooks.worker(),
                stopped.clone(),
            ),
            spawn_ticker(
                writer.clone(),
                Duration::from_millis(config.statement.tick_interval_ms.max(1)),
                stopped.clone(),
            ),
        ];
        if let Some(follow) = follow {
            tracing::info!(url = %follow.url, "the clock follows another simulator's");
            background.push(spawn_follower(writer.clone(), follow, stopped));
        }
        let state = AppState::new(writer, reader, metrics, mode);
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

/// Follows another simulator's clock: reads its `GET /api/clock` every `poll_interval_ms`, at once on start, and
/// brings the gateway's clock to it ([`Gateway::follow`]). The reading is corrected by half the request's round trip
/// at the followed speed. While the followed simulator does not answer, the clock runs on at its last speed and a
/// warning is logged once.
fn spawn_follower(
    writer: Writer,
    follow: simulator_kit::clock::FollowConfig,
    mut stop: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    #[derive(serde::Deserialize)]
    struct Remote {
        running: bool,
        speed: f64,
        now: chrono::DateTime<chrono::Utc>,
    }
    let url = format!("{}/api/clock", follow.url.trim_end_matches('/'));
    let interval = Duration::from_millis(follow.poll_interval_ms.max(10));
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut failing = false;
        loop {
            let sent = std::time::Instant::now();
            let read = async {
                client
                    .get(&url)
                    .timeout(Duration::from_secs(5))
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Remote>()
                    .await
            }
            .await;
            match read {
                Ok(remote) => {
                    if failing {
                        tracing::info!(url = %url, "following the clock again");
                        failing = false;
                    }
                    let latency = sent.elapsed() / 2;
                    let ahead = if remote.running {
                        chrono::Duration::from_std(latency.mul_f64(remote.speed))
                            .unwrap_or_default()
                    } else {
                        chrono::Duration::zero()
                    };
                    let remote = crate::gateway::RemoteClock {
                        running: remote.running,
                        speed: remote.speed,
                        now: remote.now + ahead,
                    };
                    if let Err(error) = writer.run(move |g| g.follow(remote)).await {
                        tracing::error!(%error, "following the clock failed");
                    }
                }
                Err(error) if !failing => {
                    failing = true;
                    tracing::warn!(url = %url, %error, "cannot read the followed clock; running on at the last speed");
                }
                Err(_) => {}
            }
            tokio::select! {
                _ = stop.changed() => break,
                () = tokio::time::sleep(interval) => {}
            }
            if *stop.borrow() {
                break;
            }
        }
    })
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
