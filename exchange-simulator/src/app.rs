//! Assembles the service from configuration: database, recovered exchange, exchange thread, webhook delivery worker
//! and HTTP router.

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use tokio::sync::{Notify, watch};

use crate::api::{self, AppState};
use crate::config::Config;
use crate::engine::{Dispatcher, Exchange, ExchangeOptions, SystemClock, UuidV7Ids};
use crate::persistence::{self, Store};
use crate::webhooks::DeliveryWorker;

/// A started service. Serve [`App::router`]; once serving ends, call [`App::shutdown`] so the delivery worker stops
/// and the exchange thread finishes the commands it already accepted. Must be started inside a Tokio runtime.
pub struct App {
    pub router: Router,
    exchange_thread: JoinHandle<()>,
    delivery_worker: tokio::task::JoinHandle<()>,
    stop_delivery: watch::Sender<bool>,
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
                ids: Box::new(UuidV7Ids),
                clock: Box::new(SystemClock),
                simulation_id: config.events.simulation_id.clone(),
                delivery_signal: Some(delivery_signal.clone()),
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
        let (stop_delivery, stopped) = watch::channel(false);
        let delivery_worker = DeliveryWorker::spawn(
            dispatcher.clone(),
            delivery_signal,
            config.webhooks.worker(),
            stopped,
        );
        let router = api::router(AppState::new(dispatcher, Store::new(reader)));
        Ok(Self {
            router,
            exchange_thread,
            delivery_worker,
            stop_delivery,
        })
    }

    /// Stops the delivery worker (after its current batch), drops the router (the last command senders) and waits for
    /// the exchange thread to drain its queue.
    pub async fn shutdown(self) {
        let _ = self.stop_delivery.send(true);
        if let Err(error) = self.delivery_worker.await {
            tracing::error!(%error, "webhook delivery worker failed");
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
