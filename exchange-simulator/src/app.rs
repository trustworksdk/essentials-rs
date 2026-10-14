//! Assembles the service from configuration: database, recovered exchange, exchange thread and HTTP router.

use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::Context;
use axum::Router;

use crate::api::{self, AppState};
use crate::config::Config;
use crate::engine::{Dispatcher, Exchange, ExchangeOptions, SystemClock, UuidV7Ids};
use crate::persistence::{self, Store};

/// A started service. Serve [`App::router`]; once serving ends, drop the router and call [`App::shutdown`] so the
/// exchange thread finishes the commands it already accepted.
pub struct App {
    pub router: Router,
    exchange_thread: JoinHandle<()>,
}

impl App {
    pub fn start(config: &Config) -> anyhow::Result<Self> {
        let busy_timeout = Duration::from_millis(config.database.busy_timeout_ms);
        let path = &config.database.path;
        let writer = persistence::open(path, busy_timeout)
            .with_context(|| format!("opening database {path}"))?;
        let exchange = Exchange::recover(
            Store::new(writer),
            ExchangeOptions {
                identifier_policy: config.exchange.identifier_policy,
                ids: Box::new(UuidV7Ids),
                clock: Box::new(SystemClock),
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
        let router = api::router(AppState::new(dispatcher, Store::new(reader)));
        Ok(Self {
            router,
            exchange_thread,
        })
    }

    /// Drops the router (the last command senders) and waits for the exchange thread to drain its queue.
    pub fn shutdown(self) {
        drop(self.router);
        if self.exchange_thread.join().is_err() {
            tracing::error!("exchange thread panicked");
        }
    }
}
