use anyhow::Context;
use exchange_simulator::app::App;
use exchange_simulator::config::{Config, LogFormat};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load().context("loading configuration")?;
    init_tracing(&config);

    let app = App::start(&config)?;
    let listener = tokio::net::TcpListener::bind(&config.server.bind)
        .await
        .with_context(|| format!("binding {}", config.server.bind))?;
    tracing::info!(address = %listener.local_addr()?, "exchange simulator listening");
    axum::serve(listener, app.router.clone())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    tokio::task::spawn_blocking(move || app.shutdown()).await?;
    tracing::info!("shut down");
    Ok(())
}

fn init_tracing(config: &Config) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.logging.filter));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match config.logging.format {
        LogFormat::Pretty => builder.init(),
        LogFormat::Json => builder.json().init(),
    }
}

/// Resolves on Ctrl-C or SIGTERM (what `docker stop` sends).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
