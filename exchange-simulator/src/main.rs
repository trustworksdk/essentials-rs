use std::process::ExitCode;

use anyhow::Context;
use exchange_simulator::app::App;
use exchange_simulator::config::{Config, LogFormat};
use exchange_simulator::scenario::{self, Scenario};
use tracing_subscriber::EnvFilter;

const USAGE: &str = "usage:
  exchange-simulator                                  run the service
  exchange-simulator scenario run <file>... [--json]  run scenario files in sandboxes; exit 0 if all pass,
                                                      1 if any fails, 2 on unreadable files or bad scenarios";

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let result = match args.as_slice() {
        [] | ["serve"] => serve().await.map(|()| ExitCode::SUCCESS),
        ["scenario", "run", rest @ ..] if !rest.is_empty() => run_scenarios(rest).await,
        ["help" | "--help" | "-h"] => {
            println!("{USAGE}");
            Ok(ExitCode::SUCCESS)
        }
        _ => {
            eprintln!("{USAGE}");
            Ok(ExitCode::from(2))
        }
    };
    result.unwrap_or_else(|error| {
        eprintln!("error: {error:#}");
        ExitCode::from(2)
    })
}

async fn serve() -> anyhow::Result<()> {
    let config = Config::load().context("loading configuration")?;
    init_tracing(&config, None);

    let app = App::start(&config)?;
    let listener = tokio::net::TcpListener::bind(&config.server.bind)
        .await
        .with_context(|| format!("binding {}", config.server.bind))?;
    tracing::info!(address = %listener.local_addr()?, "exchange simulator listening");
    axum::serve(listener, app.router.clone())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    app.shutdown().await;
    tracing::info!("shut down");
    Ok(())
}

/// Runs each scenario file in its own sandbox and prints the reports (as text, or as a JSON array with `--json`).
async fn run_scenarios(args: &[&str]) -> anyhow::Result<ExitCode> {
    let json = args.contains(&"--json");
    let files: Vec<&str> = args.iter().copied().filter(|a| *a != "--json").collect();
    let config = Config::load().context("loading configuration")?;
    // Scenario failures are in the reports; the sandboxes' own logging would drown them. RUST_LOG still wins.
    init_tracing(&config, Some("error"));
    let mut reports = Vec::new();
    let mut broken = false;
    for file in files {
        let parsed = std::fs::read_to_string(file)
            .map_err(|e| e.to_string())
            .and_then(|text| Scenario::parse(&text));
        let scenario = match parsed {
            Ok(scenario) => scenario,
            Err(error) => {
                eprintln!("{file}: {error}");
                broken = true;
                continue;
            }
        };
        let report = scenario::run(&scenario, &config)
            .await
            .with_context(|| format!("running {file}"))?;
        if !json {
            print!("{}", report.render());
        }
        reports.push(report);
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&reports)?);
    }
    Ok(if broken {
        ExitCode::from(2)
    } else if reports.iter().all(|r| r.passed) {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

/// Service logs go to stdout. The scenario CLI passes a `default_filter` and logs to stderr, since its stdout carries
/// the reports.
fn init_tracing(config: &Config, default_filter: Option<&str>) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_filter.unwrap_or(&config.logging.filter)));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match (config.logging.format, default_filter.is_some()) {
        (LogFormat::Pretty, false) => builder.init(),
        (LogFormat::Json, false) => builder.json().init(),
        (LogFormat::Pretty, true) => builder.with_writer(std::io::stderr).init(),
        (LogFormat::Json, true) => builder.json().with_writer(std::io::stderr).init(),
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
