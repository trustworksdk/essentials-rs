//! Application configuration: compiled-in defaults, an optional YAML file, then environment overrides.

use serde::Deserialize;

const DEFAULTS: &str = include_str!("../config/default.yaml");

/// Environment variable naming an optional YAML file layered over the defaults.
pub const CONFIG_FILE_ENV: &str = "EXCHANGE_CONFIG";

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DatabaseConfig {
    pub path: String,
    pub busy_timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct LoggingConfig {
    pub filter: String,
    pub format: LogFormat,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Pretty,
    Json,
}

impl Config {
    /// Loads the defaults, the file named by [`CONFIG_FILE_ENV`] if set, and `EXCHANGE__*` environment variables.
    pub fn load() -> Result<Self, config::ConfigError> {
        let mut builder = config::Config::builder()
            .add_source(config::File::from_str(DEFAULTS, config::FileFormat::Yaml));
        if let Ok(path) = std::env::var(CONFIG_FILE_ENV) {
            builder = builder.add_source(config::File::with_name(&path));
        }
        builder
            .add_source(
                config::Environment::with_prefix("EXCHANGE")
                    .prefix_separator("__")
                    .separator("__"),
            )
            .build()?
            .try_deserialize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_parse() {
        let config: Config = config::Config::builder()
            .add_source(config::File::from_str(DEFAULTS, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        assert_eq!(config.server.bind, "127.0.0.1:8080");
        assert_eq!(config.logging.format, LogFormat::Pretty);
    }
}
