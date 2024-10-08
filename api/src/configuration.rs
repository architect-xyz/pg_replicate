use std::fmt::Display;

use secrecy::{ExposeSecret, Secret};
use sqlx::postgres::{PgConnectOptions, PgSslMode};

#[derive(serde::Deserialize, Clone)]
pub struct EncryptionKey {
    pub id: u32,
    pub key: String,
}

#[derive(serde::Deserialize, Clone)]
pub struct Settings {
    pub database: DatabaseSettings,
    pub application: ApplicationSettings,
    pub worker: WorkerSettings,
    pub encryption_key: EncryptionKey,
}

impl Display for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "configuration:")?;
        writeln!(f, "  database:\n{}", self.database)?;
        writeln!(f, "  application:\n{}", self.application)?;
        writeln!(f, "  worker:\n{}", self.worker)
    }
}

#[derive(serde::Deserialize, Clone)]
pub struct DatabaseSettings {
    /// Host on which Postgres is running
    pub host: String,

    /// Port on which Postgres is running
    pub port: u16,

    /// Postgres database name
    pub name: String,

    /// Postgres database user name
    pub username: String,

    /// Postgres database user password
    pub password: Option<Secret<String>>,

    /// Whether to enable ssl or not
    pub require_ssl: bool,
}

impl Display for DatabaseSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "    host: {}", self.host)?;
        writeln!(f, "    port: {}", self.port)?;
        writeln!(f, "    name: {}", self.name)?;
        writeln!(f, "    username: {}", self.username)?;
        writeln!(f, "    password: REDACTED")?;
        writeln!(f, "    require_ssl: {}", self.require_ssl)
    }
}

impl DatabaseSettings {
    pub fn without_db(&self) -> PgConnectOptions {
        let ssl_mode = if self.require_ssl {
            PgSslMode::Require
        } else {
            PgSslMode::Prefer
        };
        let options = PgConnectOptions::new_without_pgpass()
            .host(&self.host)
            .username(&self.username)
            .port(self.port)
            .ssl_mode(ssl_mode);
        if let Some(password) = &self.password {
            options.password(password.expose_secret())
        } else {
            options
        }
    }

    pub fn with_db(&self) -> PgConnectOptions {
        self.without_db().database(&self.name)
    }
}

#[derive(serde::Deserialize, Clone)]
pub struct ApplicationSettings {
    /// host the api listens on
    pub host: String,

    /// port the api listens on
    pub port: u16,
}

impl Display for ApplicationSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "    host: {}", self.host)?;
        writeln!(f, "    port: {}", self.port)
    }
}

#[derive(serde::Deserialize, Clone)]
pub struct WorkerSettings {
    /// interval after which the worker looks in the queue for tasks
    pub poll_interval_secs: u64,
}

impl Display for WorkerSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "    poll_interval_secs: {}", self.poll_interval_secs)
    }
}

pub fn get_configuration() -> Result<Settings, config::ConfigError> {
    let base_path = std::env::current_dir().expect("Failed to determine the current directory");
    let configuration_directory = base_path.join("configuration");

    // Detect the running environment.
    // Default to `dev` if unspecified.
    let environment: Environment = std::env::var("APP_ENVIRONMENT")
        .unwrap_or_else(|_| DEV_ENV_NAME.into())
        .try_into()
        .expect("Failed to parse APP_ENVIRONMENT.");

    let environment_filename = format!("{}.yaml", environment.as_str());
    let settings = config::Config::builder()
        .add_source(config::File::from(
            configuration_directory.join("base.yaml"),
        ))
        .add_source(config::File::from(
            configuration_directory.join(environment_filename),
        ))
        // Add in settings from environment variables (with a prefix of APP and '__' as separator)
        // E.g. `APP_SINK__BIGQUERY__PROJECT_ID=my-project-id would set `Settings { sink: BigQuery { project_id }}` to my-project-id
        .add_source(
            config::Environment::with_prefix("APP")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;

    settings.try_deserialize::<Settings>()
}

const DEV_ENV_NAME: &str = "dev";
const PROD_ENV_NAME: &str = "prod";

/// The possible runtime environment for our application.
pub enum Environment {
    Dev,
    Prod,
}

impl Environment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Dev => DEV_ENV_NAME,
            Environment::Prod => PROD_ENV_NAME,
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "dev" => Ok(Self::Dev),
            "prod" => Ok(Self::Prod),
            other => Err(format!(
                "{other} is not a supported environment. Use either `{DEV_ENV_NAME}` or `{PROD_ENV_NAME}`.",
            )),
        }
    }
}
