use aws_lc_rs::{aead::Nonce, error::Unspecified};
use base64::{prelude::BASE64_STANDARD, DecodeError, Engine};
use sqlx::PgPool;
use std::{
    fmt::{Debug, Formatter},
    str::{from_utf8, Utf8Error},
};
use thiserror::Error;

use crate::encryption::{decrypt, encrypt, EncryptedValue, EncryptionKey};

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SinkConfig {
    BigQuery {
        /// BigQuery project id
        project_id: String,

        /// BigQuery dataset id
        dataset_id: String,

        /// BigQuery service account key
        service_account_key: String,
    },
}

impl SinkConfig {
    fn into_db_config(self, encryption_key: &EncryptionKey) -> Result<SinkConfigInDb, Unspecified> {
        let SinkConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key,
        } = self;

        let (encrypted_sa_key, nonce) =
            encrypt(service_account_key.as_bytes(), &encryption_key.key)?;
        let encrypted_encoded_sa_key = BASE64_STANDARD.encode(encrypted_sa_key);
        let encoded_nonce = BASE64_STANDARD.encode(nonce.as_ref());
        let encrypted_sa_key = EncryptedValue {
            id: encryption_key.id,
            nonce: encoded_nonce,
            value: encrypted_encoded_sa_key,
        };

        Ok(SinkConfigInDb::BigQuery {
            project_id,
            dataset_id,
            service_account_key: encrypted_sa_key,
        })
    }
}

impl Debug for SinkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key: _,
            } => f
                .debug_struct("BigQuery")
                .field("project_id", project_id)
                .field("dataset_id", dataset_id)
                .field("service_account_key", &"REDACTED")
                .finish(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SinkConfigInDb {
    BigQuery {
        /// BigQuery project id
        project_id: String,

        /// BigQuery dataset id
        dataset_id: String,

        /// BigQuery service account key
        service_account_key: EncryptedValue,
    },
}

impl SinkConfigInDb {
    fn into_config(self, encryption_key: &EncryptionKey) -> Result<SinkConfig, SinksDbError> {
        let SinkConfigInDb::BigQuery {
            project_id,
            dataset_id,
            service_account_key: encrypted_sa_key,
        } = self;

        if encrypted_sa_key.id != encryption_key.id {
            return Err(SinksDbError::MismatchedKeyId(
                encrypted_sa_key.id,
                encryption_key.id,
            ));
        }

        let encrypted_sa_key_bytes = BASE64_STANDARD.decode(encrypted_sa_key.value)?;
        let nonce =
            Nonce::try_assume_unique_for_key(&BASE64_STANDARD.decode(encrypted_sa_key.nonce)?)?;
        let decrypted_sa_key = from_utf8(&decrypt(
            encrypted_sa_key_bytes,
            nonce,
            &encryption_key.key,
        )?)?
        .to_string();

        Ok(SinkConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key: decrypted_sa_key,
        })
    }
}

#[derive(Debug, Error)]
pub enum SinksDbError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("encryption error: {0}")]
    Encryption(#[from] Unspecified),

    #[error("invalid source config in db")]
    InvalidConfig(#[from] serde_json::Error),

    #[error("mismatched key id. Expected: {0}, actual: {1}")]
    MismatchedKeyId(u32, u32),

    #[error("base64 decode error: {0}")]
    Base64Decode(#[from] DecodeError),

    #[error("utf8 error: {0}")]
    Utf8(#[from] Utf8Error),
}

pub struct Sink {
    pub id: i64,
    pub tenant_id: String,
    pub name: String,
    pub config: SinkConfig,
}

pub async fn create_sink(
    pool: &PgPool,
    tenant_id: &str,
    name: &str,
    config: SinkConfig,
    encryption_key: &EncryptionKey,
) -> Result<i64, SinksDbError> {
    let db_config = config.into_db_config(encryption_key)?;
    let db_config = serde_json::to_value(db_config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        insert into app.sinks (tenant_id, name, config)
        values ($1, $2, $3)
        returning id
        "#,
        tenant_id,
        name,
        db_config
    )
    .fetch_one(pool)
    .await?;

    Ok(record.id)
}

pub async fn read_sink(
    pool: &PgPool,
    tenant_id: &str,
    sink_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<Option<Sink>, SinksDbError> {
    let record = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.sinks
        where tenant_id = $1 and id = $2
        "#,
        tenant_id,
        sink_id,
    )
    .fetch_optional(pool)
    .await?;

    let sink = record
        .map(|r| {
            let config: SinkConfigInDb = serde_json::from_value(r.config)?;
            let config = config.into_config(encryption_key)?;
            let source = Sink {
                id: r.id,
                tenant_id: r.tenant_id,
                name: r.name,
                config,
            };
            Ok::<Sink, SinksDbError>(source)
        })
        .transpose()?;
    Ok(sink)
}

pub async fn update_sink(
    pool: &PgPool,
    tenant_id: &str,
    name: &str,
    sink_id: i64,
    config: SinkConfig,
    encryption_key: &EncryptionKey,
) -> Result<Option<i64>, SinksDbError> {
    let db_config = config.into_db_config(encryption_key)?;
    let db_config = serde_json::to_value(db_config).expect("failed to serialize config");
    let record = sqlx::query!(
        r#"
        update app.sinks
        set config = $1, name = $2
        where tenant_id = $3 and id = $4
        returning id
        "#,
        db_config,
        name,
        tenant_id,
        sink_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_sink(
    pool: &PgPool,
    tenant_id: &str,
    sink_id: i64,
) -> Result<Option<i64>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        delete from app.sinks
        where tenant_id = $1 and id = $2
        returning id
        "#,
        tenant_id,
        sink_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_sinks(
    pool: &PgPool,
    tenant_id: &str,
    encryption_key: &EncryptionKey,
) -> Result<Vec<Sink>, SinksDbError> {
    let records = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.sinks
        where tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(pool)
    .await?;

    let mut sinks = Vec::with_capacity(records.len());
    for record in records {
        let config: SinkConfigInDb = serde_json::from_value(record.config)?;
        let config = config.into_config(encryption_key)?;
        let source = Sink {
            id: record.id,
            tenant_id: record.tenant_id,
            name: record.name,
            config,
        };
        sinks.push(source);
    }

    Ok(sinks)
}

pub async fn sink_exists(
    pool: &PgPool,
    tenant_id: &str,
    sink_id: i64,
) -> Result<bool, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select exists (select id
        from app.sinks
        where tenant_id = $1 and id = $2) as "exists!"
        "#,
        tenant_id,
        sink_id,
    )
    .fetch_one(pool)
    .await?;

    Ok(record.exists)
}
