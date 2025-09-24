use std::{io::Write, sync::Arc};

use crate::error::Error;
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use once_cell::sync::Lazy;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
// use arrow2::{
//     array::Array,
//     chunk::Chunk,
//     datatypes::Schema,
//     io::parquet::write::{
//         CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version, WriteOptions,
//     },
// };
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use opendal::{services, Operator};
use reqsign::{AwsCredential, AwsCredentialLoad};
use reqwest::Client;
use serde::Deserialize;
use tokio::runtime::Runtime;

use super::encryption::Encryptor;

// Lazy static for Tokio runtime
static TOKIO_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

// static TOKIO_RUNTIME_MT: Lazy<Runtime> = Lazy::new(|| {
//     tokio::runtime::Builder::new_multi_thread()
//         .enable_all()
//         .build()
//         .expect("Failed to create Tokio runtime")
// });

// Main parquet writer function, should write to anything that implements std::io::Write
pub(crate) fn write_parquet<T>(
    target: &mut T,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
) -> Result<(), Error>
where
    T: std::io::Write + Send,
{
    let options = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_1_0)
        .build();
    let mut writer = ArrowWriter::try_new(target, schema, Some(options))?;
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(())
}

pub trait ParquetWriterBackend {
    fn write_parquet(
        &self,
        file_name: &str,
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> Result<(), Error>;
}

pub struct FileWriterBackend {
    // Optional encryption key
    encryptor: Option<Encryptor>,
}

impl FileWriterBackend {
    pub fn new(encryption_key: Option<String>) -> Result<Self, Error> {
        match encryption_key {
            Some(key) => {
                let encryptor = TOKIO_RUNTIME.block_on(Encryptor::new(key.as_bytes(), None))?;
                Ok(Self {
                    encryptor: Some(encryptor),
                })
            }
            None => Ok(Self { encryptor: None }),
        }
    }
}

impl ParquetWriterBackend for FileWriterBackend {
    /**
     * Write parquet file to disk
     */
    fn write_parquet(
        &self,
        file_name: &str,
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> Result<(), Error> {
        let mut file = std::fs::File::create(&file_name)?;
        // Optional encryption here
        if let Some(encryptor) = &self.encryptor {
            let mut buffer: Vec<u8> = Vec::with_capacity(1000);
            write_parquet(&mut buffer, schema, batches)?;
            let encrypted = TOKIO_RUNTIME.block_on(encryptor.encrypt_in_place(&buffer))?;
            Ok(file.write_all(&encrypted)?)
        } else {
            write_parquet(&mut file, schema, batches)
        }
    }
}

pub struct S3WriterBackend {
    op: Operator,
    encryptor: Option<Encryptor>,
}

struct EcsCredentialLoader {
    ecs_uri: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ECSAwsCredential {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(rename = "Token")]
    pub session_token: Option<String>,
    #[serde(rename = "Expiration")]
    pub expires_in: Option<DateTime<Utc>>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl AwsCredentialLoad for EcsCredentialLoader {
    async fn load_credential(&self, client: Client) -> anyhow::Result<Option<AwsCredential>> {
        let uri = format!("http://169.254.170.2/{}", self.ecs_uri);
        let response = client.get(&uri).send().await?;
        let cred: ECSAwsCredential = response.json().await?;
        Ok(Some(AwsCredential {
            access_key_id: cred.access_key_id,
            secret_access_key: cred.secret_access_key,
            session_token: cred.session_token,
            expires_in: cred.expires_in,
        }))
    }
}

impl S3WriterBackend {
    pub fn new(bucket: &str, encryption_key: Option<String>) -> Result<Self, Error> {
        let builder = services::S3::default().bucket(&bucket);
        let builder = if let Ok(ecs_uri) = std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
            builder.customized_credential_load(Box::new(EcsCredentialLoader { ecs_uri }))
        } else {
            builder
        };
        let op = Operator::new(builder)?.finish();
        match encryption_key {
            Some(key) => {
                let encryptor = TOKIO_RUNTIME.block_on(Encryptor::new(key.as_bytes(), None))?;
                Ok(Self {
                    op,
                    encryptor: Some(encryptor),
                })
            }
            None => Ok(Self {
                op,
                encryptor: None,
            }),
        }
    }
}

impl ParquetWriterBackend for S3WriterBackend {
    /**
     * Write parquet file to S3
     */
    fn write_parquet(
        &self,
        file_name: &str,
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> Result<(), Error> {
        // write parquet to buffer
        let mut buffer: Vec<u8> = Vec::with_capacity(1000);
        write_parquet(&mut buffer, schema, batches)?;
        // Optional encryption before move to S3
        // push file to s3
        TOKIO_RUNTIME.block_on(async move {
            if let Some(encryptor) = &self.encryptor {
                buffer = encryptor.encrypt_in_place(&buffer).await?;
            }
            self.op.write(file_name, buffer).await?;
            Ok::<(), Error>(())
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deserialize() {
        let json = r#"{
            "RoleArn": "",
            "AccessKeyId": "SOME_ACCESS_KEY",
            "SecretAccessKey": "SOME_SECRET_KEY",
            "Token": "SOME_TOKEN",
            "Expiration": "2024-08-26T13:10:01Z"
        }"#;
        let credentials: ECSAwsCredential = serde_json::from_str(json).unwrap();
        assert_eq!(credentials.access_key_id, "SOME_ACCESS_KEY");
        assert_eq!(credentials.secret_access_key, "SOME_SECRET_KEY");
        assert!(credentials.session_token.is_some());
        assert!(credentials.expires_in.is_some());
    }
}
