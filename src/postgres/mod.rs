use std::{collections::HashMap, iter::FromIterator, sync::Arc};

use crate::{error::Error, ConnectionOption};
use arrow::{
    array::Array,
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use crossbeam_channel::Sender;
use itertools::Itertools;
use postgres::{
    binary_copy::BinaryCopyOutIter, fallible_iterator::FallibleIterator, Client, Column,
};
use postgres_rustls::{MakeTlsConnector};
use tokio_rustls::TlsConnector;
use rustls::crypto::aws_lc_rs::default_provider;
use rustls::client::danger::{ServerCertVerifier, ServerCertVerified, HandshakeSignatureValid};
use rustls::{DigitallySignedStruct, SignatureScheme};

mod deserializer;

#[derive(Debug)]
struct NoCertVerifier;


impl ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
        ]
    }
}


fn init_crypto_provider() {
    let provider = default_provider();
    provider
        .install_default()
        .expect("failed to install default crypto provider");
}

fn try_connect_with_fallback(conn_str: &str) -> Result<Client, Error> {
    // Try NoTls first (more common in dev environments)
    match Client::connect(conn_str, postgres::NoTls) {
        Ok(client) => {
            println!("Connected to PostgreSQL without TLS");
            return Ok(client);
        }
        Err(_) => {
            println!("NoTls connection failed, trying with TLS...");
        }
    }
    init_crypto_provider();

    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    config.dangerous().set_certificate_verifier(Arc::new(NoCertVerifier));

    let connector = TlsConnector::from(Arc::new(config));
    
    let tls = MakeTlsConnector::new(connector);
    match Client::connect(conn_str, tls) {
        Ok(client) => {
            println!("Connected to PostgreSQL with rustls TLS");
            Ok(client)
        }
        Err(e) => Err(Error::ConnectionError(format!("Failed to connect with both NoTls and rustls TLS: {}", e)))
    }
}

fn infer_schema(columns: &[Column]) -> Result<Schema, Error> {
    let fields = columns.iter().map(|c| {
        let data_type = match c.type_().name() {
            "int2" => DataType::Int16,
            "int4" => DataType::Int32,
            "int8" => DataType::Int64,
            "float4" => DataType::Float32,
            "float8" => DataType::Float64,
            "bool" => DataType::Boolean,
            "numeric" => DataType::Decimal128(38, 10),
            "text" | "citext" | "ltree" | "lquery" | "ltxtquery" | "name" | "char" | "json"
            | "jsonb" => DataType::LargeUtf8,
            "timestamptz" | "timestamp" => DataType::Timestamp(TimeUnit::Millisecond, None),
            "date" => DataType::Date32,
            // "bytea" => DataType::LargeBinary,
            _ => DataType::LargeBinary, // Convert the rest to binary
        };
        // Assume every field is nullable, since postgres driver provide no indication of nullability
        // Add metadata for JSON, JSONB
        if c.type_().name() == "json" || c.type_().name() == "jsonb" {
            Field::new(c.name(), data_type, true).with_metadata(HashMap::from_iter(
                vec![("json".to_string(), "true".to_string())].into_iter(),
            ))
        } else {
            Field::new(c.name(), data_type, true)
        }
    });
    Ok(Schema::new(fields.collect::<Vec<_>>()))
}


pub fn extract_postgres(
    sender: Sender<Vec<Arc<dyn Array>>>,
    schema_tx: (Sender<Schema>, usize),
    batch_size: usize,
    connection_options: Arc<ConnectionOption>,
) -> Result<(), Error> {
    // expect postgres://user:password@host:port/database
    let conn_str = format!(
        "postgres://{}:{}@{}",
        connection_options.user_name().unwrap_or(""),
        connection_options.password().unwrap_or(""),
        connection_options.connection_string(),
    );
    
    // Try connecting with automatic fallback (NoTls first, then TLS)
    let mut conn = try_connect_with_fallback(&conn_str)?;
    let stmt = conn.prepare(connection_options.query())?;
    
    // Debug: Print column information
    for (i, col) in stmt.columns().iter().enumerate() {
        println!("Column {}: {} -> {}", i, col.name(), col.type_().name());
    }
    
    let pg_schema = stmt
        .columns()
        .iter()
        .map(|c| c.type_().clone())
        .collect_vec();
    let schema = infer_schema(&stmt.columns())?;
    let (schema_tx, num_writer_thread) = schema_tx;
    for _ in 0..num_writer_thread {
        schema_tx.send(schema.clone())?;
    }
    let copy_query = format!(
        "COPY ({}) TO STDOUT WITH BINARY",
        connection_options.query()
    );
    let reader = conn.copy_out(&copy_query)?;
    let mut iter = BinaryCopyOutIter::new(reader, &pg_schema);
    // let mut it = conn.query_raw::<postgres::Statement, &i8, &[_; 0]>(&stmt, &[])?;
    let mut batch = Vec::with_capacity(batch_size);
    while let Some(row) = iter.next()? {
        batch.push(row);
        if batch.len() == batch_size {
            let array_batch = deserializer::deserialize_batch(&batch, &schema)?;
            sender.send(array_batch)?;
            batch.clear();
        }
    }
    // Send the final batch
    if !batch.is_empty() {
        let array_batch = deserializer::deserialize_batch(&batch, &schema)?;
        sender.send(array_batch)?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use std::fs;

    use super::*;
    use crate::common::{
        extract_generic_to_parquet, ConnectionOption, ExtractOptions, print_json_generic
    };

    #[test]
    fn test_print_json() {
        let connection_options = ConnectionOption::new(
            "localhost:5432/postgres",
            Some("postgres"),
            Some("postgres"),
            "SELECT num::NUMERIC FROM generate_series(1, 6) num",
        );
        let rs = print_json_generic(&extract_postgres, Arc::new(connection_options));
        println!("{:?}", rs);
    }

    #[test]
    fn test_extract_postgres() {
        fs::create_dir_all("/tmp/test_extraction/data/transaction").unwrap();
        fs::create_dir_all("/tmp/test_extraction/logs").unwrap();
        let connection_options = ConnectionOption::new(
            "localhost:5432/postgres",
            Some("postgres"),
            Some("postgres"),
            r#"SELECT num::NUMERIC,  Current_timestamp::Date as date_col, '{"a": "a"}'::jsonb as json_col 
                    FROM generate_series(1, 6) num"#,
        );
        let extract_options = ExtractOptions::new(
            "/tmp/test_extraction/data",
            "transaction",
            "/tmp/test_extraction/logs",
            None,
            1000,
            None,
        );
        let rs = extract_generic_to_parquet(
            &extract_postgres,
            extract_options,
            Some(5000),
            Arc::new(connection_options),
        );
        println!("{:?}", rs);
    }

    #[test]
    fn test_extract_postgres_s3() {
        let connection_options = ConnectionOption::new(
            "localhost:5432/postgres",
            Some("postgres"),
            Some("postgres"),
            "SELECT num::NUMERIC FROM generate_series(1, 6) num",
        );
        let extract_options = ExtractOptions::new(
            "s3://temporary-bucket-just-for-sharing-things-123",
            "transaction",
            "/tmp/test/",
            None,
            1000,
            None,
        );
        let rs = extract_generic_to_parquet(
            &extract_postgres,
            extract_options,
            Some(5000),
            Arc::new(connection_options),
        );
        println!("{:?}", rs);
    }
}
