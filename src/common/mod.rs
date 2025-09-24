use crate::error::Error;
use arrow::{array::Array, datatypes::Schema, record_batch::RecordBatch};
use crossbeam_channel::{bounded, Sender};
use itertools::Itertools;
use once_cell::sync::Lazy;
use rayon::{ThreadPool, ThreadPoolBuilder};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::thread;

mod encryption;
mod parquet;

pub static POOL: Lazy<ThreadPool> = Lazy::new(|| {
    ThreadPoolBuilder::new()
        .num_threads(
            std::env::var("POLARS_MAX_THREADS")
                .map(|s| s.parse::<usize>().expect("integer"))
                .unwrap_or_else(|_| {
                    std::thread::available_parallelism()
                        .unwrap_or(std::num::NonZeroUsize::new(1).unwrap())
                        .get()
                }),
        )
        .build()
        .expect("could not spawn threads")
});

#[derive(Debug, Clone)]
pub struct ExtractOptions {
    base_dir: String,
    table_dir: String, // Data Path
    log_dir: String,   // Log Directory
    file_prefix: Option<String>,
    total_size: usize,
    encryption_key: Option<String>,
}
impl ExtractOptions {
    pub fn new(
        base_dir: &str,
        table_dir: &str,
        log_dir: &str,
        file_prefix: Option<String>,
        total_size: usize,
        encryption_key: Option<String>,
    ) -> Self {
        Self {
            base_dir: base_dir.to_string(),
            table_dir: table_dir.to_string(),
            log_dir: log_dir.to_string(),
            file_prefix,
            total_size,
            encryption_key,
        }
    }

    pub fn table_dir(&self) -> &str {
        &self.table_dir
    }

    pub fn get_extract_file_path(&self, index: usize) -> String {
        // if base_dir is s3://, split to get key and bucket
        // else just use base_dir
        let base_dir = if self.base_dir.starts_with("s3://") {
            let base_dir = self.base_dir.replace("s3://", "");
            let base_dir = base_dir.split("/").collect::<Vec<_>>();
            if base_dir.len() < 2 {
                "".to_string()
            } else {
                base_dir[1..].join("/")
            }
        } else {
            self.base_dir.clone()
        };
        let prefix = self.file_prefix.as_deref().unwrap_or("");
        let suffix = if self.encryption_key.is_some() {
            ".parquet.encrypted"
        } else {
            ".parquet"
        };
        // Remove trailing slash
        let base_dir = base_dir.trim_end_matches('/');
        if base_dir.is_empty() {
            format!("/{}/{}{:06}{}", self.table_dir, prefix, index, suffix)
        } else {
            format!(
                "/{}/{}/{}{:06}{}",
                base_dir, self.table_dir, prefix, index, suffix
            )
        }
    }

    pub fn total_size(&self) -> usize {
        self.total_size
    }
    pub fn get_extract_log_path(&self) -> String {
        // .../TABLE_NAME/year=2022/month=02/day=01
        // TABLE_NAME_2022_02_01.csv
        format!(
            "{}/{}.csv",
            self.log_dir,
            self.table_dir
                .replace("/", "_")
                .replace("year=", "")
                .replace("month=", "")
                .replace("day=", "")
        )
    }

    pub fn get_writer_backend(&self) -> Result<Box<dyn parquet::ParquetWriterBackend>, Error> {
        // Get Backend based on base_dir
        // if begin with s3://, then use S3WriterBackend
        // else use FileWriterBackend
        if self.base_dir.starts_with("s3://") {
            // separate bucket and key
            let base_dir = self.base_dir.replace("s3://", "");
            let base_dir = base_dir.split("/").collect::<Vec<_>>();
            Ok(Box::new(parquet::S3WriterBackend::new(
                base_dir[0],
                self.encryption_key.clone(),
            )?))
        } else {
            Ok(Box::new(parquet::FileWriterBackend::new(
                self.encryption_key.clone(),
            )?))
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionOption {
    connection_string: String,
    user_name: Option<String>,
    password: Option<String>,
    query: String,
}

impl ConnectionOption {
    pub fn new(
        connection_string: &str,
        user_name: Option<&str>,
        password: Option<&str>,
        query: &str,
    ) -> Self {
        Self {
            connection_string: connection_string.to_string(),
            user_name: user_name.map(String::from),
            password: password.map(String::from),
            query: query.to_string(),
        }
    }

    pub fn connection_string<'a>(&'a self) -> &'a str {
        &self.connection_string
    }
    pub fn user_name<'a>(&'a self) -> Option<&'a str> {
        self.user_name.as_ref().map(|e| e.as_str())
    }
    pub fn password<'a>(&'a self) -> Option<&'a str> {
        self.password.as_ref().map(|e| e.as_str())
    }
    pub fn query<'a>(&'a self) -> &'a str {
        &self.query
    }
}

pub(crate) type ExtractFunction = dyn Fn(
        Sender<Vec<Arc<dyn Array>>>,
        (Sender<Schema>, usize),
        usize,
        Arc<ConnectionOption>,
    ) -> Result<(), Error>
    + Send
    + Sync;

pub(crate) fn extract_generic_to_parquet(
    extract_function: &ExtractFunction,
    extract_options: ExtractOptions,
    batch_size: Option<usize>,
    connection_option: Arc<ConnectionOption>,
) -> Result<i64, Error> {
    let num_writer_thread = 1usize;
    let (sender, receiver) = bounded(10);
    let (schema_tx, schema_rx) = bounded(num_writer_thread);
    // let schema_barrier = Arc::new((RwLock::new(Schema::from(vec![])), Barrier::new(2)));
    let total_records = thread::scope(|scope| {
        let extract_thread = scope.spawn(|| {
            extract_function(
                sender,
                (schema_tx, num_writer_thread),
                batch_size.unwrap_or(50_000),
                connection_option.clone(),
            )
        });
        let writer_thread = scope.spawn(|| {
            let schema_rx = schema_rx.clone();
            let schema = Arc::new(schema_rx.recv()?);
            let mut log_file = std::fs::File::create(extract_options.get_extract_log_path())?;
            log_file.write_all(
                b"OUTPUT_PATH,NUM_RECORDS,TOTAL_RECORDS,CURRENT_TIMESTAMP_UTC,PART_NUMBER\n",
            )?;
            receiver
                .iter()
                .map(|columns| RecordBatch::try_new(schema.clone(), columns))
                .chunks(10)
                .into_iter()
                .enumerate()
                .map(|(chunk_id, chunk)| {
                    let chunks = chunk.collect::<Result<Vec<_>, _>>()?;
                    let total_len: usize = chunks.iter().map(|e| e.num_rows()).sum();
                    let file_name = extract_options.get_extract_file_path(chunk_id);
                    let writer = extract_options.get_writer_backend()?;
                    writer.write_parquet(&file_name, schema.clone(), chunks)?;
                    // write_parquet(&mut file, schema.clone(), chunks)?;
                    // Output LOG, No Allocate
                    writeln!(
                        &mut log_file,
                        "{},{:?},{:?},{:?},{:?}",
                        extract_options.table_dir(),
                        total_len,
                        extract_options.total_size(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis(),
                        chunk_id,
                    )?;
                    Ok(total_len as i64)
                })
                .collect::<Result<Vec<i64>, Error>>()
        });
        extract_thread
            .join()
            .map_err(|panic| Error::sync_error("extract thread", &panic))??;
        writer_thread
            .join()
            .map_err(|panic| Error::sync_error("writer thread", &panic))?
    })?;
    Ok(total_records.iter().sum())
}

pub(crate) fn print_json_generic(
    extract_function: &ExtractFunction,
    connection_option: Arc<ConnectionOption>,
) -> Result<Vec<HashMap<String, Value>>, Error> {
    let num_writer_thread = 1usize;
    let (sender, receiver) = bounded(10);
    let (schema_tx, schema_rx) = bounded(num_writer_thread);
    thread::scope(|scope| {
        let extract_thread = scope.spawn(|| {
            extract_function(
                sender,
                (schema_tx, num_writer_thread),
                2_000,
                connection_option.clone(),
            )
        });
        let writer_thread = scope.spawn(|| {
            let schema = Arc::new(schema_rx.recv()?);
            let buf = Vec::new();
            let mut writer = arrow::json::ArrayWriter::new(buf);
            // Just ignore the send error here
            while let Ok(arr) = receiver.recv() {
                let record_batch = RecordBatch::try_new(schema.clone(), arr)?;
                writer.write(&record_batch)?;
            }
            match writer.finish() {
                Ok(_) => Ok(writer.into_inner()),
                Err(e) => Err(Error::from(e)),
            }
        });
        extract_thread
            .join()
            .map_err(|panic| Error::sync_error("extract thread", &panic))??;
        let raw_data = writer_thread
            .join()
            .map_err(|panic| Error::sync_error("writer thread", &panic))??;
        serde_json::from_slice(&raw_data).map_err(|e| Error::NotImplementedError(e.to_string()))
    })
}

#[cfg(test)]
mod test {
    use super::*;

    type ExtractFunction2 = dyn Fn(Sender<i64>) -> () + Send + Sync;

    fn test_sender(sender: Sender<i64>) {
        for i in 0..10i64 {
            sender.send(i).unwrap();
        }
    }

    fn test_consumer(extract_fun: &ExtractFunction2) {
        let (sender, receiver) = bounded(10);
        let rs = thread::scope(|scope| {
            let send_thread = scope.spawn(|| extract_fun(sender));
            for i in receiver {
                println!("{:?}", i);
            }
            send_thread.join()
        });
        println!("{:?}", rs);
    }

    #[test]
    fn test_closuse() {
        test_consumer(&test_sender);
    }

    #[test]
    fn test_s3_path() {
        let options =
            ExtractOptions::new("s3://bucket_name", "table_name", "log_dir", None, 100, None);
        assert_eq!(
            options.get_extract_file_path(1),
            "/table_name/000001.parquet"
        );
    }

    #[test]
    fn test_s3_path_with_prefix() {
        let options = ExtractOptions::new(
            "s3://bucket_name/prefix",
            "table_name",
            "log_dir",
            None,
            100,
            None,
        );
        assert_eq!(
            options.get_extract_file_path(1),
            "/prefix/table_name/000001.parquet"
        );
    }

    #[test]
    fn test_s3_path_with_prefix_and_trailing_slash() {
        let options = ExtractOptions::new(
            "s3://bucket_name/prefix/",
            "table_name",
            "log_dir",
            None,
            100,
            None,
        );
        assert_eq!(
            options.get_extract_file_path(1),
            "/prefix/table_name/000001.parquet"
        );
    }
}
