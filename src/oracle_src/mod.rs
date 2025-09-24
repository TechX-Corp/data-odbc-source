use crate::common::{ConnectionOption, POOL};
use crate::error::Error;
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::{array::Array, datatypes::Schema};
use crossbeam_channel::Sender;
use deserializer::deserialize_column;
// use oracle::{Connection, Row};
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use techx_rust_oracle::{ColumnInfo, Connection, OracleType, Statement};

mod deserializer;

fn infer_schema(column_info: &[ColumnInfo]) -> Result<Schema, Error> {
    let fields = column_info
        .iter()
        .map(|col| {
            let mut is_free_size_number = false;
            let data_type = match col.oracle_type() {
                OracleType::Varchar2(_)
                | OracleType::NVarchar2(_)
                | OracleType::Char(_)
                | OracleType::CLOB
                | OracleType::NCLOB
                | OracleType::Json
                | OracleType::NChar(_) => DataType::LargeUtf8,
                OracleType::Number(p, s) => {
                    if (*s >= 0) & (*p <= 38) & ((*s as u8) <= *p) {
                        // Maximum precision if precision is not specified or is 0
                        let new_p = if *p == 0 { 38 } else { *p };
                        DataType::Decimal128(new_p, *s)
                    } else {
                        is_free_size_number = true;
                        DataType::Float64
                    }
                }
                OracleType::Float(_) => {
                    is_free_size_number = true;
                    DataType::Float64
                }
                OracleType::BinaryDouble => DataType::Float64,
                OracleType::BinaryFloat => DataType::Float32,
                // OracleType::Date => DataType::Date32, // get Date without Time
                OracleType::Date => DataType::Timestamp(TimeUnit::Millisecond, None),
                OracleType::Timestamp(_)
                | OracleType::TimestampTZ(_)
                | OracleType::TimestampLTZ(_) => DataType::Timestamp(TimeUnit::Millisecond, None),
                OracleType::Raw(_)
                | OracleType::BLOB
                | OracleType::BFILE
                | OracleType::Long
                | OracleType::LongRaw => DataType::LargeBinary,
                ora_type => {
                    return Err(Error::NotImplementedError(format!(
                        "Extractor does not support Oracle Type {:?}",
                        ora_type
                    )))
                }
            };
            let mut metadata: HashMap<String, String> = HashMap::new();
            metadata.insert(
                "is_free_size_number".to_string(),
                is_free_size_number.to_string(),
            );
            Ok(Field::new(col.name(), data_type, col.nullable()).with_metadata(metadata))
        })
        .collect::<Result<Vec<Field>, Error>>()?;
    Ok(Schema::new(fields))
}

pub fn extract_oracle(
    sender: Sender<Vec<Arc<dyn Array>>>,
    schema_tx: (Sender<Schema>, usize),
    batch_size: usize,
    connection_options: Arc<ConnectionOption>,
) -> Result<(), Error> {
    let conn = Connection::connect(
        connection_options.user_name().unwrap(),
        connection_options.password().unwrap(),
        connection_options.connection_string(),
    )?;
    let mut statement = Statement::new(&conn, connection_options.query(), batch_size as u32)?;
    statement.execute()?;
    let column_info = statement.column_info();
    let rvs = statement.sql_values();
    let schema = infer_schema(&column_info)?;
    // println!("{:?}", schema);
    // Send schema info to writers
    let (schema_tx, num_writer_thread) = schema_tx;
    for _ in 0..num_writer_thread {
        schema_tx.send(schema.clone())?;
    }
    // println!("Schema {:?}", column_info);
    let used_schema = Arc::new(schema);
    #[allow(unused_assignments)]
    let mut num_row_fetched = 0;
    let mut more_rows = true;
    while more_rows {
        (_, num_row_fetched, more_rows) = statement.fetch_next_batch()?;
        let arrs = POOL.install(|| {
            rvs.par_iter()
                .zip(used_schema.fields.par_iter())
                .map(|(sv, field)| deserialize_column(sv, field, num_row_fetched as usize))
                .collect::<Result<Vec<_>, Error>>()
        })?;
        sender.send(arrs)?;
    }
    drop(sender);
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::{
        extract_generic_to_parquet, print_json_generic, ConnectionOption, ExtractOptions,
    };

    #[test]
    fn test_extract_oracle() {
        let connection_options = ConnectionOption::new(
            "localhost:1521/xepdb1",
            Some("oracle"),
            Some("123456a@"),
            "SELECT * FROM test where 1=0",
        );
        let extract_options = ExtractOptions::new(
            "/tmp/extraction-lib/oracle_test",
            "test",
            "/tmp/extraction-lib/oracle_test/logs/test/",
            None,
            1000,
            None,
        );
        let rs = extract_generic_to_parquet(
            &extract_oracle,
            extract_options,
            Some(5000),
            Arc::new(connection_options),
        );
        println!("{:?}", rs);
    }

    #[test]
    fn test_print_json() {
        let connection_options = ConnectionOption::new(
            "localhost:1521/dev",
            Some("user"),
            Some("pass"),
            r#"SELECT ARRANGEMENT_ID, M, S as EXPECTED_ROW FROM "schema"."table""#,
        );
        let rs = print_json_generic(&extract_oracle, Arc::new(connection_options));
        let rs = rs.unwrap();
        println!("{:?}", rs.len());
        // assert!(rs.is_ok());
    }

    #[test]
    fn test_print_json_dev() {
        let connection_options = ConnectionOption::new(
            "localhost:1521/dev",
            Some("user"),
            Some("pass"),
            r#"SELECT * FROM "schema"."table" WHERE id = 242"#,
        );
        let rs = print_json_generic(&extract_oracle, Arc::new(connection_options));
        println!("{:?}", rs);
    }

    #[test]
    fn test_print_json_clob() {
        let connection_options = ConnectionOption::new(
            "localhost:1521/dev",
            Some("user"),
            Some("pass"),
            r#"SELECT RECID, (XMLRECORD).getClobVal() as XMLRECORD FROM "schema"."table" WHERE ROWNUM < 10"#,
        );
        let rs = print_json_generic(&extract_oracle, Arc::new(connection_options));
        let rs = rs.unwrap();
        println!("{:?}", rs);
        // assert!(rs.is_ok());
    }
}
