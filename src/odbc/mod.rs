use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::common::{ConnectionOption, POOL};
use crate::error::Error;
use arrow::{array::Array, datatypes::Schema};
use crossbeam_channel::Sender;
use deserialize::deserialize_cv;
use odbc_api::ConnectionOptions;
use odbc_api::{
    buffers::{AnyBuffer, BufferDesc, ColumnarAnyBuffer, ColumnarBuffer},
    Cursor, Environment,
};
use once_cell::sync::Lazy;
use rayon::prelude::*;

mod deserialize;
mod schema;

static ODBC_ENV: Lazy<Environment> =
    Lazy::new(|| Environment::new().expect("Can't create new environment."));

fn cap_text_type_length(length: Option<NonZeroUsize>) -> Option<NonZeroUsize> {
    match length {
        Some(length) if length.get() < 40000 => Some(length),
        _ => NonZeroUsize::new(40000),
    }
}

fn buffer_from_metadata(
    resut_set_metadata: &mut impl odbc_api::ResultSetMetadata,
    max_batch_size: usize,
) -> Result<ColumnarBuffer<AnyBuffer>, odbc_api::Error> {
    let num_cols: u16 = resut_set_metadata.num_result_cols()? as u16;

    let descs = (0..num_cols)
        .map(|index| {
            let mut column_description = odbc_api::ColumnDescription::default();

            resut_set_metadata.describe_col(index + 1, &mut column_description)?;
            let data_type = match column_description.data_type {
                odbc_api::DataType::LongVarchar { length } => odbc_api::DataType::LongVarchar {
                    length: cap_text_type_length(length),
                },
                odbc_api::DataType::Varchar { length } => odbc_api::DataType::Varchar {
                    length: cap_text_type_length(length),
                },
                odbc_api::DataType::LongVarbinary { length } => odbc_api::DataType::LongVarbinary {
                    length: cap_text_type_length(length),
                },
                odbc_api::DataType::Varbinary { length } => odbc_api::DataType::Varbinary {
                    length: cap_text_type_length(length),
                },
                odbc_api::DataType::WVarchar { length } => odbc_api::DataType::WVarchar {
                    length: cap_text_type_length(length),
                },
                odbc_api::DataType::Other {
                    data_type: odbc_api::sys::SqlDataType::EXT_W_LONG_VARCHAR,
                    column_size,
                    decimal_digits: _,
                } => odbc_api::DataType::LongVarchar {
                    length: cap_text_type_length(column_size),
                },
                odbc_api::DataType::Other {
                    data_type: odbc_api::sys::SqlDataType::EXT_GUID,
                    column_size: _,
                    decimal_digits: _,
                } => odbc_api::DataType::Varchar {
                    length: NonZeroUsize::new(36),
                },
                _ => column_description.data_type,
            };
            // println!("{:?}", data_type);
            match data_type {
                odbc_api::DataType::WChar { length } | odbc_api::DataType::WVarchar { length } => {
                    length.map(|l| BufferDesc::WText {
                        max_str_len: l.into(),
                    })
                }
                _ => BufferDesc::from_data_type(data_type, column_description.could_be_nullable()),
            }
            .ok_or_else(|| {
                odbc_api::Error::FailedReadingInput(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Unsupported data type {:?}", data_type.data_type()).as_str(),
                ))
            })
        })
        .collect::<std::result::Result<Vec<_>, odbc_api::Error>>()?;
    // println!("descs: {:?}", descs);
    ColumnarAnyBuffer::try_from_descs(max_batch_size, descs.into_iter())
}

pub fn extract_odbc(
    sender: Sender<Vec<Arc<dyn Array>>>,
    schema_tx: (Sender<Schema>, usize),
    batch_size: usize,
    connection_options: Arc<ConnectionOption>,
) -> Result<(), Error> {
    let conn = ODBC_ENV.connect_with_connection_string(
        connection_options.connection_string(),
        ConnectionOptions {
            login_timeout_sec: Some(10),
            packet_size: None,
        },
    )?;
    let mut cur = conn.prepare(connection_options.query())?;
    let fields = schema::infer_schema(&mut cur)?;
    let schema = Schema::new(fields.clone());
    // Send schema info to writers
    let (schema_tx, num_writer_thread) = schema_tx;
    for _ in 0..num_writer_thread {
        schema_tx.send(schema.clone())?;
    }
    let buffer = buffer_from_metadata(&mut cur, batch_size)?;
    if let Some(cursor) = cur.execute(())? {
        let mut cursor = cursor.bind_buffer(buffer)?;
        let mut is_first_batch = true;
        loop {
            match cursor.fetch()? {
                Some(batch) => {
                    // tx.send(batch);
                    // println!("Fetching Buffer");
                    let arr = POOL.install(|| {
                        fields
                            .par_iter()
                            .enumerate()
                            .map(|(index, field)| {
                                let column_view = batch.column(index);
                                match deserialize_cv(column_view, field.data_type()) {
                                    Ok(arr) => Ok(Arc::from(arr)),
                                    Err(Error::DataError(message)) => Err(Error::DataError(
                                        format!("Column: {}, Error: {}", field.name(), message),
                                    )),
                                    Err(err) => Err(err),
                                }
                            })
                            .collect::<Result<Vec<Arc<dyn Array>>, _>>()
                    })?;
                    sender.send(arr)?;
                    is_first_batch = false;
                }
                None => {
                    if is_first_batch {
                        let arr = fields
                            .iter()
                            .map(|field| arrow::array::new_empty_array(field.data_type()))
                            .collect();
                        sender.send(arr)?;
                    }
                    break;
                }
            }
        }
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

    const MSSQL_CONNECTION_STRING:  &'static str = "Driver={MSSQL};DATABASE=test_mssql;SERVER=localhost,1521;UID=admin;PWD=pass;Encrypt=no;ClientCharset=UTF-8;ServerCharset=CP1258;";

    #[test]
    fn test_connect_mysql() {
        let conn_string = "Driver={MySQL};Server=localhost;charset=UTF8;Database=test_mssql;User=user_read;Password=pass;Option=3;";
        let query = r#"
            SELECT *
            FROM `test_mssql`.`table`
            limit 10
        "#;
        let connection_option = Arc::new(ConnectionOption::new(conn_string, None, None, query));
        let data = print_json_generic(&extract_odbc, connection_option);
        println!("{:?}", data);
        assert!(data.is_ok());
    }

    #[test]
    fn test_connect_mysql_err() {
        let conn_string = "Driver={MySQL};Server=localhost;Database=test_mssql;User=user_read;Password=pass;Option=3;";
        let query = r#"
            SELECT *
            FROM `test_mssql`.`table`
            limit 100
        "#;
        let connection_option = Arc::new(ConnectionOption::new(conn_string, None, None, query));
        let data = print_json_generic(&extract_odbc, connection_option);
        println!("{:?}", data);
        assert!(data.is_err());
    }

    #[test]
    fn test_extract_mysql() {
        let conn_string = "Driver={MySQL};Server=localhost;charset=UTF8;Database=test_mssql;User=user_read;Password=pass;Option=3;";
        let query = r#"
            SELECT
                `id` as `id`,
                `created_timestamp` as `created_timestamp`,
                `updated_timestamp` as `updated_timestamp`,
                `created_by` as `created_by`,
                `updated_by` as `updated_by`,
                `email` as `email`,
                `legal_id` as `legal_id`,
                `name` as `name`,
                `phone` as `phone`,
                `type_customer` as `type_customer`,
                `cif` as `cif`,
                `birthday` as `birthday`,
                `gender` as `gender`,
                `address` as `address`,
                `account_payment` as `account_payment`,
                `account_contract` as `account_contract`,
                `account_card_type` as `account_card_type`,
                `issued_by` as `issued_by`,
                `date_range` as `date_range`,
                `sub_segment` as `sub_segment`
            FROM
                `test_mssql`.`nl_customer`
        "#;
        let connection_option = Arc::new(ConnectionOption::new(conn_string, None, None, query));
        let extract_options = ExtractOptions::new(
            "/tmp/extraction-lib/mysql_test",
            "test",
            "/tmp/extraction-lib/mysql_test/logs/test",
            None,
            1000,
            None,
        );
        let rs = extract_generic_to_parquet(
            &extract_odbc,
            extract_options,
            Some(5000),
            connection_option,
        );
        println!("{:?}", rs);
        let result = std::panic::catch_unwind(|| {
            panic!("oh no!");
        });
        println!("{:?}", result);
    }

    #[test]
    fn test_invalid_conn_str() {
        let conn = ODBC_ENV.connect_with_connection_string(
            "localhost/pdb1",
            ConnectionOptions::default(),
        );
        if let Err(err) = conn {
            println!("{:?}", err);
        }
    }

    #[test]
    fn test_mssql_conn_str() {
        let conn = ODBC_ENV
            .connect_with_connection_string(MSSQL_CONNECTION_STRING, ConnectionOptions::default());
        if let Err(err) = conn {
            println!("{:?}", err);
        }
        // Test mssql result
        if let Err(err) = test_result_mssql_internal() {
            println!("{:?}", err);
        }
    }

    #[test]
    fn test_connect_mssql() {
        let query = r#"
            SELECT * from test_mssql.dbo.table ORDER BY BONDS_BOOK_SOURCE_ID OFFSET 62238 ROWS
        "#;
        let connection_option = Arc::new(ConnectionOption::new(
            MSSQL_CONNECTION_STRING,
            None,
            None,
            query,
        ));
        let data = print_json_generic(&extract_odbc, connection_option);
        println!("{:?}", data);
        // let result = extract_generic_to_parquet(
        //     &extract_odbc,
        //     ExtractOptions::new(
        //         "/tmp/extraction-lib/mssql_test",
        //         "test",
        //         "/tmp/extraction-lib/mssql_test/logs/test",
        //         None,
        //         1000,
        //         None,
        //     ),
        //     Some(5000),
        //     connection_option,
        // );
        // println!("{:?}", result);
    }

    fn test_result_mssql_internal() -> Result<(), Error> {
        let conn = ODBC_ENV.connect_with_connection_string(
            MSSQL_CONNECTION_STRING,
            ConnectionOptions::default(),
        )?;
        let mut stmt = conn.prepare("SELECT * FROM test_uniqueidentifier")?;
        let fields = schema::infer_schema(&mut stmt)?;
        let schema = Schema::new(fields.clone());
        println!("Schema {:?}", schema);
        let buffer = buffer_from_metadata(&mut stmt, 2000)?;
        if let Some(cursor) = stmt.execute(())? {
            let mut cursor = cursor.bind_buffer(buffer)?;
            while let Some(batch) = cursor.fetch()? {
                fields.iter().enumerate().for_each(|(index, field)| {
                    let column_view = batch.column(index);
                    let arr = deserialize_cv(column_view, field.data_type());
                    println!("Column {} {:?}", field.name(), arr);
                });
            }
        }

        Ok(())
    }

    fn test_connect_db2_internal() -> Result<(), Error> {
        let conn_string =
            "Driver={DB2};Hostname=127.0.0.1;Database=testdb;Uid=db2inst1;Pwd=123123;";
        let conn =
            ODBC_ENV.connect_with_connection_string(conn_string, ConnectionOptions::default())?;
        let mut stmt = conn.prepare("SELECT DISTINCT TABNAME FROM SYSCAT.COLUMNS")?;
        let fields = schema::infer_schema(&mut stmt)?;
        let schema = Schema::new(fields.clone());
        println!("Schema {:?}", schema);
        // if let Some(mut cursor) = stmt.execute(())? {
        //     while let Some(mut row) = cursor.next_row()? {
        //         let mut buf = Vec::new();
        //         row.get_text(1, &mut buf)?;
        //         let ret = String::from_utf8(buf).unwrap();
        //         println!("{ret}")
        //     }
        // }

        let buffer = buffer_from_metadata(&mut stmt, 2000)?;
        // let buffer = ColumnarAnyBuffer::from_description(
        //     2000, vec![odbc_api::buffers::BufferDescription{
        //         nullable: true,
        //         kind: BufferKind::Binary { length: 20 }
        //     }]
        // );
        if let Some(cursor) = stmt.execute(())? {
            let mut cursor = cursor.bind_buffer(buffer)?;
            while let Some(batch) = cursor.fetch()? {
                let column_view = batch.column(0);
                let arr = column_view
                    .as_text_view()
                    .unwrap()
                    .iter()
                    .map(|e| e.map(|s| String::from_utf8_lossy(s).to_string()))
                    .collect::<Vec<_>>();
                // let arr = deserialize_cv(column_view, fields[0].data_type());
                println!("TABNAME {:?}", arr);
            }
        }
        Ok(())
    }

    #[test]
    fn test_connect_db2() {
        if let Err(err) = test_connect_db2_internal() {
            println!("{:?}", err);
        }
    }
}
