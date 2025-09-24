// use odbc_rm::{read_to_parquet_internal, read_to_json_internal, ExtractOptions};
use common::{
    extract_generic_to_parquet, print_json_generic, ConnectionOption, ExtractFunction,
    ExtractOptions,
};
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
};
use serde_json::Value;
use std::{collections::HashMap, convert::TryFrom, sync::Arc};

mod common;
mod error;
mod odbc;
mod oracle_src;
mod postgres;

enum DataSourceSupport {
    ORACLE,
    ODBC,
    POSTGRES,
}

impl TryFrom<&str> for DataSourceSupport {
    type Error = error::Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "ORACLE" => Ok(Self::ORACLE),
            "ODBC" => Ok(Self::ODBC),
            "POSTGRES" => Ok(Self::POSTGRES),
            _ => Err(Self::Error::NotImplementedError(format!(
                "Datasource {} is not supported",
                value
            ))),
        }
    }
}

#[pyfunction]
#[pyo3(signature = (connection_string, query, base_dir, table_dir, log_dir, total_size, datasource_type, batch_size=None, user_name=None, password=None, encryption_key=None,prefix=None))]
fn read_to_parquet(
    connection_string: &str,
    query: &str,
    base_dir: &str,
    table_dir: &str,
    log_dir: &str,
    total_size: usize,
    datasource_type: &str,
    batch_size: Option<usize>,
    user_name: Option<&str>,
    password: Option<&str>,
    encryption_key: Option<String>,
    prefix: Option<String>,
) -> PyResult<i64> {
    let extract_option = ExtractOptions::new(
        base_dir,
        table_dir,
        log_dir,
        prefix,
        total_size,
        encryption_key,
    );
    let connection_options = Arc::new(ConnectionOption::new(
        connection_string,
        user_name,
        password,
        query,
    ));
    let datasource_type = DataSourceSupport::try_from(datasource_type)?;
    let extract_function: &ExtractFunction = match datasource_type {
        DataSourceSupport::ORACLE => &oracle_src::extract_oracle,
        DataSourceSupport::ODBC => &odbc::extract_odbc,
        DataSourceSupport::POSTGRES => &postgres::extract_postgres,
    };
    // println!("{connection_string}");
    Ok(extract_generic_to_parquet(
        extract_function,
        extract_option,
        batch_size,
        connection_options,
    )?)
}

struct RustDicts {
    data: Vec<HashMap<String, Value>>,
}

impl<'py> IntoPyObject<'py> for RustDicts {
    type Target = PyList;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let mut result = Vec::new();
        for row in self.data {
            let dict = PyDict::new(py);
            for (k, v) in row {
                match v {
                    Value::Null => dict.set_item(k, None::<String>).unwrap(),
                    Value::Bool(b) => dict.set_item(k, b).unwrap(),
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            dict.set_item(k, i).unwrap();
                        } else if let Some(f) = n.as_f64() {
                            dict.set_item(k, f).unwrap();
                        } else {
                            panic!("Invalid number");
                        }
                    }
                    Value::String(s) => dict.set_item(k, s).unwrap(),
                    Value::Array(_) | Value::Object(_) => dict.set_item(k, v.to_string()).unwrap(),
                }
            }
            result.push(dict);
        }
        PyList::new(py, result)
    }
}

#[pyfunction]
#[pyo3(signature = (connection_string, query, datasource_type, user_name=None, password=None))]
fn read_to_json(
    connection_string: &str,
    query: &str,
    datasource_type: &str,
    user_name: Option<&str>,
    password: Option<&str>,
) -> PyResult<RustDicts> {
    let connection_options = Arc::new(ConnectionOption::new(
        connection_string,
        user_name,
        password,
        query,
    ));
    let datasource_type = DataSourceSupport::try_from(datasource_type)?;
    let extract_function: &ExtractFunction = match datasource_type {
        DataSourceSupport::ORACLE => &oracle_src::extract_oracle,
        DataSourceSupport::ODBC => &odbc::extract_odbc,
        DataSourceSupport::POSTGRES => &postgres::extract_postgres,
    };
    let data = print_json_generic(extract_function, connection_options)?;
    Ok(RustDicts { data })
}

/// A Python module implemented in Rust.
#[pymodule]
fn odbc_source(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_to_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(read_to_json, m)?)?;
    Ok(())
}
