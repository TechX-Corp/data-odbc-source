use std::sync::Arc;

use arrow::{
    array::{
        Array, BooleanBuilder, Decimal128Builder, LargeBinaryBuilder, LargeStringBuilder,
        PrimitiveBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    },
    datatypes::{
        ArrowPrimitiveType, DataType, Date32Type, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, Int8Type, Schema, TimeUnit,
    },
};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use postgres::{binary_copy::BinaryCopyOutRow, types::FromSql};

use crate::error::Error;
use rust_decimal::Decimal;

const DATE_1970_01_01: i32 = 719_163;

fn primitive<'a, T: ArrowPrimitiveType>(
    rows: &'a [BinaryCopyOutRow],
    col_idx: usize,
    data_type: DataType,
) -> Arc<dyn Array>
where
    T: ArrowPrimitiveType,
    T::Native: FromSql<'a> + Sized,
{
    let mut mut_array = PrimitiveBuilder::<T>::with_capacity(rows.len());
    for row in rows {
        let value = row.get::<Option<T::Native>>(col_idx);
        mut_array.append_option(value)
    }
    Arc::new(mut_array.with_data_type(data_type).finish())
}

fn bool(rows: &[BinaryCopyOutRow], col_idx: usize) -> Arc<dyn Array> {
    let values = rows.iter().map(|row| row.get::<Option<bool>>(col_idx));
    let mut builder = BooleanBuilder::with_capacity(rows.len());
    builder.extend(values);
    Arc::new(builder.finish())
}

fn string(rows: &[BinaryCopyOutRow], col_idx: usize, is_json: bool) -> Arc<dyn Array> {
    let mut mut_array = LargeStringBuilder::with_capacity(rows.len(), rows.len() * 5000);
    for row in rows {
        let value = if is_json {
            row.get::<Option<serde_json::Value>>(col_idx)
                .and_then(|e| serde_json::to_string(&e).ok())
        } else {
            row.get::<Option<String>>(col_idx)
        };
        mut_array.append_option(value)
    }
    Arc::new(mut_array.finish())
}

fn decimal(rows: &[BinaryCopyOutRow], col_idx: usize, data_type: DataType) -> Arc<dyn Array> {
    let mut mut_array = Decimal128Builder::with_capacity(rows.len());
    for row in rows {
        let value = row.get::<Option<Decimal>>(col_idx);
        let tmp = value.map(|e| {
            if e.scale() <= 10 {
                e.mantissa() * 10i128.pow(10 - e.scale())
            } else {
                e.mantissa() / 10i128.pow(e.scale() - 10)
            }
        });
        mut_array.append_option(tmp)
    }
    Arc::new(mut_array.with_data_type(data_type).finish())
}

fn date(rows: &[BinaryCopyOutRow], col_idx: usize) -> Arc<dyn Array> {
    let mut mut_array = PrimitiveBuilder::<Date32Type>::with_capacity(rows.len());
    for row in rows {
        let value = row
            .get::<Option<NaiveDate>>(col_idx)
            .map(|e| e.num_days_from_ce() - DATE_1970_01_01);
        mut_array.append_option(value)
    }
    Arc::new(mut_array.finish())
}

fn timestamp_microseconds(rows: &[BinaryCopyOutRow], col_idx: usize, data_type: DataType) -> Arc<dyn Array> {
    let mut mut_array = TimestampMicrosecondBuilder::with_capacity(rows.len());
    for row in rows {
        let value = row.get::<Option<NaiveDateTime>>(col_idx).map(|dt| {
            // Convert to microseconds since epoch
            dt.and_utc().timestamp() * 1_000_000 + (dt.and_utc().timestamp_subsec_nanos() as i64) / 1000
        });
        mut_array.append_option(value)
    }
    Arc::new(mut_array.with_data_type(data_type).finish())
}

fn timestamp_milliseconds(rows: &[BinaryCopyOutRow], col_idx: usize, data_type: DataType) -> Arc<dyn Array> {
    let mut mut_array = TimestampMicrosecondBuilder::with_capacity(rows.len());
    for row in rows {
        let value = row.get::<Option<NaiveDateTime>>(col_idx).map(|dt| {
            // Convert to milliseconds since epoch, but store as microseconds (multiply by 1000)
            (dt.and_utc().timestamp() * 1_000 + (dt.and_utc().timestamp_subsec_nanos() as i64) / 1_000_000) * 1_000
        });
        mut_array.append_option(value)
    }
    Arc::new(mut_array.with_data_type(data_type).finish())
}

fn time_microseconds(rows: &[BinaryCopyOutRow], col_idx: usize) -> Arc<dyn Array> {
    let mut mut_array = Time64MicrosecondBuilder::with_capacity(rows.len());
    for row in rows {
        let value = row.get::<Option<NaiveTime>>(col_idx).map(|t| {
            // Convert to microseconds since midnight
            t.num_seconds_from_midnight() as i64 * 1_000_000 + (t.nanosecond() as i64) / 1000
        });
        mut_array.append_option(value)
    }
    Arc::new(mut_array.finish())
}

// Common trait for all binary types
struct BinaryWrapper<'a>(&'a [u8]);

impl<'a> FromSql<'a> for BinaryWrapper<'a> {
    fn accepts(_: &postgres::types::Type) -> bool {
        // We accept all types
        true
    }

    fn from_sql(
        _: &postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(BinaryWrapper(raw))
    }
}

pub fn deserialize_batch(
    rows: &[BinaryCopyOutRow],
    schema: &Schema,
) -> Result<Vec<Arc<dyn Array>>, Error> {
    schema
        .fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let data_type = field.data_type().clone();
            Ok(match data_type {
                DataType::Boolean => bool(rows, i),
                DataType::Int8 => primitive::<Int8Type>(rows, i, data_type),
                DataType::Int16 => primitive::<Int16Type>(rows, i, data_type),
                DataType::Int32 => primitive::<Int32Type>(rows, i, data_type),
                DataType::Int64 => primitive::<Int64Type>(rows, i, data_type),
                DataType::Float32 => primitive::<Float32Type>(rows, i, data_type),
                DataType::Float64 => primitive::<Float64Type>(rows, i, data_type),
                DataType::Decimal128(_, _) => decimal(rows, i, data_type),
                DataType::Date32 => date(rows, i),
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    timestamp_microseconds(rows, i, data_type)
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    timestamp_milliseconds(rows, i, data_type)
                }
                DataType::Time64(TimeUnit::Microsecond) => time_microseconds(rows, i),
                DataType::List(_) => {
                    // Handle arrays as strings for now
                    string(rows, i, false)
                }
                DataType::LargeUtf8 => string(rows, i, field.metadata().get("json").is_some()),
                DataType::LargeBinary => {
                    let mut mut_array =
                        LargeBinaryBuilder::with_capacity(rows.len(), rows.len() * 5000);
                    for row in rows {
                        let value: Option<&[u8]> = row.get::<Option<BinaryWrapper>>(i).map(|e| e.0);
                        mut_array.append_option(value)
                    }
                    Arc::new(mut_array.finish())
                }
                _ => Err(Error::NotImplementedError(
                    format!("Unsupported data type: {:?}", data_type).to_string(),
                ))?,
            })
        })
        .collect::<Result<Vec<_>, _>>()
}
