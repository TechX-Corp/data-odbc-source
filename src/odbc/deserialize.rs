use std::convert::TryInto;

use arrow::array::Decimal128Builder;
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int8Type, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
};
use chrono::{NaiveDate, NaiveDateTime};
use odbc_api::buffers::{AnySlice, BinColumnView, Item, TextColumnView};
use odbc_api::{decimal_text_to_i128, Bit};

use crate::error::Error;
use arrow::{
    array::{Array, BooleanBuilder, LargeBinaryBuilder, LargeStringBuilder, PrimitiveBuilder},
    datatypes::{ArrowPrimitiveType, DataType, Date32Type, Int64Type, Time32SecondType, TimeUnit},
};
use once_cell::sync::Lazy;

static UNIX_EPOCH: Lazy<NaiveDate> = Lazy::new(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());

pub fn deserialize_cv(
    column_view: AnySlice,
    data_type: &DataType,
) -> Result<Box<dyn Array>, Error> {
    Ok(match data_type {
        // DataType::Decimal128(p , s) if s == 0 && p < 3 => {
        //     let view = i8::as_nullable_slice(column_view).unwrap();

        // }
        // DataType::Decimal128(p , s) if s == 0 && p < 10 => {

        // }
        // DataType::Decimal128(p , s) if s == 0 && p < 19 => {

        // }
        DataType::Decimal128(p, s) => {
            let mut builder = Decimal128Builder::new();
            match (p, s) {
                (0_u8..=2_u8, 0) => {
                    let view = i8::as_nullable_slice(column_view).unwrap();
                    for opt in view.into_iter() {
                        if let Some(num) = opt {
                            builder.append_value(*num as i128);
                        } else {
                            builder.append_null();
                        }
                    }
                }
                (3_u8..=9_u8, 0) => {
                    let view = i32::as_nullable_slice(column_view).unwrap();
                    for opt in view.into_iter() {
                        if let Some(num) = opt {
                            builder.append_value(*num as i128);
                        } else {
                            builder.append_null();
                        }
                    }
                }
                (10..=18, 0) => {
                    let view = i64::as_nullable_slice(column_view).unwrap();
                    for opt in view.into_iter() {
                        if let Some(num) = opt {
                            builder.append_value(*num as i128);
                        } else {
                            builder.append_null();
                        }
                    }
                }
                _ => {
                    let view = column_view.as_text_view().unwrap();
                    for opt in view.iter() {
                        if let Some(text) = opt {
                            let num = decimal_text_to_i128(text, *s as usize);
                            builder.append_value(num);
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }
            Box::new(builder.with_data_type(data_type.clone()).finish())
        }
        _ => deserialize(column_view, data_type.clone())?,
    })
}

fn utf16(view: TextColumnView<u16>) -> Result<Box<dyn Array>, Error> {
    let item_capacity = view.len();
    let data_capacity = view.max_len() * item_capacity;
    // println!("UTF16 used. item_capacity: {}, data_capacity: {}", item_capacity, data_capacity);
    let mut builder = LargeStringBuilder::with_capacity(item_capacity, data_capacity);
    for value in view.iter() {
        let value = match value {
            Some(value) => Some(String::from_utf16(value.into()).map_err(|_| {
                Error::DataError("Failed to convert bytes to UTF-16 string".to_string())
            })?),
            None => None,
        };
        builder.append_option(value);
    }
    Ok(Box::new(builder.finish()))
}

/// Deserializes a [`AnySlice`] into an array of [`DataType`].
/// This is CPU-bounded
fn deserialize(column: AnySlice, data_type: DataType) -> Result<Box<dyn Array>, Error> {
    Ok(match column {
        AnySlice::Text(view) => utf8(view)?,
        AnySlice::WText(view) => utf16(view)?,
        AnySlice::Binary(view) => binary(view),
        AnySlice::Date(values) => date(values),
        AnySlice::Time(values) => time(values),
        AnySlice::Timestamp(values) => timestamp(data_type, values),
        AnySlice::F64(_) => primitive::<Float64Type>(column),
        AnySlice::F32(_) => primitive::<Float32Type>(column),
        AnySlice::I8(_) => primitive::<Int8Type>(column),
        AnySlice::I16(_) => primitive::<Int16Type>(column),
        AnySlice::I32(_) => primitive::<Int32Type>(column),
        AnySlice::I64(_) => primitive::<Int64Type>(column),
        AnySlice::U8(_) => primitive::<UInt8Type>(column),
        AnySlice::Bit(_) => bool(column),
        AnySlice::NullableDate(slice) => date_optional(slice.raw_values().0, slice.raw_values().1),
        AnySlice::NullableTime(slice) => time_optional(slice.raw_values().0, slice.raw_values().1),
        AnySlice::NullableTimestamp(slice) => {
            timestamp_optional(data_type, slice.raw_values().0, slice.raw_values().1)
        }
        AnySlice::NullableF64(_) => primitive_optional::<Float64Type>(column),
        AnySlice::NullableF32(_) => primitive_optional::<Float32Type>(column),
        AnySlice::NullableI8(_) => primitive_optional::<Int8Type>(column),
        AnySlice::NullableI16(_) => primitive_optional::<Int16Type>(column),
        AnySlice::NullableI32(_) => primitive_optional::<Int32Type>(column),
        AnySlice::NullableI64(_) => primitive_optional::<Int64Type>(column),
        AnySlice::NullableU8(_) => primitive_optional::<UInt8Type>(column),
        AnySlice::NullableBit(_) => bool_optional(column),
    })
}

fn primitive<T>(column_view: AnySlice) -> Box<dyn Array>
where
    T: ArrowPrimitiveType,
    T::Native: Item,
{
    let slice = T::Native::as_slice(column_view).unwrap();
    let mut builder = PrimitiveBuilder::<T>::with_capacity(slice.len());
    builder.append_slice(slice);
    Box::new(builder.finish())
}

fn primitive_optional<T>(column_view: AnySlice) -> Box<dyn Array>
where
    T: ArrowPrimitiveType,
    T::Native: Item,
{
    let values = T::Native::as_nullable_slice(column_view).unwrap();
    let mut builder = PrimitiveBuilder::<T>::with_capacity(values.len());
    for value in values {
        builder.append_option(value.copied());
    }
    Box::new(builder.finish())
}

fn bool(column_view: AnySlice) -> Box<dyn Array> {
    let values = Bit::as_slice(column_view).unwrap();
    let mut builder = BooleanBuilder::new();
    for bit in values {
        builder.append_value(bit.as_bool());
    }
    Box::new(builder.finish())
}

fn bool_optional(column_view: AnySlice) -> Box<dyn Array> {
    let values = Bit::as_nullable_slice(column_view).unwrap();
    let mut builder = BooleanBuilder::new();
    for bit in values {
        builder.append_option(bit.copied().map(Bit::as_bool))
    }
    Box::new(builder.finish())
}

fn binary(view: BinColumnView) -> Box<dyn Array> {
    let mut builder = LargeBinaryBuilder::new();
    for value in view.iter() {
        if let Some(bytes) = value {
            builder.append_value(bytes);
        } else {
            builder.append_null();
        }
    }
    Box::new(builder.finish())
}

fn utf8(view: TextColumnView<u8>) -> Result<Box<dyn Array>, Error> {
    let item_capacity = view.len();
    let data_capacity = view.max_len() * item_capacity;
    let mut builder = LargeStringBuilder::with_capacity(item_capacity, data_capacity);
    for value in view.iter() {
        let value = match value {
            Some(value) => Some(std::str::from_utf8(value).map_err(|_| {
                Error::DataError("Failed to convert bytes to UTF-8 string".to_string())
            })?),
            None => None,
        };
        builder.append_option(value);
    }
    Ok(Box::new(builder.finish()))
}

fn date(values: &[odbc_api::sys::Date]) -> Box<dyn Array> {
    let values = values.iter().map(days_since_epoch).collect::<Vec<_>>();
    let mut builder = PrimitiveBuilder::<Date32Type>::with_capacity(values.len());
    builder.append_slice(&values);
    Box::new(builder.finish())
}

fn date_optional(values: &[odbc_api::sys::Date], indicators: &[isize]) -> Box<dyn Array> {
    let values = values.iter().map(days_since_epoch).collect::<Vec<_>>();
    let mut builder = PrimitiveBuilder::<Date32Type>::with_capacity(values.len());
    builder.append_values(&values, &validity(indicators));
    Box::new(builder.finish())
}

fn days_since_epoch(date: &odbc_api::sys::Date) -> i32 {
    let date = NaiveDate::from_ymd_opt(date.year as i32, date.month as u32, date.day as u32)
        .unwrap_or(*UNIX_EPOCH);
    let duration = date.signed_duration_since(*UNIX_EPOCH);
    duration.num_days().try_into().unwrap_or(i32::MAX)
}

fn time(values: &[odbc_api::sys::Time]) -> Box<dyn Array> {
    let values = values.iter().map(time_since_midnight).collect::<Vec<_>>();
    let mut builder = PrimitiveBuilder::<Time32SecondType>::with_capacity(values.len());
    builder.append_slice(&values);
    Box::new(builder.finish())
}

fn time_since_midnight(date: &odbc_api::sys::Time) -> i32 {
    (date.hour as i32) * 60 * 60 + (date.minute as i32) * 60 + date.second as i32
}

fn time_optional(values: &[odbc_api::sys::Time], indicators: &[isize]) -> Box<dyn Array> {
    let values = values.iter().map(time_since_midnight).collect::<Vec<_>>();
    let mut builder = PrimitiveBuilder::<Time32SecondType>::with_capacity(values.len());
    builder.append_values(&values, &validity(indicators));
    Box::new(builder.finish())
}

fn timestamp(data_type: DataType, values: &[odbc_api::sys::Timestamp]) -> Box<dyn Array> {
    let unit = if let DataType::Timestamp(unit, _) = &data_type {
        unit
    } else {
        unreachable!()
    };
    match unit {
        TimeUnit::Second => timestamp_internal::<TimestampSecondType, _>(values, &[], timestamp_s),
        TimeUnit::Millisecond => {
            timestamp_internal::<TimestampMillisecondType, _>(values, &[], timestamp_ms)
        }
        TimeUnit::Microsecond => {
            timestamp_internal::<TimestampMicrosecondType, _>(values, &[], timestamp_us)
        }
        TimeUnit::Nanosecond => {
            timestamp_internal::<TimestampNanosecondType, _>(values, &[], timestamp_ns)
        }
    }
}

fn timestamp_optional(
    data_type: DataType,
    values: &[odbc_api::sys::Timestamp],
    indicators: &[isize],
) -> Box<dyn Array> {
    let unit = if let DataType::Timestamp(unit, _) = &data_type {
        unit
    } else {
        unreachable!()
    };
    match unit {
        TimeUnit::Second => {
            timestamp_internal::<TimestampSecondType, _>(values, indicators, timestamp_s)
        }
        TimeUnit::Millisecond => {
            timestamp_internal::<TimestampMillisecondType, _>(values, indicators, timestamp_ms)
        }
        TimeUnit::Microsecond => {
            timestamp_internal::<TimestampMicrosecondType, _>(values, indicators, timestamp_us)
        }
        TimeUnit::Nanosecond => {
            timestamp_internal::<TimestampNanosecondType, _>(values, indicators, timestamp_ns)
        }
    }
}

fn timestamp_internal<T, F>(
    values: &[odbc_api::sys::Timestamp],
    indicators: &[isize],
    parse_fn: F,
) -> Box<dyn Array>
where
    T: ArrowPrimitiveType,
    F: FnMut(&odbc_api::sys::Timestamp) -> T::Native,
{
    let values = values.iter().map(parse_fn).collect::<Vec<_>>();
    let mut builder = PrimitiveBuilder::<T>::with_capacity(values.len());
    if indicators.is_empty() {
        builder.append_slice(&values);
    } else {
        builder.append_values(&values, &validity(indicators));
    }
    Box::new(builder.finish())
}

fn validity(indicators: &[isize]) -> Vec<bool> {
    indicators.iter().map(|x| *x != -1).collect()
}

fn timestamp_to_naive(timestamp: &odbc_api::sys::Timestamp) -> Option<NaiveDateTime> {
    NaiveDate::from_ymd_opt(
        timestamp.year as i32,
        timestamp.month as u32,
        timestamp.day as u32,
    )
    .and_then(|x| {
        x.and_hms_nano_opt(
            timestamp.hour as u32,
            timestamp.minute as u32,
            timestamp.second as u32,
            /*
            https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types?view=sql-server-ver15
            [b] The value of the fraction field is [...] for a billionth of a second (one nanosecond) is 1.
            */
            timestamp.fraction,
        )
    })
}

fn timestamp_s(timestamp: &odbc_api::sys::Timestamp) -> i64 {
    timestamp_to_naive(timestamp)
        .map(|x| x.and_utc().timestamp())
        .unwrap_or(0)
}

fn timestamp_ms(timestamp: &odbc_api::sys::Timestamp) -> i64 {
    timestamp_to_naive(timestamp)
        .map(|x| x.and_utc().timestamp_millis())
        .unwrap_or(0)
}

fn timestamp_us(timestamp: &odbc_api::sys::Timestamp) -> i64 {
    timestamp_to_naive(timestamp)
        .map(|x| x.and_utc().timestamp_nanos_opt().unwrap_or(0) / 1000)
        .unwrap_or(0)
}

fn timestamp_ns(timestamp: &odbc_api::sys::Timestamp) -> i64 {
    timestamp_to_naive(timestamp)
        .map(|x| x.and_utc().timestamp_nanos_opt().unwrap_or(0))
        .unwrap_or(0)
}
