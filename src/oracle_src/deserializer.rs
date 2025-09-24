use crate::error::Error;
use arrow::{
    array::{Array, Decimal128Builder, LargeBinaryBuilder, LargeStringBuilder, PrimitiveBuilder},
    datatypes::{DataType, Field, Float32Type, Float64Type, TimestampMillisecondType},
};
use std::sync::Arc;
use techx_rust_oracle::SqlValue;

fn deserialize_str(sv: &SqlValue, expected_len: usize) -> Result<Arc<dyn Array>, Error> {
    let mut arr = LargeStringBuilder::with_capacity(expected_len, expected_len * 4000); // 4000 is the max length of a string in oracle
    for offset in 0..expected_len {
        arr.append_option(sv.get_string(offset as isize)?)
    }
    Ok(Arc::new(arr.finish()))
}

fn deserialize_decimal(
    sv: &SqlValue,
    expected_len: usize,
    data_type: DataType,
) -> Result<Arc<dyn Array>, Error> {
    let mut arr = Decimal128Builder::with_capacity(expected_len);
    if let DataType::Decimal128(_, scale) = data_type {
        for offset in 0..expected_len {
            arr.append_option(sv.get_i128(offset as isize, scale as u32))
        }
        Ok(Arc::new(arr.with_data_type(data_type).finish()))
    } else {
        unreachable!()
    }
}

fn deserialize_ts(
    sv: &SqlValue,
    expected_len: usize,
    data_type: DataType,
) -> Result<Arc<dyn Array>, Error> {
    let mut arr = PrimitiveBuilder::<TimestampMillisecondType>::with_capacity(expected_len);
    for offset in 0..expected_len {
        arr.append_option(sv.get_timestamp(offset as isize))
    }
    Ok(Arc::new(arr.with_data_type(data_type).finish()))
}

// fn deserialize_date(sv: &SqlValue, expected_len: usize) -> Result<Arc<dyn Array>, Error> {
//     let mut arr = PrimitiveBuilder::<Int32Type>::with_capacity(expected_len);
//     for offset in 0..expected_len {
//         arr.append_option(
//             sv.get_timestamp(offset as isize)
//                 .map(|e| (e / 86400000) as i32),
//         )
//     }
//     Ok(Arc::new(arr.finish()))
// }

fn deserialize_f32(sv: &SqlValue, expected_len: usize) -> Result<Arc<dyn Array>, Error> {
    let mut arr = PrimitiveBuilder::<Float32Type>::with_capacity(expected_len);
    for offset in 0..expected_len {
        arr.append_option(sv.get_f32(offset as isize))
    }
    Ok(Arc::new(arr.finish()))
}

fn deserialize_f64(
    sv: &SqlValue,
    expected_len: usize,
    is_free_size_number: bool,
) -> Result<Arc<dyn Array>, Error> {
    let mut arr = PrimitiveBuilder::<Float64Type>::with_capacity(expected_len);
    for offset in 0..expected_len {
        if is_free_size_number {
            arr.append_option(sv.get_f64_from_bytes(offset as isize))
        } else {
            arr.append_option(sv.get_f64(offset as isize))
        }
    }
    Ok(Arc::new(arr.finish()))
}

fn deserialize_binary(sv: &SqlValue, expected_len: usize) -> Result<Arc<dyn Array>, Error> {
    let mut arr = LargeBinaryBuilder::with_capacity(expected_len, expected_len * 4000); // 4000 is the max length of a string in oracle
    for offset in 0..expected_len {
        arr.append_option(sv.get_blob(offset as isize)?)
    }
    Ok(Arc::new(arr.finish()))
}

pub fn deserialize_column(
    sv: &SqlValue,
    field: &Field,
    expected_len: usize,
) -> Result<Arc<dyn Array>, Error> {
    let is_free_size_number =
        field.metadata().get("is_free_size_number") == Some(&true.to_string());
    match field.data_type() {
        DataType::LargeUtf8 => deserialize_str(sv, expected_len),
        DataType::Decimal128(_, _) => {
            deserialize_decimal(sv, expected_len, field.data_type().clone())
        }
        DataType::Timestamp(_, _) => deserialize_ts(sv, expected_len, field.data_type().clone()),
        DataType::Float32 => deserialize_f32(sv, expected_len),
        DataType::Float64 => deserialize_f64(sv, expected_len, is_free_size_number),
        DataType::LargeBinary => deserialize_binary(sv, expected_len),
        // DataType::Date32 => deserialize_date(sv, expected_len),
        data_type => Err(Error::NotImplementedError(format!(
            "deserialization for type {data_type:?} is not implemented"
        ))),
    }
}
