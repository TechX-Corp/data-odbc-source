use crate::{error::Error, odbc::cap_text_type_length};
use arrow::datatypes::{DataType, Field, TimeUnit};

use odbc_api::{ColumnDescription, ResultSetMetadata};

/// Infers the Arrow [`Field`]s from a [`ResultSetMetadata`]
pub fn infer_schema(resut_set_metadata: &mut impl ResultSetMetadata) -> Result<Vec<Field>, Error> {
    let num_cols: u16 = resut_set_metadata.num_result_cols()? as u16;

    let fields = (0..num_cols)
        .map(|index| {
            let mut column_description = ColumnDescription::default();
            resut_set_metadata
                .describe_col(index + 1, &mut column_description)
                .unwrap();
            // println!("{:?}", column_description);
            column_to_field(&column_description)
        })
        .collect();
    Ok(fields)
}

fn column_to_field(column_description: &ColumnDescription) -> Field {
    Field::new(
        &column_description
            .name_to_string()
            .expect("Column name must be representable in utf8"),
        column_to_data_type(&column_description.data_type),
        column_description.could_be_nullable(),
    )
}

fn column_to_data_type(data_type: &odbc_api::DataType) -> DataType {
    use odbc_api::DataType as OdbcDataType;
    match data_type {
        OdbcDataType::Numeric {
            precision: p @ 0..=38,
            scale: scale @ -127..=127,
        }
        | OdbcDataType::Decimal {
            precision: p @ 0..=38,
            scale: scale @ -127..=127,
        } => DataType::Decimal128(*p as u8, (*scale) as i8),
        OdbcDataType::Integer => DataType::Int32,
        OdbcDataType::SmallInt => DataType::Int16,
        OdbcDataType::Real | OdbcDataType::Float { precision: 0..=24 } => DataType::Float32,
        OdbcDataType::Float { precision: _ } | OdbcDataType::Double => DataType::Float64,
        OdbcDataType::Date => DataType::Date32,
        // OdbcDataType::Timestamp { precision: 0 } => DataType::Timestamp(TimeUnit::Second, None),
        OdbcDataType::Timestamp { precision: _ } => {
            DataType::Timestamp(TimeUnit::Millisecond, None)
        }
        // OdbcDataType::Timestamp { precision: 4..=6 } => {
        //     DataType::Timestamp(TimeUnit::Microsecond, None)
        // }
        // OdbcDataType::Timestamp { precision: _ } => DataType::Timestamp(TimeUnit::Nanosecond, None),
        OdbcDataType::BigInt => DataType::Int64,
        OdbcDataType::TinyInt => DataType::Int8,
        OdbcDataType::Bit => DataType::Boolean,
        OdbcDataType::Binary { length } => {
            DataType::FixedSizeBinary(cap_text_type_length(*length).unwrap().get() as i32)
        }
        OdbcDataType::LongVarbinary { length: _ } | OdbcDataType::Varbinary { length: _ } => {
            DataType::LargeBinary
        }
        OdbcDataType::Time { precision: _ } => DataType::Time32(TimeUnit::Second), // Regardless of precision, ODBC time is always in seconds
        OdbcDataType::Numeric { .. }
        | OdbcDataType::Decimal { .. }
        | OdbcDataType::WChar { length: _ }
        | OdbcDataType::Char { length: _ }
        | OdbcDataType::WVarchar { length: _ }
        | OdbcDataType::LongVarchar { length: _ }
        | OdbcDataType::Varchar { length: _ }
        | OdbcDataType::Other {
            data_type: odbc_api::sys::SqlDataType::EXT_GUID,
            column_size: _,
            decimal_digits: _,
        }
        | odbc_api::DataType::Other {
            data_type: odbc_api::sys::SqlDataType::EXT_W_LONG_VARCHAR,
            column_size: _,
            decimal_digits: _,
        } => DataType::LargeUtf8,
        _ => DataType::Binary,
    }
}
