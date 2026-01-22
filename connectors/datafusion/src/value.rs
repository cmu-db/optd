//! Converts between DataFusion ScalarValue and optd ScalarValue.

use crate::OptdResult;
use datafusion::scalar::ScalarValue as DFScalarValue;
use optd_core::{connector_err, ir::ScalarValue as OptdScalarValue};

/// Converts a DataFusion ScalarValue to an optd ScalarValue.
/// Returns an error if the conversion is not implemented.
pub fn try_into_optd_value(value: DFScalarValue) -> OptdResult<OptdScalarValue> {
    match value {
        DFScalarValue::Boolean(v) => Ok(OptdScalarValue::Boolean(v)),
        DFScalarValue::Int8(v) => Ok(OptdScalarValue::Int8(v)),
        DFScalarValue::Int16(v) => Ok(OptdScalarValue::Int16(v)),
        DFScalarValue::Int32(v) => Ok(OptdScalarValue::Int32(v)),
        DFScalarValue::Int64(v) => Ok(OptdScalarValue::Int64(v)),
        DFScalarValue::UInt8(v) => Ok(OptdScalarValue::UInt8(v)),
        DFScalarValue::UInt16(v) => Ok(OptdScalarValue::UInt16(v)),
        DFScalarValue::UInt32(v) => Ok(OptdScalarValue::UInt32(v)),
        DFScalarValue::UInt64(v) => Ok(OptdScalarValue::UInt64(v)),
        DFScalarValue::Utf8(v) => Ok(OptdScalarValue::Utf8(v)),
        DFScalarValue::Utf8View(v) => Ok(OptdScalarValue::Utf8View(v)),
        DFScalarValue::Decimal32(v, p, s) => Ok(OptdScalarValue::Decimal32(v, p, s)),
        DFScalarValue::Decimal64(v, p, s) => Ok(OptdScalarValue::Decimal64(v, p, s)),
        DFScalarValue::Decimal128(v, p, s) => Ok(OptdScalarValue::Decimal128(v, p, s)),
        DFScalarValue::Date32(v) => Ok(OptdScalarValue::Date32(v)),
        DFScalarValue::Date64(v) => Ok(OptdScalarValue::Date64(v)),
        value => connector_err!(
            "Conversion from DataFusion ScalarValue {:?} is not implemented",
            value
        ),
    }
}

/// Converts an optd `ScalarValue` to a DataFusion `ScalarValue``.
pub fn from_optd_value(value: OptdScalarValue) -> DFScalarValue {
    match value {
        OptdScalarValue::Boolean(v) => DFScalarValue::Boolean(v),
        OptdScalarValue::Int8(v) => DFScalarValue::Int8(v),
        OptdScalarValue::Int16(v) => DFScalarValue::Int16(v),
        OptdScalarValue::Int32(v) => DFScalarValue::Int32(v),
        OptdScalarValue::Int64(v) => DFScalarValue::Int64(v),
        OptdScalarValue::UInt8(v) => DFScalarValue::UInt8(v),
        OptdScalarValue::UInt16(v) => DFScalarValue::UInt16(v),
        OptdScalarValue::UInt32(v) => DFScalarValue::UInt32(v),
        OptdScalarValue::UInt64(v) => DFScalarValue::UInt64(v),
        OptdScalarValue::Utf8(v) => DFScalarValue::Utf8(v),
        OptdScalarValue::Utf8View(v) => DFScalarValue::Utf8View(v),
        OptdScalarValue::Decimal32(v, p, s) => DFScalarValue::Decimal32(v, p, s),
        OptdScalarValue::Decimal64(v, p, s) => DFScalarValue::Decimal64(v, p, s),
        OptdScalarValue::Decimal128(v, p, s) => DFScalarValue::Decimal128(v, p, s),
        OptdScalarValue::Date32(v) => DFScalarValue::Date32(v),
        OptdScalarValue::Date64(v) => DFScalarValue::Date64(v),
    }
}
