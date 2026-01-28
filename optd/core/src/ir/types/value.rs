//! A module for representing nullable scalar values in the IR.

use std::{convert::Infallible, str::FromStr};

use crate::ir::DataType;

// TODO(yuchen): Might require implementation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarValue {
    /// True or false.
    Boolean(Option<bool>),
    /// Signed 8-bit integer.
    Int8(Option<i8>),
    /// Signed 16-bit integer.
    Int16(Option<i16>),
    /// Signed 32-bit integer.
    Int32(Option<i32>),
    /// Signed 64-bit integer.
    Int64(Option<i64>),
    /// Unsigned 8-bit integer.
    UInt8(Option<u8>),
    /// Unsigned 16-bit integer.
    UInt16(Option<u16>),
    /// Unsigned 32-bit integer.
    UInt32(Option<u32>),
    /// Unsigned 64-bit integer.
    UInt64(Option<u64>),
    /// UTF-8 encoded string.
    Utf8(Option<String>),
    /// UTF-8 encoded string view.
    Utf8View(Option<String>),
    /// Date stored as a signed 32-bit int days since UNIX epoch 1970-01-01.
    Date32(Option<i32>),
    /// Date stored as a signed 64-bit int milliseconds since UNIX epoch 1970-01-01.
    Date64(Option<i64>),
    /// 32bit decimal, using the i32 to represent the decimal, precision scale
    Decimal32(Option<i32>, u8, i8),
    /// 64bit decimal, using the i64 to represent the decimal, precision scale
    Decimal64(Option<i64>, u8, i8),
    /// 128bit decimal, using the i128 to represent the decimal, precision scale
    Decimal128(Option<i128>, u8, i8),
}

impl ScalarValue {
    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Boolean(v) => v.is_none(),
            ScalarValue::Int8(v) => v.is_none(),
            ScalarValue::Int16(v) => v.is_none(),
            ScalarValue::Int32(v) => v.is_none(),
            ScalarValue::Int64(v) => v.is_none(),
            ScalarValue::UInt8(v) => v.is_none(),
            ScalarValue::UInt16(v) => v.is_none(),
            ScalarValue::UInt32(v) => v.is_none(),
            ScalarValue::UInt64(v) => v.is_none(),
            ScalarValue::Utf8(v) => v.is_none(),
            ScalarValue::Utf8View(v) => v.is_none(),
            ScalarValue::Date32(v) => v.is_none(),
            ScalarValue::Date64(v) => v.is_none(),
            ScalarValue::Decimal32(v, _, _) => v.is_none(),
            ScalarValue::Decimal64(v, _, _) => v.is_none(),
            ScalarValue::Decimal128(v, _, _) => v.is_none(),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::Utf8View(_) => DataType::Utf8View,
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Date64(_) => DataType::Date64,
            ScalarValue::Decimal32(_, precision, scale) => DataType::Decimal32(*precision, *scale),
            ScalarValue::Decimal64(_, precision, scale) => DataType::Decimal64(*precision, *scale),
            ScalarValue::Decimal128(_, precision, scale) => {
                DataType::Decimal128(*precision, *scale)
            }
        }
    }
}

macro_rules! impl_scalar {
    ($ty:ty, $scalar:tt) => {
        impl From<$ty> for ScalarValue {
            fn from(value: $ty) -> Self {
                ScalarValue::$scalar(Some(value))
            }
        }

        impl From<Option<$ty>> for ScalarValue {
            fn from(value: Option<$ty>) -> Self {
                ScalarValue::$scalar(value)
            }
        }
    };
}

impl_scalar!(i8, Int8);
impl_scalar!(i16, Int16);
impl_scalar!(i32, Int32);
impl_scalar!(i64, Int64);
impl_scalar!(bool, Boolean);
impl_scalar!(u8, UInt8);
impl_scalar!(u16, UInt16);
impl_scalar!(u32, UInt32);
impl_scalar!(u64, UInt64);

impl From<&str> for ScalarValue {
    fn from(value: &str) -> Self {
        Some(value).into()
    }
}

impl From<Option<&str>> for ScalarValue {
    fn from(value: Option<&str>) -> Self {
        let value = value.map(|s| s.to_string());
        value.into()
    }
}

impl FromStr for ScalarValue {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.into())
    }
}

impl From<String> for ScalarValue {
    fn from(value: String) -> Self {
        Some(value).into()
    }
}

impl From<Option<String>> for ScalarValue {
    fn from(value: Option<String>) -> Self {
        ScalarValue::Utf8(value)
    }
}

impl std::fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt_optional<T: std::fmt::Display>(
            f: &mut std::fmt::Formatter<'_>,
            optional: &Option<T>,
            type_name: &str,
        ) -> std::fmt::Result {
            match optional {
                Some(v) => write!(f, "{v}::{type_name}"),
                None => write!(f, "null::{type_name}"),
            }
        }

        match self {
            ScalarValue::Int32(v) => fmt_optional(f, v, "integer"),
            ScalarValue::Int64(v) => fmt_optional(f, v, "bigint"),
            ScalarValue::Boolean(v) => fmt_optional(f, v, "boolean"),
            ScalarValue::Utf8(v) => fmt_optional(f, v, "utf8"),
            ScalarValue::Utf8View(v) => fmt_optional(f, v, "utf8_view"),
            ScalarValue::Int8(v) => fmt_optional(f, v, "int8"),
            ScalarValue::Int16(v) => fmt_optional(f, v, "int16"),
            ScalarValue::UInt8(v) => fmt_optional(f, v, "uint8"),
            ScalarValue::UInt16(v) => fmt_optional(f, v, "uint16"),
            ScalarValue::UInt32(v) => fmt_optional(f, v, "uint32"),
            ScalarValue::UInt64(v) => fmt_optional(f, v, "uint64"),
            ScalarValue::Date32(v) => fmt_optional(
                f,
                &v.map(|v| {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    match epoch.checked_add_signed(chrono::Duration::try_days(v as i64).unwrap()) {
                        Some(date) => date.to_string(),
                        None => "".to_string(),
                    }
                }),
                "date32",
            ),
            ScalarValue::Date64(v) => fmt_optional(
                f,
                &v.map(|v| {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    match epoch.checked_add_signed(chrono::Duration::try_milliseconds(v).unwrap()) {
                        Some(date) => date.to_string(),
                        None => "".to_string(),
                    }
                }),
                "date64",
            ),
            ScalarValue::Decimal32(v, precision, scale) => match v {
                Some(val) => write!(f, "{}::decimal32({}, {})", val, precision, scale),
                None => write!(f, "null::decimal32({}, {})", precision, scale),
            },
            ScalarValue::Decimal64(v, precision, scale) => match v {
                Some(val) => write!(f, "{}::decimal64({}, {})", val, precision, scale),
                None => write!(f, "null::decimal64({}, {})", precision, scale),
            },
            ScalarValue::Decimal128(v, precision, scale) => match v {
                Some(val) => write!(f, "{}::decimal128({}, {})", val, precision, scale),
                None => write!(f, "null::decimal128({}, {})", precision, scale),
            },
        }
    }
}
