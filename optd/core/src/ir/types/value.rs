//! A module for representing nullable scalar values in the IR.

use std::{
    convert::Infallible,
    hash::{Hash, Hasher},
    str::FromStr,
};

use crate::ir::DataType;

#[derive(Debug, Clone)]
pub enum ScalarValue {
    /// True or false.
    Boolean(Option<bool>),
    /// 32-bit float.
    Float32(Option<f32>),
    /// 64-bit float.
    Float64(Option<f64>),
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
    /// 32-bit decimal, using the i32 to represent the decimal, precision scale.
    Decimal32(Option<i32>, u8, i8),
    /// 64-bit decimal, using the i64 to represent the decimal, precision scale.
    Decimal64(Option<i64>, u8, i8),
    /// 128-bit decimal, using the i128 to represent the decimal, precision scale.
    Decimal128(Option<i128>, u8, i8),
}

impl ScalarValue {
    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Boolean(v) => v.is_none(),
            ScalarValue::Float32(v) => v.is_none(),
            ScalarValue::Float64(v) => v.is_none(),
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
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
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

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        use ScalarValue::*;

        match (self, other) {
            (Boolean(v1), Boolean(v2)) => v1 == v2,
            (Boolean(_), _) => false,
            (Float32(v1), Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1 == v2,
            },
            (Float32(_), _) => false,
            (Float64(v1), Float64(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1 == v2,
            },
            (Float64(_), _) => false,
            (Int8(v1), Int8(v2)) => v1 == v2,
            (Int8(_), _) => false,
            (Int16(v1), Int16(v2)) => v1 == v2,
            (Int16(_), _) => false,
            (Int32(v1), Int32(v2)) => v1 == v2,
            (Int32(_), _) => false,
            (Int64(v1), Int64(v2)) => v1 == v2,
            (Int64(_), _) => false,
            (UInt8(v1), UInt8(v2)) => v1 == v2,
            (UInt8(_), _) => false,
            (UInt16(v1), UInt16(v2)) => v1 == v2,
            (UInt16(_), _) => false,
            (UInt32(v1), UInt32(v2)) => v1 == v2,
            (UInt32(_), _) => false,
            (UInt64(v1), UInt64(v2)) => v1 == v2,
            (UInt64(_), _) => false,
            (Utf8(v1), Utf8(v2)) => v1 == v2,
            (Utf8(_), _) => false,
            (Utf8View(v1), Utf8View(v2)) => v1 == v2,
            (Utf8View(_), _) => false,
            (Date32(v1), Date32(v2)) => v1 == v2,
            (Date32(_), _) => false,
            (Date64(v1), Date64(v2)) => v1 == v2,
            (Date64(_), _) => false,
            (Decimal32(v1, p1, s1), Decimal32(v2, p2, s2)) => v1 == v2 && p1 == p2 && s1 == s2,
            (Decimal32(_, _, _), _) => false,
            (Decimal64(v1, p1, s1), Decimal64(v2, p2, s2)) => v1 == v2 && p1 == p2 && s1 == s2,
            (Decimal64(_, _, _), _) => false,
            (Decimal128(v1, p1, s1), Decimal128(v2, p2, s2)) => v1 == v2 && p1 == p2 && s1 == s2,
            (Decimal128(_, _, _), _) => false,
        }
    }
}

impl Eq for ScalarValue {}

struct Fl<T>(T);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+ $(,)?) => {
        $(
            impl Hash for Fl<$t> {
                fn hash<H: Hasher>(&self, state: &mut H) {
                    state.write(&<$i>::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
                }
            }
        )+
    };
}

hash_float_value!((f32, u32), (f64, u64));

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use ScalarValue::*;

        match self {
            Boolean(v) => v.hash(state),
            Float32(v) => v.map(Fl).hash(state),
            Float64(v) => v.map(Fl).hash(state),
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            Utf8(v) | Utf8View(v) => v.hash(state),
            Date32(v) => v.hash(state),
            Date64(v) => v.hash(state),
            Decimal32(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state);
            }
            Decimal64(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state);
            }
            Decimal128(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state);
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

impl_scalar!(f32, Float32);
impl_scalar!(f64, Float64);
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
            ScalarValue::Float32(v) => fmt_optional(f, v, "float32"),
            ScalarValue::Float64(v) => fmt_optional(f, v, "float64"),
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::ScalarValue;
    use crate::ir::DataType;

    #[test]
    fn float_values_report_types_and_nullability() {
        assert_eq!(
            ScalarValue::Float32(Some(1.0)).data_type(),
            DataType::Float32
        );
        assert_eq!(
            ScalarValue::Float64(Some(1.0)).data_type(),
            DataType::Float64
        );
        assert!(ScalarValue::Float32(None).is_null());
        assert!(ScalarValue::Float64(None).is_null());
    }

    #[test]
    fn float_values_compare_and_hash_by_bits() {
        let nan32_a = ScalarValue::Float32(Some(f32::from_bits(0x7fc0_0001)));
        let nan32_b = ScalarValue::Float32(Some(f32::from_bits(0x7fc0_0001)));
        let nan32_c = ScalarValue::Float32(Some(f32::from_bits(0x7fc0_0002)));

        assert_eq!(nan32_a, nan32_b);
        assert_ne!(nan32_a, nan32_c);

        let neg_zero = ScalarValue::Float64(Some(-0.0));
        let pos_zero = ScalarValue::Float64(Some(0.0));
        assert_ne!(neg_zero, pos_zero);

        let mut set = HashSet::new();
        set.insert(nan32_a.clone());
        assert!(set.contains(&nan32_b));
        assert!(!set.contains(&nan32_c));
    }
}
