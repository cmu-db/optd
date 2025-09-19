use crate::ir::DataType;

/// A dynamically typed, nullable single value.
// TODO(yuchen: Might require implementation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarValue {
    Int32(Option<i32>),
    Int64(Option<i64>),
    Boolean(Option<bool>),
    Utf8(Option<String>),
    Utf8View(Option<String>),
}

impl ScalarValue {
    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Int32(v) => v.is_none(),
            ScalarValue::Int64(v) => v.is_none(),
            ScalarValue::Boolean(v) => v.is_none(),
            ScalarValue::Utf8(v) => v.is_none(),
            ScalarValue::Utf8View(v) => v.is_none(),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::Utf8View(_) => DataType::Utf8View,
        }
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
        }
    }
}
