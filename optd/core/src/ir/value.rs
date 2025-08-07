/// A dynamically typed, nullable single value.
// TODO(yuchen: Might require implementation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarValue {
    Int32(Option<i32>),
    Boolean(Option<bool>),
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
            ScalarValue::Int32(v) => fmt_optional(f, v, "int32"),
            ScalarValue::Boolean(v) => fmt_optional(f, v, "boolean"),
        }
    }
}
