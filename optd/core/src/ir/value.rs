/// A dynamically typed, nullable single value.
// TODO(yuchen: Might require implementation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarValue {
    Int32(Option<i32>),
    Boolean(Option<bool>),
}
