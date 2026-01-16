//! A simple data type system for IR values.

pub mod value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Int32,
    Int64,
    Boolean,
    Utf8,
    Utf8View,
}
