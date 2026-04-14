//! This module defines the column type for operators in optd.
//! Columns are lightweight structures with only an identifier, and a
//! ColumnMetaStore is used to access more information about a Column

mod metadata;
mod set;

pub use metadata::ColumnMeta;
pub use set::ColumnSet;

/// A column of data in an operator. Also known as an information unit (IU).
/// The column is represented as (table_index, column_index), where each table index
/// is globally unique in a query plan.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Column(pub i64, pub usize);

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}.{}", self.0, self.1)
    }
}

impl std::fmt::Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}.{}", self.0, self.1)
    }
}
