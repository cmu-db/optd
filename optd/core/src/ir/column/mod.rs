//! This module defines the column type for operators in optd.
//! Columns are lightweight structures with only an identifier, and a
//! ColumnMetaStore is used to access more information about a Column

mod metadata;
mod set;

pub use metadata::{ColumnMeta, ColumnMetaStore};
pub use set::ColumnSet;

/// A column of data in an operator. Also known as an information unit (IU).
/// The usize represents the identifier of the column in the system, and the
/// identifiers are globally unique in a query plan
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Column(pub usize);

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v#{}", self.0)
    }
}

impl std::fmt::Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v#{}", self.0)
    }
}
