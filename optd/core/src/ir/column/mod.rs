mod metadata;
mod set;

pub use metadata::{ColumnMeta, ColumnMetaStore};
pub use set::ColumnSet;
pub type ColumnMap<T> = std::collections::HashMap<Column, T>;

/// A column of data in the operator. Also known as an information unit (IU).
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
