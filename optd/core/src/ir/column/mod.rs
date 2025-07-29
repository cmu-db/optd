mod set;

pub use set::ColumnSet;

/// A column of data in the operator. Also known as an information unit (IU).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Column(pub i64);
