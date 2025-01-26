// ColumnRef
pub struct ColumnRef {
    column_index: usize,
}

pub enum Constant {
    Int(i64),
    Float(f64),
    String(String),
}