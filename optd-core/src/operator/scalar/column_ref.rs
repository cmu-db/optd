/// Column reference using position index (e.g. #0 for first column)
#[derive(Clone)]
pub struct ColumnRef {
    /// Index of the column
    pub column_idx: usize,
}
