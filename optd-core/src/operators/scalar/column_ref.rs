/// Column reference using position index (e.g. #0 for first column)
#[derive(Clone)]
pub struct ColumnRef<Metadata> {
    pub table: Metadata,
    pub column_name: Metadata,
}
