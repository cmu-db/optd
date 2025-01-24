use diesel::prelude::*;

use crate::storage::schema::logical_scans;

#[derive(Debug, Queryable, Insertable, Selectable)]
#[diesel(table_name = logical_scans)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalScan {
    /// The name of the table to scan.
    /// TODO(yuchen): Eventually this should become the unique identifier
    /// for the table in the catalog.
    pub table_name: String,
}
