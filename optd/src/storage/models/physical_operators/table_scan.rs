use diesel::prelude::*;

use crate::storage::schema::physical_table_scans;

#[derive(Debug, Queryable, Insertable)]
#[diesel(table_name = physical_table_scans)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalTableScan {
    /// The name of the table to scan.
    /// TODO(yuchen): Eventually this should become the unique identifier
    /// for the table in the catalog.
    pub table_name: String,
}
