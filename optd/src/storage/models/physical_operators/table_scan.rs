use diesel::prelude::*;

use crate::storage::{models::physical_expr::PhysicalExprId, schema};

#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = schema::physical_table_scans)]
#[diesel(primary_key(physical_expr_id))]
#[diesel(belongs_to(PhysicalExpr))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalTableScan {
    pub physical_expr_id: PhysicalExprId,
    /// The name of the table to scan.
    /// TODO(yuchen): Eventually this should become the unique identifier
    /// for the table in the catalog.
    pub table_name: String,
}
