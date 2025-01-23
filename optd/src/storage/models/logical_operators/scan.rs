use diesel::prelude::*;

use crate::storage::{models::logical_expr::LogicalExprId, schema};

#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = schema::logical_scans)]
#[diesel(primary_key(logical_expr_id))]
#[diesel(belongs_to(LogicalExpr))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalScan {
    pub logical_expr_id: LogicalExprId,
    /// The name of the table to scan.
    /// TODO(yuchen): Eventually this should become the unique identifier
    /// for the table in the catalog.
    pub table_name: String,
}
