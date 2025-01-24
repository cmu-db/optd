use diesel::prelude::*;

use crate::storage::{
    models::logical_expr::{LogicalExprId, LogicalExprStorage, LogicalOp},
    schema::logical_scans,
    StorageManager,
};

#[derive(Debug, Queryable, Insertable, Selectable)]
#[diesel(table_name = logical_scans)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalScan {
    /// The name of the table to scan.
    /// TODO(yuchen): Eventually this should become the unique identifier
    /// for the table in the catalog.
    pub table_name: String,
}

impl LogicalOp for LogicalScan {
    const NAME: &'static str = "LogicalScan";

    fn get(id: LogicalExprId, storage: &mut StorageManager) -> Self {
        use crate::storage::schema::logical_scans::dsl::*;
        logical_scans
            .filter(logical_expr_id.eq(id))
            .select(LogicalScan::as_select())
            .first::<LogicalScan>(&mut storage.conn)
            .expect("failed to get logical scan")
    }
}

impl LogicalExprStorage for LogicalScan {
    fn id(&self, storage: &mut StorageManager) -> Option<LogicalExprId> {
        use self::logical_scans::dsl::*;
        logical_scans
            .filter(table_name.eq(&self.table_name))
            .select(logical_expr_id)
            .first::<LogicalExprId>(&mut storage.conn)
            .ok()
    }

    fn insert_op(&self, id: LogicalExprId, storage: &mut StorageManager) {
        use crate::storage::schema::logical_scans::dsl::*;
        diesel::insert_into(logical_scans)
            .values((logical_expr_id.eq(id), self))
            .execute(&mut storage.conn)
            .expect("failed to insert new logical scan");
    }

    fn name(&self) -> &'static str {
        <Self as LogicalOp>::NAME
    }
}
