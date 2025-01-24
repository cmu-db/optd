//! The logical expression object.

use diesel::{prelude::*, sql_types::BigInt};

use crate::{
    define_diesel_new_id_type_from_to_sql,
    storage::{schema::logical_exprs, StorageManager},
};

use super::{
    logical_operators::{LogicalFilter, LogicalJoin, LogicalOpKindId, LogicalScan},
    rel_group::RelGroupId,
};

// Identifier for a logical expression.
define_diesel_new_id_type_from_to_sql!(LogicalExprId, i64, BigInt);

#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = logical_exprs)]
#[diesel(belongs_to(RelGroup))]
#[diesel(belongs_to(LogicalOpKind))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalExprRecord {
    /// The logical expression id unique to the database.
    pub id: LogicalExprId,
    /// The type descriptor of the logical expression.
    pub logical_op_kind_id: LogicalOpKindId,
    /// The relational group that this logical expression belongs to.
    pub group_id: RelGroupId,
    /// The time at which this logical expression was created.
    pub created_at: chrono::NaiveDateTime,
}

#[derive(Debug)]
pub enum LogicalExpr {
    Scan(LogicalScan),
    Filter(LogicalFilter),
    Join(LogicalJoin),
}

#[derive(Debug)]
pub struct LogicalExprWithId {
    pub id: LogicalExprId,
    pub inner: LogicalExpr,
}

/// This trait defines the interface for working with logical expression in storage.
pub trait LogicalOperatorStorage {
    /// Gets the name of the logical operator.
    fn op_name() -> &'static str;

    /// Gets the logical expression id if it is already in the database.
    fn id(&self, storage: &mut StorageManager) -> Option<LogicalExprId>;

    /// Gets the logical expression using the id.
    fn get(id: LogicalExprId, storage: &mut StorageManager) -> Self;

    /// Inserts the logical operator into its own operator table.
    fn insert_op(&self, id: LogicalExprId, storage: &mut StorageManager);

    /// Gets the logical operator kind id.
    fn op_kind(&self, storage: &mut StorageManager) -> LogicalOpKindId {
        use crate::storage::schema::logical_op_kinds::dsl::*;

        logical_op_kinds
            .filter(name.eq(Self::op_name()))
            .select(id)
            .first::<LogicalOpKindId>(&mut storage.conn)
            .expect("Failed to get logical operator kind id for LogicalScan")
    }

    /// Gets the logical expression id and the relational group id if it is already in the database.
    fn get_identifiers(&self, storage: &mut StorageManager) -> Option<(LogicalExprId, RelGroupId)> {
        let id = self.id(storage)?;
        let rel_group_id = storage.rel_group_of_logical_expr(id);
        Some((id, rel_group_id))
    }

    /// Adds the logical expression to the database.
    /// If the logical expression is already in the database, it returns the logical expression id and the relational group id.
    /// Otherwise, it inserts the logical expression into the database and returns the generated logical expression id and
    /// the relational group id.
    fn add(&self, storage: &mut StorageManager) -> (LogicalExprId, RelGroupId) {
        if let Some((id, rel_group_id)) = self.get_identifiers(storage) {
            (id, rel_group_id)
        } else {
            let rel_group_id = storage.create_rel_group();
            let op_kind_id = self.op_kind(storage);
            let id = storage.add_logical_expr_record(op_kind_id, rel_group_id);
            self.insert_op(id, storage);
            (id, rel_group_id)
        }
    }

    /// Adds the logical expression to an existing relational group.
    /// If the logical expression is already in the database, it returns
    /// the logical expression id. Otherwise, it inserts the logical expression
    /// and returns the generated logical expression id.
    fn add_to_group(
        &self,
        rel_group_id: RelGroupId,
        storage: &mut StorageManager,
    ) -> LogicalExprId {
        if let Some(logcal_expr_id) = self.id(storage) {
            return logcal_expr_id;
        }
        let op_kind_id = self.op_kind(storage);
        let logical_expr_id = storage.add_logical_expr_record(op_kind_id, rel_group_id);
        self.insert_op(logical_expr_id, storage);
        logical_expr_id
    }
}
