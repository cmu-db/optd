//! The logical expression object.

use diesel::{prelude::*, sql_types::BigInt};

use crate::{
    define_diesel_new_id_type_from_to_sql,
    storage::{schema, StorageManager},
};

use super::{
    logical_operators::{LogicalFilter, LogicalJoin, LogicalOpKindId, LogicalScan},
    rel_group::RelGroupId,
};

// Identifier for a logical expression.
define_diesel_new_id_type_from_to_sql!(LogicalExprId, i64, BigInt);

#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = schema::logical_exprs)]
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

pub enum LogicalExpr {
    Scan(LogicalScan),
    Filter(LogicalFilter),
    Join(LogicalJoin),
}

pub trait LogicalStorage {
    fn get_group_expr(&self, conn: &mut StorageManager) -> Option<(LogicalExprId, RelGroupId)>;
    fn insert(&self, conn: &mut StorageManager) -> bool;
}
