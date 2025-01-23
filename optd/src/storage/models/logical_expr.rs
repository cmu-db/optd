//! The logical expression object.

use diesel::{prelude::*, sql_types::BigInt};

use crate::{define_diesel_new_id_type_from_to_sql, storage::schema};

use super::{logical_operators::LogicalOpDescId, rel_group::RelGroupId};

// Identifier for a logical expression.
define_diesel_new_id_type_from_to_sql!(LogicalExprId, i64, BigInt);

#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = schema::logical_exprs)]
#[diesel(belongs_to(RelGroup))]
#[diesel(belongs_to(LogicalOpDesc))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalExpr {
    /// The logical expression id unique to the database.
    pub id: LogicalExprId,
    /// The type descriptor of the logical expression.
    pub logical_op_desc_id: LogicalOpDescId,
    /// The relational group that this logical expression belongs to.
    pub group_id: RelGroupId,
    /// The time at which this logical expression was created.
    pub created_at: chrono::NaiveDateTime,
}
