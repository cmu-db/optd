//! The physical expression object.

use diesel::{prelude::*, sql_types::BigInt};

use crate::{define_diesel_new_id_type_from_to_sql, storage::schema};

use super::{physical_operators::PhysicalOpKindId, rel_group::RelGroupId};

// Identifier for a physical expression.
define_diesel_new_id_type_from_to_sql!(PhysicalExprId, i64, BigInt);

#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = schema::physical_exprs)]
#[diesel(belongs_to(RelGroup))]
#[diesel(belongs_to(PhysicalOpKind))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalExpr {
    /// The physical expression id unique to the database.
    pub id: PhysicalExprId,
    /// The type descriptor of the physical expression.
    pub physical_op_kind_id: PhysicalOpKindId,
    /// The relational group that this physical expression belongs to.
    pub group_id: RelGroupId,
    /// The time at which this physical expression was created.
    pub created_at: chrono::NaiveDateTime,
}
