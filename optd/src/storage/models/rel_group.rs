//! The relational group object.

use diesel::prelude::*;
use diesel::sql_types::BigInt;

use crate::define_diesel_new_id_type_from_to_sql;
use crate::storage::schema;

// Identifier for a relational group.
define_diesel_new_id_type_from_to_sql!(RelGroupId, i64, BigInt);

/// A relational group contains a set of relational expressions
/// that are logically equivalent.
#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = schema::rel_groups)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct RelGroup {
    /// The relational group id unique to the database.
    pub id: RelGroupId,
    /// Timestamp at which the group was created.
    pub created_at: chrono::NaiveDateTime,
}
