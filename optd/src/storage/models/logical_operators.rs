//! The logical operator objects in the optimizer storage layer.

pub mod filter;
pub mod join;
pub mod scan;

pub use filter::*;
pub use join::*;
pub use scan::*;

use diesel::{prelude::*, sql_types::BigInt};

use crate::{define_diesel_new_id_type_from_to_sql, storage::schema};

// Identifier for a logical operator descriptor.
define_diesel_new_id_type_from_to_sql!(LogicalOpKindId, i64, BigInt);

/// Descriptor for a logical operator.
///
/// There is a descriptor for each logical operator supported
/// in the optimizer.
#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = schema::logical_op_kinds)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalOpKind {
    /// The logical operator descriptor id unique to the database.
    pub id: LogicalOpKindId,
    /// The name of the logical operator.
    pub name: String,
}
