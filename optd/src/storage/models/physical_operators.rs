//! The physical operator objects in the optimizer storage layer.

pub mod filter;
pub mod nljoin;
pub mod table_scan;

pub use filter::*;
pub use nljoin::*;
pub use table_scan::*;

use diesel::{prelude::*, sql_types::BigInt};

use crate::{define_diesel_new_id_type_from_to_sql, storage::schema};

// Identifier for a physical operator descriptor.
define_diesel_new_id_type_from_to_sql!(PhysicalOpDescId, i64, BigInt);

/// Descriptor for a physical operator.
///
/// There is a descriptor for each physical operator supported
/// in the optimizer.
#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = schema::physical_op_descs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalOpDesc {
    /// The physical operator descriptor id unique to the database.
    pub id: PhysicalOpDescId,
    /// The name of the physical operator.
    pub name: String,
}
