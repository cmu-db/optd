use sea_orm::DbErr;
use snafu::Snafu;

use crate::SnapshotId;

/// Error returned by catalog reads and mutations.
#[derive(Debug, Snafu)]
pub enum CatalogError {
    /// Persistence-layer failure.
    #[snafu(transparent)]
    Database { source: DbErr },

    /// Requested branch does not exist.
    #[snafu(display("branch '{branch_name}' not found"))]
    BranchNotFound { branch_name: String },

    /// Requested snapshot does not exist.
    #[snafu(display("snapshot {snapshot_id} not found"))]
    SnapshotNotFound { snapshot_id: SnapshotId },

    /// A write was based on an outdated branch head.
    #[snafu(display(
        "stale snapshot for branch '{branch_name}': expected {expected}, current head is {actual}"
    ))]
    StaleSnapshot {
        branch_name: String,
        expected: SnapshotId,
        actual: SnapshotId,
    },

    /// Requested schema ID is not visible.
    #[snafu(display("schema {schema_id} not found"))]
    SchemaNotFound { schema_id: i64 },

    /// Requested schema name is not visible.
    #[snafu(display("schema '{schema_name}' not found"))]
    SchemaNameNotFound { schema_name: String },

    /// Requested relation is not visible.
    #[snafu(display("relation {relation_id} not found"))]
    RelationNotFound { relation_id: i64 },

    /// A visible relation already has the requested qualified name.
    #[snafu(display("relation '{schema_name}.{relation_name}' already exists"))]
    RelationAlreadyExists {
        schema_name: String,
        relation_name: String,
    },

    /// A schema cannot be dropped while it contains a visible relation.
    #[snafu(display(
        "cannot drop schema {schema_id}: active relation '{relation_name}' still exists"
    ))]
    SchemaNotEmpty {
        schema_id: i64,
        relation_name: String,
    },
}

/// Result returned by public catalog operations.
pub type CatalogResult<T> = Result<T, CatalogError>;
