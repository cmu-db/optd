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

    /// A visible schema already has the requested name.
    #[snafu(display("schema '{schema_name}' already exists"))]
    SchemaAlreadyExists { schema_name: String },

    /// Requested relation ID is not visible.
    #[snafu(display("relation {relation_id} not found"))]
    RelationNotFound { relation_id: i64 },

    /// Requested relation name is not visible in its schema.
    #[snafu(display("relation '{schema_name}.{relation_name}' not found"))]
    RelationNameNotFound {
        schema_name: String,
        relation_name: String,
    },

    /// A statistics payload references a column not visible in its relation.
    #[snafu(display("column {column_id} not found in relation {relation_id}"))]
    StatisticsColumnNotFound {
        /// Relation expected to own the referenced column.
        relation_id: i64,
        /// Relation-scoped durable column identifier.
        column_id: i64,
    },

    /// A statistics count cannot be represented by the persistence schema.
    #[snafu(display("statistics field '{field}' is out of range: {value}"))]
    StatisticsCountOutOfRange {
        /// Name of the count field being encoded or decoded.
        field: &'static str,
        /// Rejected value rendered without truncation.
        value: String,
    },

    /// A typed statistics bound could not be encoded or decoded.
    #[snafu(display("invalid statistics value: {source}"))]
    StatisticsValue {
        /// JSON encoding or decoding failure.
        source: serde_json::Error,
    },

    /// A statistics payload uses an unsupported typed-value encoding.
    #[snafu(display("unsupported statistics value encoding version {version}"))]
    UnsupportedStatisticsValueEncoding {
        /// Encoding version found in the persisted payload.
        version: i32,
    },

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
