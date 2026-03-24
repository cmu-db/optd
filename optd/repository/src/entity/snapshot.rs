use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_snapshot")]
pub struct Model {
    /// The continuously increasing numeric identifier of the snapshot.
    #[sea_orm(primary_key)]
    pub snapshot_id: i64,
    /// The timestamp at which the snapshot was created.
    pub snapshot_time: DateTime,
    /// A continuously increasing number that is incremented whenever the schema is changed,
    /// e.g., by creating a table. This allows for caching of schema information if only
    /// data is changed.
    pub schema_version: i64,
    /// A continuously increasing number that describes the next identifier for schemas, tables, views, partitions,
    /// and column name mappings.
    pub next_catalog_id: i64,
    /// A continuously increasing number that contains the next id for a data or deletion file to be added.
    pub next_file_id: i64,

    #[sea_orm(has_one)]
    pub snapshot_changes: HasOne<super::snapshot_changes::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
