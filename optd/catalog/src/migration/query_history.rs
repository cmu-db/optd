//! Operational query history does not participate in metadata snapshot publication.
use crate::entity::{query, query_instance, snapshot};
use sea_orm_migration::{prelude::*, sea_orm::Schema};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let schema = Schema::new(manager.get_database_backend());
        manager
            .create_table(schema.create_table_from_entity(query::Entity))
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_optd_query_fingerprint")
                    .table(query::Entity)
                    .col(query::Column::Fingerprint)
                    .unique()
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                schema
                    .create_table_from_entity(query_instance::Entity)
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_optd_query_instance_query")
                            .from(query_instance::Entity, query_instance::Column::QueryId)
                            .to(query::Entity, query::Column::QueryId),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_optd_query_instance_snapshot")
                            .from(
                                query_instance::Entity,
                                query_instance::Column::CatalogSnapshotId,
                            )
                            .to(snapshot::Entity, snapshot::Column::SnapshotId),
                    )
                    .to_owned(),
            )
            .await?;
        for (name, column) in [
            (
                "idx_optd_query_instance_query",
                query_instance::Column::QueryId,
            ),
            (
                "idx_optd_query_instance_snapshot",
                query_instance::Column::CatalogSnapshotId,
            ),
        ] {
            manager
                .create_index(
                    Index::create()
                        .name(name)
                        .table(query_instance::Entity)
                        .col(column)
                        .col(query_instance::Column::StartedAt)
                        .col(query_instance::Column::QueryInstanceId)
                        .to_owned(),
                )
                .await?;
        }
        manager
            .create_index(
                Index::create()
                    .name("idx_optd_query_instance_time")
                    .table(query_instance::Entity)
                    .col(query_instance::Column::StartedAt)
                    .col(query_instance::Column::QueryInstanceId)
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "query history downgrade requires restoring a pre-upgrade backup".into(),
        ))
    }
}
