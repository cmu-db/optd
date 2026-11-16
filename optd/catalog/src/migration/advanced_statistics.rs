//! Add advanced objects without altering the existing basic statistics tables or rows.
use crate::entity::advanced_statistics::{self, Column};
use sea_orm_migration::{prelude::*, sea_orm::Schema};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Schema::new(manager.get_database_backend())
                    .create_table_from_entity(advanced_statistics::Entity),
            )
            .await?;
        for index in [
            Index::create()
                .name("idx_optd_adv_stats_version")
                .table(advanced_statistics::Entity)
                .col(Column::StatisticsId)
                .col(Column::BeginSnapshot)
                .unique()
                .to_owned(),
            Index::create()
                .name("idx_optd_adv_stats_visibility")
                .table(advanced_statistics::Entity)
                .col(Column::RelationId)
                .col(Column::BeginSnapshot)
                .col(Column::EndSnapshot)
                .to_owned(),
        ] {
            manager.create_index(index).await?;
        }
        // Names are relation-local and independent of contents kind.
        manager.get_connection().execute_unprepared(
            "CREATE UNIQUE INDEX idx_optd_adv_stats_active_name ON optd_adv_stats (relation_id, statistics_name) WHERE end_snapshot IS NULL"
        ).await?;
        Ok(())
    }

    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "advanced statistics downgrade requires restoring a pre-upgrade backup".into(),
        ))
    }
}
