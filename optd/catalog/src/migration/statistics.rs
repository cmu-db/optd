use sea_orm_migration::{prelude::*, sea_orm::Schema};

use crate::entity;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let schema = Schema::new(manager.get_database_backend());
        for statement in [
            schema
                .create_table_from_entity(entity::statistics::Entity)
                .if_not_exists()
                .to_owned(),
            schema
                .create_table_from_entity(entity::statistics_payload::Entity)
                .if_not_exists()
                .to_owned(),
            schema
                .create_table_from_entity(entity::column_statistics::Entity)
                .if_not_exists()
                .to_owned(),
        ] {
            manager.create_table(statement).await?;
        }

        for statement in [
            Index::create()
                .if_not_exists()
                .name("idx_optd_statistics_visibility")
                .table(entity::statistics::Entity)
                .col(entity::statistics::Column::RelationId)
                .col(entity::statistics::Column::StatisticsKind)
                .col(entity::statistics::Column::BeginSnapshot)
                .col(entity::statistics::Column::EndSnapshot)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("idx_optd_statistics_payload_visibility")
                .table(entity::statistics_payload::Entity)
                .col(entity::statistics_payload::Column::StatisticsId)
                .col(entity::statistics_payload::Column::StatisticsBeginSnapshot)
                .col(entity::statistics_payload::Column::BeginSnapshot)
                .col(entity::statistics_payload::Column::EndSnapshot)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("idx_optd_column_statistics_payload")
                .table(entity::column_statistics::Entity)
                .col(entity::column_statistics::Column::StatisticsId)
                .col(entity::column_statistics::Column::StatisticsBeginSnapshot)
                .col(entity::column_statistics::Column::PayloadBeginSnapshot)
                .col(entity::column_statistics::Column::ColumnId)
                .to_owned(),
        ] {
            manager.create_index(statement).await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for table in [
            Table::drop()
                .table(entity::column_statistics::Entity)
                .if_exists()
                .to_owned(),
            Table::drop()
                .table(entity::statistics_payload::Entity)
                .if_exists()
                .to_owned(),
            Table::drop()
                .table(entity::statistics::Entity)
                .if_exists()
                .to_owned(),
        ] {
            manager.drop_table(table).await?;
        }
        Ok(())
    }
}
