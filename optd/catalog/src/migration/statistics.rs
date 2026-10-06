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
