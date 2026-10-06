use sea_orm_migration::prelude::*;

use crate::entity;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column("optd_statistics_payload", "value_encoding_version")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(entity::statistics_payload::Entity)
                        .add_column(
                            ColumnDef::new(
                                entity::statistics_payload::Column::ValueEncodingVersion,
                            )
                            .integer()
                            .not_null()
                            .default(1),
                        )
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager
            .has_column("optd_statistics_payload", "value_encoding_version")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(entity::statistics_payload::Entity)
                        .drop_column(entity::statistics_payload::Column::ValueEncodingVersion)
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}
