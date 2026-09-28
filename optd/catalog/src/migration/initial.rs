use sea_orm_migration::{
    prelude::*,
    sea_orm::{ActiveValue::Set, EntityTrait, Schema},
};

use crate::entity;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let schema = Schema::new(manager.get_database_backend());

        for statement in [
            schema
                .create_table_from_entity(entity::snapshot::Entity)
                .if_not_exists()
                .to_owned(),
            schema
                .create_table_from_entity(entity::schema::Entity)
                .if_not_exists()
                .to_owned(),
            schema
                .create_table_from_entity(entity::relation::Entity)
                .if_not_exists()
                .to_owned(),
            schema
                .create_table_from_entity(entity::column::Entity)
                .if_not_exists()
                .to_owned(),
        ] {
            manager.create_table(statement).await?;
        }

        if entity::snapshot::Entity::find()
            .one(manager.get_connection())
            .await?
            .is_none()
        {
            entity::snapshot::Entity::insert(entity::snapshot::ActiveModel {
                next_catalog_id: Set(1),
                ..Default::default()
            })
            .exec(manager.get_connection())
            .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for table in [
            Table::drop()
                .table(entity::column::Entity)
                .if_exists()
                .to_owned(),
            Table::drop()
                .table(entity::relation::Entity)
                .if_exists()
                .to_owned(),
            Table::drop()
                .table(entity::schema::Entity)
                .if_exists()
                .to_owned(),
            Table::drop()
                .table(entity::snapshot::Entity)
                .if_exists()
                .to_owned(),
        ] {
            manager.drop_table(table).await?;
        }
        Ok(())
    }
}
