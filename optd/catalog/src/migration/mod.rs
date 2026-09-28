//! Catalog database migrations ported from `origin/spring26`.

use sea_orm_migration::prelude::*;

mod initial;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(initial::Migration)]
    }
}
