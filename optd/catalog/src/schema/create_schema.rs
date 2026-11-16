use crate::{
    CatalogError, CatalogResult,
    entity::schema,
    schema::{CreateSchemaInfo, get_all_schema_infos},
    snapshot::SnapshotInfo,
};
use sea_orm::{ActiveValue::Set, ConnectionTrait, EntityTrait};

pub(crate) async fn create_new_schema<C: ConnectionTrait>(
    info: CreateSchemaInfo,
    catalog_id: i64,
    schema_id: i64,
    db: &C,
    read: &SnapshotInfo,
    begin_snapshot: i64,
) -> CatalogResult<i64> {
    if get_all_schema_infos(db, read)
        .await?
        .iter()
        .any(|s| s.catalog_id == catalog_id && s.schema_name == info.schema_name)
    {
        return Err(CatalogError::SchemaAlreadyExists {
            schema_name: info.schema_name,
        });
    }
    schema::Entity::insert(schema::ActiveModel {
        catalog_id: Set(catalog_id),
        schema_id: Set(schema_id),
        begin_snapshot: Set(begin_snapshot),
        end_snapshot: Set(None),
        schema_name: Set(info.schema_name),
        ..Default::default()
    })
    .exec(db)
    .await?;
    Ok(schema_id)
}
