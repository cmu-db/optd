use sea_orm::{
    ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
};

use crate::{
    api::{schema::SchemaInfo, snapshot::SnapshotInfo},
    entity::{prelude::Schema, schema},
};

pub async fn get_all_schema_infos<C>(
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<Vec<SchemaInfo>, DbErr>
where
    C: ConnectionTrait,
{
    Schema::find()
        .filter(schema::Column::BeginSnapshot.lte(current_snapshot.snapshot_id))
        .filter(schema::Column::EndSnapshot.is_null())
        .select_only()
        .column_as(schema::Column::SchemaId, "id")
        .column(schema::Column::SchemaUuid)
        .column(schema::Column::SchemaName)
        .order_by_asc(schema::Column::SchemaName)
        .into_partial_model::<SchemaInfo>()
        .all(db)
        .await
}
