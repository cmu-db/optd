use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QuerySelect,
};

use crate::{
    api::{
        schema::{GetSchemaInfo, SchemaInfo},
        snapshot::SnapshotInfo,
    },
    entity::{prelude::Schema, schema},
};

pub async fn get_schema<C>(
    info: GetSchemaInfo,
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<Option<SchemaInfo>, DbErr>
where
    C: ConnectionTrait,
{
    let snapshot_id = current_snapshot.snapshot_id;

    Schema::find()
        .filter(schema::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(schema::Column::EndSnapshot.is_null())
                .add(schema::Column::EndSnapshot.gt(snapshot_id)),
        )
        .filter(schema::Column::SchemaId.eq(info.schema_id))
        .select_only()
        .column(schema::Column::SchemaId)
        .column(schema::Column::SchemaUuid)
        .column(schema::Column::SchemaName)
        .into_partial_model::<SchemaInfo>()
        .one(db)
        .await
}
