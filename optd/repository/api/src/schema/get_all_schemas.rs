use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect,
};

use crate::{
    entity::{prelude::Schema, schema},
    schema::SchemaInfo,
    snapshot::SnapshotInfo,
};

pub async fn get_all_schema_infos<C>(
    db: &C,
    current_snapshot: &mut SnapshotInfo,
) -> Result<Vec<SchemaInfo>, DbErr>
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
        .select_only()
        .column(schema::Column::SchemaId)
        .column(schema::Column::SchemaUuid)
        .column(schema::Column::SchemaName)
        .order_by_asc(schema::Column::SchemaName)
        .into_partial_model::<SchemaInfo>()
        .all(db)
        .await
}
