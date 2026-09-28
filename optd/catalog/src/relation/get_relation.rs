use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder,
};

use crate::{
    entity::{column, prelude::*, relation},
    relation::{GetRelationInfo, RelationInfo, build_columns},
    snapshot::SnapshotInfo,
};

pub(crate) async fn get_relation<C>(
    info: GetRelationInfo,
    db: &C,
    current_snapshot: &SnapshotInfo,
) -> Result<Option<RelationInfo>, DbErr>
where
    C: ConnectionTrait,
{
    let snapshot_id = current_snapshot.snapshot_id.as_i64();

    let Some(relation) = Relation::find()
        .filter(relation::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(relation::Column::EndSnapshot.is_null())
                .add(relation::Column::EndSnapshot.gt(snapshot_id)),
        )
        .filter(relation::Column::RelationId.eq(info.relation_id))
        .one(db)
        .await?
    else {
        return Ok(None);
    };

    let columns = Column::find()
        .filter(column::Column::RelationId.eq(relation.relation_id))
        .filter(column::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(column::Column::EndSnapshot.is_null())
                .add(column::Column::EndSnapshot.gt(snapshot_id)),
        )
        .order_by_asc(column::Column::ColumnOrder)
        .all(db)
        .await?;

    Ok(Some(RelationInfo {
        relation_id: relation.relation_id,
        schema_id: relation.schema_id,
        relation_name: relation.relation_name,
        columns: build_columns(columns),
    }))
}
