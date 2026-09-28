use itertools::Itertools;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder,
};

use crate::{
    entity::{column, prelude::*, relation},
    relation::{RelationInfo, build_columns},
    snapshot::SnapshotInfo,
};

pub(crate) async fn get_all_relation_infos<C>(
    db: &C,
    current_snapshot: &SnapshotInfo,
) -> Result<Vec<RelationInfo>, DbErr>
where
    C: ConnectionTrait,
{
    let snapshot_id = current_snapshot.snapshot_id.as_i64();
    let relations = Relation::find()
        .filter(relation::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(relation::Column::EndSnapshot.is_null())
                .add(relation::Column::EndSnapshot.gt(snapshot_id)),
        )
        .order_by_asc(relation::Column::RelationId)
        .all(db)
        .await?;

    if relations.is_empty() {
        return Ok(Vec::new());
    }

    let relation_ids = relations
        .iter()
        .map(|relation| relation.relation_id)
        .collect::<Vec<_>>();
    let columns = Column::find()
        .filter(column::Column::RelationId.is_in(relation_ids))
        .filter(column::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(column::Column::EndSnapshot.is_null())
                .add(column::Column::EndSnapshot.gt(snapshot_id)),
        )
        .order_by_asc(column::Column::RelationId)
        .order_by_asc(column::Column::ColumnOrder)
        .all(db)
        .await?;

    let mut columns_by_relation = columns
        .into_iter()
        .into_group_map_by(|column| column.relation_id);

    Ok(relations
        .into_iter()
        .map(|relation| RelationInfo {
            relation_id: relation.relation_id,
            schema_id: relation.schema_id,
            relation_name: relation.relation_name,
            relation_kind: relation.relation_kind,
            storage_kind: relation.storage_kind,
            columns: build_columns(
                columns_by_relation
                    .remove(&relation.relation_id)
                    .unwrap_or_default(),
            ),
        })
        .collect())
}
