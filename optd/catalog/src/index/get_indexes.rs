use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder};

use crate::{
    CatalogError, CatalogResult,
    entity::{index as index_entity, prelude::Index as IndexEntity},
    index::{IndexField, IndexInfo},
    relation::{GetRelationInfo, get_relation},
    snapshot::SnapshotInfo,
};

pub(crate) async fn get_index<C: ConnectionTrait>(
    db: &C,
    snapshot: &SnapshotInfo,
    index_id: i64,
) -> CatalogResult<Option<IndexInfo>> {
    let snapshot_id = snapshot.snapshot_id.as_i64();
    let Some(model) = IndexEntity::find()
        .filter(index_entity::Column::IndexId.eq(index_id))
        .filter(index_entity::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(index_entity::Column::EndSnapshot.is_null())
                .add(index_entity::Column::EndSnapshot.gt(snapshot_id)),
        )
        .order_by_desc(index_entity::Column::BeginSnapshot)
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    if get_relation(
        GetRelationInfo {
            relation_id: model.relation_id,
        },
        db,
        snapshot,
    )
    .await?
    .is_none()
    {
        return Ok(None);
    }
    decode_index(model).map(Some)
}

pub(crate) async fn get_relation_indexes<C: ConnectionTrait>(
    db: &C,
    snapshot: &SnapshotInfo,
    relation_id: i64,
) -> CatalogResult<Vec<IndexInfo>> {
    if get_relation(GetRelationInfo { relation_id }, db, snapshot)
        .await?
        .is_none()
    {
        return Err(CatalogError::RelationNotFound { relation_id });
    }

    let snapshot_id = snapshot.snapshot_id.as_i64();
    IndexEntity::find()
        .filter(index_entity::Column::RelationId.eq(relation_id))
        .filter(index_entity::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(index_entity::Column::EndSnapshot.is_null())
                .add(index_entity::Column::EndSnapshot.gt(snapshot_id)),
        )
        .order_by_asc(index_entity::Column::IndexName)
        .all(db)
        .await?
        .into_iter()
        .map(decode_index)
        .collect()
}

fn decode_index(model: index_entity::Model) -> CatalogResult<IndexInfo> {
    let fields = serde_json::from_value::<Vec<IndexField>>(model.fields).map_err(|source| {
        CatalogError::IndexFields {
            reason: source.to_string(),
        }
    })?;
    Ok(IndexInfo {
        index_id: model.index_id,
        relation_id: model.relation_id,
        index_name: model.index_name,
        access_method: model.access_method,
        is_unique: model.is_unique,
        nulls_not_distinct: model.nulls_not_distinct,
        fields,
        status: model.status,
    })
}
