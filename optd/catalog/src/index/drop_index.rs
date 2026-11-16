use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, sea_query::Expr};

use crate::{
    CatalogError, CatalogResult,
    entity::{index as index_entity, prelude::Index as IndexEntity},
    index::get_index,
    snapshot::SnapshotInfo,
};

pub(crate) async fn drop_index<C: ConnectionTrait>(
    index_id: i64,
    db: &C,
    read_snapshot: &SnapshotInfo,
    end_snapshot: i64,
) -> CatalogResult<()> {
    get_index(db, read_snapshot, index_id)
        .await?
        .ok_or(CatalogError::IndexNotFound { index_id })?;
    IndexEntity::update_many()
        .col_expr(index_entity::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(index_entity::Column::IndexId.eq(index_id))
        .filter(index_entity::Column::EndSnapshot.is_null())
        .exec(db)
        .await?;
    Ok(())
}

pub(crate) async fn close_relation_indexes<C: ConnectionTrait>(
    relation_id: i64,
    db: &C,
    end_snapshot: i64,
) -> CatalogResult<()> {
    IndexEntity::update_many()
        .col_expr(index_entity::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(index_entity::Column::RelationId.eq(relation_id))
        .filter(index_entity::Column::EndSnapshot.is_null())
        .exec(db)
        .await?;
    Ok(())
}
