use std::collections::BTreeSet;

use sea_orm::{
    ActiveValue::{NotSet, Set},
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter,
};

use crate::{
    CatalogError, CatalogResult,
    entity::{index as index_entity, prelude::Index as IndexEntity},
    index::{CreateIndexInfo, IndexFieldRole},
    relation::{GetRelationInfo, StorageKind, get_relation},
    snapshot::SnapshotInfo,
};

pub(crate) async fn create_index<C: ConnectionTrait>(
    info: CreateIndexInfo,
    index_id: i64,
    db: &C,
    read_snapshot: &SnapshotInfo,
    begin_snapshot: i64,
) -> CatalogResult<()> {
    let relation = get_relation(
        GetRelationInfo {
            relation_id: info.relation_id,
        },
        db,
        read_snapshot,
    )
    .await?
    .ok_or(CatalogError::RelationNotFound {
        relation_id: info.relation_id,
    })?;
    if relation.storage_kind != StorageKind::Relational {
        return Err(CatalogError::IndexUnsupportedStorage {
            relation_id: info.relation_id,
            storage_kind: relation.storage_kind,
        });
    }

    validate_definition(&info, &relation)?;
    let snapshot_id = read_snapshot.snapshot_id.as_i64();
    let duplicate = IndexEntity::find()
        .filter(index_entity::Column::RelationId.eq(info.relation_id))
        .filter(index_entity::Column::IndexName.eq(&info.index_name))
        .filter(index_entity::Column::BeginSnapshot.lte(snapshot_id))
        .filter(
            Condition::any()
                .add(index_entity::Column::EndSnapshot.is_null())
                .add(index_entity::Column::EndSnapshot.gt(snapshot_id)),
        )
        .one(db)
        .await?;
    if duplicate.is_some() {
        return Err(CatalogError::IndexAlreadyExists {
            relation_id: info.relation_id,
            index_name: info.index_name,
        });
    }

    let fields =
        serde_json::to_value(&info.fields).map_err(|source| CatalogError::IndexFields {
            reason: source.to_string(),
        })?;
    IndexEntity::insert(index_entity::ActiveModel {
        row_id: NotSet,
        index_id: Set(index_id),
        relation_id: Set(info.relation_id),
        begin_snapshot: Set(begin_snapshot),
        end_snapshot: Set(None),
        index_name: Set(info.index_name),
        access_method: Set(info.access_method),
        is_unique: Set(info.is_unique),
        nulls_not_distinct: Set(info.nulls_not_distinct),
        fields: Set(fields),
        status: Set(info.status),
    })
    .exec(db)
    .await?;
    Ok(())
}

fn validate_definition(
    info: &CreateIndexInfo,
    relation: &crate::relation::RelationInfo,
) -> CatalogResult<()> {
    if info.index_name.trim().is_empty() {
        return Err(CatalogError::InvalidIndexDefinition {
            reason: "index name must not be empty".to_owned(),
        });
    }
    if info.access_method.trim().is_empty() {
        return Err(CatalogError::InvalidIndexDefinition {
            reason: "access method must not be empty".to_owned(),
        });
    }
    if info.fields.is_empty() || info.fields[0].role != IndexFieldRole::Key {
        return Err(CatalogError::InvalidIndexDefinition {
            reason: "an index must have at least one key field".to_owned(),
        });
    }

    let visible_column_ids = relation
        .columns
        .iter()
        .map(|column| column.column_id)
        .collect::<BTreeSet<_>>();
    let mut seen_column_ids = BTreeSet::new();
    let mut saw_include = false;
    for field in &info.fields {
        if !visible_column_ids.contains(&field.column_id) {
            return Err(CatalogError::IndexColumnNotFound {
                relation_id: info.relation_id,
                column_id: field.column_id,
            });
        }
        if !seen_column_ids.insert(field.column_id) {
            return Err(CatalogError::InvalidIndexDefinition {
                reason: format!("column {} appears more than once", field.column_id),
            });
        }
        match field.role {
            IndexFieldRole::Key if saw_include => {
                return Err(CatalogError::InvalidIndexDefinition {
                    reason: "key fields must precede included fields".to_owned(),
                });
            }
            IndexFieldRole::Key => {}
            IndexFieldRole::Include => {
                saw_include = true;
                if field.direction.is_some()
                    || field.null_order.is_some()
                    || field.collation.is_some()
                    || field.operator_class.is_some()
                {
                    return Err(CatalogError::InvalidIndexDefinition {
                        reason: "included fields cannot specify key properties".to_owned(),
                    });
                }
            }
        }
    }
    Ok(())
}
