//! The only runtime publication path. The database transaction owns the writer mutex.
use super::{SnapshotChange, SnapshotId, get_current_snapshot_info};
use crate::{
    CatalogError, CatalogResult, advanced_statistics,
    entity::{snapshot, snapshot_change},
    index, relation, schema, source, statistics,
};
use sea_orm::{
    ActiveValue::{NotSet, Set},
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, DbErr, EntityTrait,
    IsolationLevel, TransactionTrait,
};

pub(crate) enum Mutation {
    RegisterCatalog(source::RegisterCatalogInfo),
    CreateSchema(schema::CreateSchemaInfo),
    DropSchema(schema::DropSchemaInfo),
    CreateRelation(relation::CreateBaseTableInfo),
    DropRelation(relation::DropRelationInfo),
    CreateIndex(index::CreateIndexInfo),
    DropIndex(index::DropIndexInfo),
    RefreshStatistics(statistics::SetRelationStatisticsInfo),
    InvalidateStatistics(i64),
    InvalidateStatisticsBatch(statistics::InvalidateStatisticsInfo),
    CreateAdvancedStatistics(advanced_statistics::CreateAdvancedStatisticsInfo),
    UpdateAdvancedStatistics(advanced_statistics::UpdateAdvancedStatisticsInfo),
    DropAdvancedStatistics(advanced_statistics::DropAdvancedStatisticsInfo),
}

pub(crate) async fn commit(
    db: &DatabaseConnection,
    catalog_id: i64,
    expected: SnapshotId,
    mutation: Mutation,
) -> CatalogResult<(i64, SnapshotId)> {
    let isolation =
        (db.get_database_backend() == DbBackend::Postgres).then_some(IsolationLevel::ReadCommitted);
    db.transaction_with_config(
        |txn| Box::pin(apply(txn, catalog_id, expected, mutation)),
        isolation,
        None,
    )
    .await
    .map_err(|e| match e {
        sea_orm::TransactionError::Connection(source) => CatalogError::Database { source },
        sea_orm::TransactionError::Transaction(error) => error,
    })
}

async fn apply(
    txn: &DatabaseTransaction,
    catalog_id: i64,
    expected: SnapshotId,
    mutation: Mutation,
) -> CatalogResult<(i64, SnapshotId)> {
    // This MUST be the first SQL statement. Even the no-op UPDATE locks the row in PG,
    // and obtains SQLite's writer reservation before establishing a read snapshot.
    let locked = txn
        .execute_unprepared(
            "UPDATE optd_catalog_metadata SET format_version = format_version WHERE id = 1",
        )
        .await?;
    if locked.rows_affected() != 1 {
        return Err(CatalogError::MissingWriterMutex);
    }
    let mut current =
        get_current_snapshot_info(txn)
            .await?
            .ok_or(CatalogError::SnapshotNotFound {
                snapshot_id: SnapshotId::INITIAL,
            })?;
    if current.snapshot_id != expected {
        return Err(CatalogError::StaleSnapshot {
            expected,
            actual: current.snapshot_id,
        });
    }
    let read = current.clone();
    if !source::get_catalogs(txn, &read)
        .await?
        .iter()
        .any(|c| c.catalog_id == catalog_id)
    {
        return Err(CatalogError::CatalogNotFound { catalog_id });
    }
    let reader = crate::CatalogReader {
        db: txn,
        snapshot: read.clone(),
        catalog_id,
    };
    match &mutation {
        Mutation::DropSchema(info) => {
            reader
                .get_schema(schema::GetSchemaInfo {
                    schema_id: info.schema_id,
                })
                .await?;
        }
        Mutation::DropRelation(info) => {
            reader
                .get_relation(relation::GetRelationInfo {
                    relation_id: info.relation_id,
                })
                .await?;
        }
        Mutation::CreateIndex(info) => {
            reader
                .get_relation(relation::GetRelationInfo {
                    relation_id: info.relation_id,
                })
                .await?;
        }
        Mutation::DropIndex(info) => {
            reader
                .get_index(index::GetIndexInfo {
                    index_id: info.index_id,
                })
                .await?;
        }
        Mutation::RefreshStatistics(info) => {
            reader
                .get_relation(relation::GetRelationInfo {
                    relation_id: info.relation_id,
                })
                .await?;
        }
        Mutation::CreateAdvancedStatistics(info) => {
            reader
                .get_relation(relation::GetRelationInfo {
                    relation_id: info.relation_id,
                })
                .await?;
        }
        Mutation::UpdateAdvancedStatistics(info) => {
            reader
                .get_advanced_statistics(info.statistics_id)
                .await?
                .ok_or(CatalogError::AdvancedStatisticsNotFound {
                    statistics_id: info.statistics_id,
                })?;
        }
        Mutation::DropAdvancedStatistics(info) => {
            reader
                .get_advanced_statistics(info.statistics_id)
                .await?
                .ok_or(CatalogError::AdvancedStatisticsNotFound {
                    statistics_id: info.statistics_id,
                })?;
        }
        Mutation::InvalidateStatisticsBatch(info) => {
            if info.relation_ids.is_empty() {
                return Err(CatalogError::EmptyStatisticsInvalidation);
            }
            for id in &info.relation_ids {
                reader
                    .get_relation(relation::GetRelationInfo { relation_id: *id })
                    .await?;
            }
        }
        Mutation::InvalidateStatistics(id) => {
            reader
                .get_relation(relation::GetRelationInfo { relation_id: *id })
                .await?;
        }
        _ => {}
    }
    let s = expected
        .as_i64()
        .checked_add(1)
        .ok_or(CatalogError::CounterExhausted {
            counter: "snapshot ID",
        })?;
    let (id, change) = match mutation {
        Mutation::RegisterCatalog(info) => {
            let id = current.get_next_catalog_id()?;
            source::register(txn, &read, id, s, info).await?;
            (id, SnapshotChange::RegisterCatalog { catalog_id: id })
        }
        Mutation::CreateSchema(info) => {
            let id = current.get_next_catalog_id()?;
            schema::create_new_schema(info, catalog_id, id, txn, &read, s).await?;
            (id, SnapshotChange::CreateSchema { schema_id: id })
        }
        Mutation::DropSchema(info) => {
            let id = info.schema_id;
            schema::get_schema(schema::GetSchemaInfo { schema_id: id }, txn, &read)
                .await?
                .ok_or(CatalogError::SchemaNotFound { schema_id: id })?;
            schema::drop_schema(info, txn, expected.as_i64(), s).await?;
            (id, SnapshotChange::DropSchema { schema_id: id })
        }
        Mutation::CreateRelation(info) => {
            let id = current.get_next_catalog_id()?;
            relation::create_relation(info.into(), catalog_id, id, txn, expected.as_i64(), s)
                .await?;
            (id, SnapshotChange::CreateRelation { relation_id: id })
        }
        Mutation::DropRelation(info) => {
            let id = info.relation_id;
            relation::get_relation(relation::GetRelationInfo { relation_id: id }, txn, &read)
                .await?
                .ok_or(CatalogError::RelationNotFound { relation_id: id })?;
            invalidate_statistics(txn, &read, id, s).await?;
            relation::drop_relation(info, txn, s).await?;
            index::close_relation_indexes(id, txn, s).await?;
            (id, SnapshotChange::DropRelation { relation_id: id })
        }
        Mutation::CreateIndex(info) => {
            let id = current.get_next_catalog_id()?;
            index::create_index(info, id, txn, &read, s).await?;
            (id, SnapshotChange::CreateIndex { index_id: id })
        }
        Mutation::DropIndex(info) => {
            let id = info.index_id;
            index::drop_index(id, txn, &read, s).await?;
            (id, SnapshotChange::DropIndex { index_id: id })
        }
        Mutation::CreateAdvancedStatistics(info) => {
            let relation_id = info.relation_id;
            let id = current.get_next_catalog_id()?;
            advanced_statistics::create(txn, &read, id, s, info).await?;
            (
                id,
                SnapshotChange::CreateAdvancedStatistics {
                    statistics_id: id,
                    relation_id,
                },
            )
        }
        Mutation::UpdateAdvancedStatistics(info) => {
            let id = info.statistics_id;
            let relation_id = advanced_statistics::get(txn, &read, id)
                .await?
                .ok_or(CatalogError::AdvancedStatisticsNotFound { statistics_id: id })?
                .relation_id;
            advanced_statistics::update(txn, &read, s, info).await?;
            (
                id,
                SnapshotChange::UpdateAdvancedStatistics {
                    statistics_id: id,
                    relation_id,
                },
            )
        }
        Mutation::DropAdvancedStatistics(info) => {
            let id = info.statistics_id;
            let relation_id = advanced_statistics::get(txn, &read, id)
                .await?
                .ok_or(CatalogError::AdvancedStatisticsNotFound { statistics_id: id })?
                .relation_id;
            advanced_statistics::close(txn, id, s).await?;
            (
                id,
                SnapshotChange::DropAdvancedStatistics {
                    statistics_id: id,
                    relation_id,
                },
            )
        }
        Mutation::RefreshStatistics(info) => {
            let relation_id = info.relation_id;
            let definition = statistics::get_statistics_definition(txn, &read, relation_id).await?;
            let (id, begin, create) = match definition {
                Some(d) => (d.statistics_id, d.begin_snapshot, false),
                None => (current.get_next_catalog_id()?, s, true),
            };
            statistics::insert_relation_statistics(txn, &read, s, id, begin, create, info).await?;
            (id, SnapshotChange::RefreshStatistics { relation_id })
        }
        Mutation::InvalidateStatisticsBatch(mut info) => {
            info.relation_ids.sort_unstable();
            info.relation_ids.dedup();
            for id in &info.relation_ids {
                invalidate_statistics(txn, &read, *id, s).await?;
            }
            (
                info.relation_ids[0],
                SnapshotChange::InvalidateStatisticsBatch {
                    relation_ids: info.relation_ids,
                    cause: info.cause,
                },
            )
        }
        Mutation::InvalidateStatistics(relation_id) => {
            invalidate_statistics(txn, &read, relation_id, s).await?;
            (
                relation_id,
                SnapshotChange::InvalidateStatistics { relation_id },
            )
        }
    };
    snapshot::Entity::insert(snapshot::ActiveModel {
        snapshot_id: Set(s),
        schema_version: Set(current
            .schema_version
            .checked_add(i64::from(change.structural()))
            .ok_or(CatalogError::CounterExhausted {
                counter: "schema version",
            })?),
        next_catalog_id: Set(current.next_catalog_id),
        snapshot_time: NotSet,
    })
    .exec(txn)
    .await?;
    snapshot_change::Entity::insert(snapshot_change::ActiveModel {
        snapshot_id: Set(s), changes_made: Set(serde_json::to_string(&change).map_err(|e| DbErr::Custom(e.to_string()))?),
        author: Set(None), commit_message: Set(None), extra_info: Set(None),
    }).exec(txn).await?;
    Ok((id, SnapshotId::from_raw(s)))
}

async fn invalidate_statistics(
    txn: &DatabaseTransaction,
    snapshot: &super::SnapshotInfo,
    relation_id: i64,
    end: i64,
) -> CatalogResult<()> {
    if let Some(definition) =
        statistics::get_statistics_definition(txn, snapshot, relation_id).await?
    {
        statistics::close_active_statistics_payload(
            txn,
            definition.statistics_id,
            definition.begin_snapshot,
            end,
        )
        .await?;
    }
    advanced_statistics::close_relation(txn, relation_id, end).await
}
