//! Snapshot-pinned readers and the typed atomic mutation API.
use crate::{
    CatalogError, CatalogResult, advanced_statistics,
    entity::snapshot_change,
    index, relation, schema,
    snapshot::{self, Mutation, SnapshotChange, SnapshotId, SnapshotInfo},
    source, statistics,
};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbErr, EntityTrait};

/// Catalog backed by a connection. Transaction-backed handles support reads only;
/// mutations own a fresh top-level transaction and never publish through a savepoint.
///
/// ```compile_fail
/// use optd_catalog::{Catalog, SnapshotId, schema::CreateSchemaInfo};
/// use sea_orm::DatabaseTransaction;
/// fn cannot_publish_from_a_savepoint(catalog: Catalog<DatabaseTransaction>) {
///     let _ = catalog.create_schema(SnapshotId::INITIAL, CreateSchemaInfo { schema_name: "public".into() });
/// }
/// ```
///
/// Typed mutation helpers cannot bypass the commit layer:
/// ```compile_fail
/// use optd_catalog::schema::drop_schema;
/// ```
/// ```compile_fail
/// use optd_catalog::relation::drop_relation;
/// ```
pub struct Catalog<T> {
    db: T,
    catalog_id: i64,
}

/// Reader pinned to immutable planning metadata (not a source-data snapshot).
pub struct CatalogReader<'a, T> {
    pub(crate) db: &'a T,
    pub(crate) snapshot: SnapshotInfo,
    pub(crate) catalog_id: i64,
}

impl<T> Catalog<T> {
    pub fn new(db: T) -> Self {
        Self::on_catalog(db, source::LEGACY_CATALOG_ID)
    }
    /// Scopes name resolution and object access to a durable external catalog registration.
    pub fn on_catalog(db: T, catalog_id: i64) -> Self {
        Self { db, catalog_id }
    }
    pub fn connection(&self) -> &T {
        &self.db
    }
}

impl<T: ConnectionTrait> CatalogReader<'_, T> {
    pub fn snapshot_id(&self) -> SnapshotId {
        self.snapshot.snapshot_id
    }
    pub fn snapshot_info(&self) -> &SnapshotInfo {
        &self.snapshot
    }
    pub fn schema_version(&self) -> i64 {
        self.snapshot.schema_version
    }

    /// Typed summary atomically published with this snapshot.
    pub async fn snapshot_change(&self) -> CatalogResult<SnapshotChange> {
        let row = snapshot_change::Entity::find_by_id(self.snapshot_id().as_i64())
            .one(self.db)
            .await?
            .ok_or_else(|| DbErr::Custom("snapshot change summary missing".into()))?;
        serde_json::from_str(&row.changes_made).map_err(|e| DbErr::Custom(e.to_string()).into())
    }
    pub async fn get_catalogs(&self) -> CatalogResult<Vec<source::CatalogRegistration>> {
        source::get_catalogs(self.db, &self.snapshot).await
    }
    pub async fn get_all_schemas(&self) -> CatalogResult<Vec<schema::SchemaInfo>> {
        Ok(schema::get_all_schema_infos(self.db, &self.snapshot)
            .await?
            .into_iter()
            .filter(|s| s.catalog_id == self.catalog_id)
            .collect())
    }
    pub async fn get_schema(
        &self,
        info: schema::GetSchemaInfo,
    ) -> CatalogResult<schema::SchemaInfo> {
        let schema_id = info.schema_id;
        schema::get_schema(info, self.db, &self.snapshot)
            .await?
            .filter(|s| s.catalog_id == self.catalog_id)
            .ok_or(CatalogError::SchemaNotFound { schema_id })
    }
    pub async fn get_all_relations(&self) -> CatalogResult<Vec<relation::RelationInfo>> {
        let schemas = self.get_all_schemas().await?;
        Ok(relation::get_all_relation_infos(self.db, &self.snapshot)
            .await?
            .into_iter()
            .filter(|r| schemas.iter().any(|s| s.schema_id == r.schema_id))
            .collect())
    }
    pub async fn get_relation(
        &self,
        info: relation::GetRelationInfo,
    ) -> CatalogResult<relation::RelationInfo> {
        let relation_id = info.relation_id;
        let relation = relation::get_relation(info, self.db, &self.snapshot)
            .await?
            .ok_or(CatalogError::RelationNotFound { relation_id })?;
        match self
            .get_schema(schema::GetSchemaInfo {
                schema_id: relation.schema_id,
            })
            .await
        {
            Ok(_) => Ok(relation),
            Err(CatalogError::SchemaNotFound { .. }) => {
                Err(CatalogError::RelationNotFound { relation_id })
            }
            Err(error) => Err(error),
        }
    }
    pub async fn get_index(&self, info: index::GetIndexInfo) -> CatalogResult<index::IndexInfo> {
        let index_id = info.index_id;
        let index = index::get_index(self.db, &self.snapshot, index_id)
            .await?
            .ok_or(CatalogError::IndexNotFound { index_id })?;
        self.get_relation(relation::GetRelationInfo {
            relation_id: index.relation_id,
        })
        .await?;
        Ok(index)
    }
    pub async fn get_relation_indexes(
        &self,
        relation_id: i64,
    ) -> CatalogResult<Vec<index::IndexInfo>> {
        self.get_relation(relation::GetRelationInfo { relation_id })
            .await?;
        index::get_relation_indexes(self.db, &self.snapshot, relation_id).await
    }
    /// Returns a visible advanced object only when its owner belongs to this catalog.
    pub async fn get_advanced_statistics(
        &self,
        statistics_id: i64,
    ) -> CatalogResult<Option<advanced_statistics::AdvancedStatisticsInfo>> {
        let Some(row) = advanced_statistics::get(self.db, &self.snapshot, statistics_id).await?
        else {
            return Ok(None);
        };
        match self
            .get_relation(relation::GetRelationInfo {
                relation_id: row.relation_id,
            })
            .await
        {
            Ok(owner) => advanced_statistics::decode(row, &owner).map(Some),
            Err(CatalogError::RelationNotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }
    /// Advanced statistics for a visible relation, ordered by stable object ID.
    pub async fn get_relation_advanced_statistics(
        &self,
        relation_id: i64,
    ) -> CatalogResult<Vec<advanced_statistics::AdvancedStatisticsInfo>> {
        let owner = self
            .get_relation(relation::GetRelationInfo { relation_id })
            .await?;
        advanced_statistics::list(self.db, &self.snapshot, relation_id)
            .await?
            .into_iter()
            .map(|row| advanced_statistics::decode(row, &owner))
            .collect()
    }
    pub async fn get_relation_statistics(
        &self,
        relation_id: i64,
    ) -> CatalogResult<Option<statistics::RelationStatistics>> {
        match self
            .get_relation(relation::GetRelationInfo { relation_id })
            .await
        {
            Ok(_) => {
                statistics::get_relation_statistics(self.db, &self.snapshot, relation_id).await
            }
            Err(CatalogError::RelationNotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

impl<T: ConnectionTrait> Catalog<T> {
    pub async fn reader_at(&self, snapshot_id: SnapshotId) -> CatalogResult<CatalogReader<'_, T>> {
        let snapshot = snapshot::get_snapshot_info(&self.db, snapshot_id)
            .await?
            .ok_or(CatalogError::SnapshotNotFound { snapshot_id })?;
        Ok(CatalogReader {
            db: &self.db,
            snapshot,
            catalog_id: self.catalog_id,
        })
    }
    pub async fn reader(&self) -> CatalogResult<CatalogReader<'_, T>> {
        let snapshot = snapshot::get_current_snapshot_info(&self.db).await?.ok_or(
            CatalogError::SnapshotNotFound {
                snapshot_id: SnapshotId::INITIAL,
            },
        )?;
        Ok(CatalogReader {
            db: &self.db,
            snapshot,
            catalog_id: self.catalog_id,
        })
    }
}

impl Catalog<DatabaseConnection> {
    /// Registers an authoritative external catalog; does not connect to it.
    pub async fn register_catalog(
        &self,
        expected: SnapshotId,
        info: source::RegisterCatalogInfo,
    ) -> CatalogResult<i64> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::RegisterCatalog(info),
        )
        .await
        .map(|r| r.0)
    }
    pub async fn create_schema(
        &self,
        expected: SnapshotId,
        info: schema::CreateSchemaInfo,
    ) -> CatalogResult<i64> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::CreateSchema(info),
        )
        .await
        .map(|r| r.0)
    }
    pub async fn drop_schema(
        &self,
        expected: SnapshotId,
        info: schema::DropSchemaInfo,
    ) -> CatalogResult<()> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::DropSchema(info),
        )
        .await
        .map(|_| ())
    }
    pub async fn create_base_table(
        &self,
        expected: SnapshotId,
        info: relation::CreateBaseTableInfo,
    ) -> CatalogResult<i64> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::CreateRelation(info),
        )
        .await
        .map(|r| r.0)
    }
    pub async fn drop_relation(
        &self,
        expected: SnapshotId,
        info: relation::DropRelationInfo,
    ) -> CatalogResult<i64> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::DropRelation(info),
        )
        .await
        .map(|r| r.0)
    }
    pub async fn create_index(
        &self,
        expected: SnapshotId,
        info: index::CreateIndexInfo,
    ) -> CatalogResult<i64> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::CreateIndex(info),
        )
        .await
        .map(|r| r.0)
    }
    pub async fn drop_index(
        &self,
        expected: SnapshotId,
        info: index::DropIndexInfo,
    ) -> CatalogResult<()> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::DropIndex(info),
        )
        .await
        .map(|_| ())
    }
    pub async fn set_relation_statistics(
        &self,
        expected: SnapshotId,
        info: statistics::SetRelationStatisticsInfo,
    ) -> CatalogResult<SnapshotId> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::RefreshStatistics(info),
        )
        .await
        .map(|r| r.1)
    }
    /// Creates a caller-collected advanced object without changing basic statistics.
    pub async fn create_advanced_statistics(
        &self,
        expected: SnapshotId,
        info: advanced_statistics::CreateAdvancedStatisticsInfo,
    ) -> CatalogResult<i64> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::CreateAdvancedStatistics(info),
        )
        .await
        .map(|r| r.0)
    }
    /// Replaces contents, keeping the object's ID, owner, name, targets, and kind.
    /// Closed objects cannot be updated or resurrected.
    pub async fn update_advanced_statistics(
        &self,
        expected: SnapshotId,
        info: advanced_statistics::UpdateAdvancedStatisticsInfo,
    ) -> CatalogResult<SnapshotId> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::UpdateAdvancedStatistics(info),
        )
        .await
        .map(|r| r.1)
    }
    /// Retires one advanced object without changing basic statistics.
    pub async fn drop_advanced_statistics(
        &self,
        expected: SnapshotId,
        info: advanced_statistics::DropAdvancedStatisticsInfo,
    ) -> CatalogResult<SnapshotId> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::DropAdvancedStatistics(info),
        )
        .await
        .map(|r| r.1)
    }
    /// Atomically invalidates all requested relations in one nonstructural snapshot.
    /// Metadata-only retries must not replay the original source writes.
    pub async fn invalidate_statistics(
        &self,
        expected: SnapshotId,
        info: statistics::InvalidateStatisticsInfo,
    ) -> CatalogResult<SnapshotId> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::InvalidateStatisticsBatch(info),
        )
        .await
        .map(|r| r.1)
    }
    /// Closes the basic payload and all active advanced objects atomically.
    /// Historical reads survive; advanced recollection must allocate new object IDs.
    /// Does not mutate source data.
    pub async fn invalidate_relation_statistics(
        &self,
        expected: SnapshotId,
        relation_id: i64,
    ) -> CatalogResult<SnapshotId> {
        snapshot::commit(
            &self.db,
            self.catalog_id,
            expected,
            Mutation::InvalidateStatistics(relation_id),
        )
        .await
        .map(|r| r.1)
    }
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
