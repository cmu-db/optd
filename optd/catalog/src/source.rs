//! External source registrations. IDs are optd-local, not backend object IDs.
use crate::{CatalogError, CatalogResult, entity::source_catalog, snapshot::SnapshotInfo};
use sea_orm::{
    ActiveValue::{NotSet, Set},
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder,
};

/// Reserved namespace for databases upgraded from the original DataFusion catalog.
pub const LEGACY_CATALOG_ID: i64 = 0;

/// Connection information is referenced, never embedded as credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterCatalogInfo {
    pub catalog_name: String,
    pub catalog_kind: String,
    pub external_catalog_name: Option<String>,
    pub adapter_name: String,
    pub connection_profile_reference: String,
}

/// A registration visible at a pinned planning-metadata snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRegistration {
    pub catalog_id: i64,
    pub catalog_name: String,
    pub catalog_kind: String,
    pub external_catalog_name: Option<String>,
    pub adapter_name: String,
    pub connection_profile_reference: String,
}

pub(crate) async fn get_catalogs<C: ConnectionTrait>(
    db: &C,
    snapshot: &SnapshotInfo,
) -> CatalogResult<Vec<CatalogRegistration>> {
    let s = snapshot.snapshot_id.as_i64();
    Ok(source_catalog::Entity::find()
        .filter(source_catalog::Column::BeginSnapshot.lte(s))
        .filter(
            Condition::any()
                .add(source_catalog::Column::EndSnapshot.is_null())
                .add(source_catalog::Column::EndSnapshot.gt(s)),
        )
        .order_by_asc(source_catalog::Column::CatalogId)
        .all(db)
        .await?
        .into_iter()
        .map(|row| CatalogRegistration {
            catalog_id: row.catalog_id,
            catalog_name: row.catalog_name,
            catalog_kind: row.catalog_kind,
            external_catalog_name: row.external_catalog_name,
            adapter_name: row.adapter_name,
            connection_profile_reference: row.connection_profile_reference,
        })
        .collect())
}

pub(crate) async fn register<C: ConnectionTrait>(
    db: &C,
    snapshot: &SnapshotInfo,
    id: i64,
    begin: i64,
    info: RegisterCatalogInfo,
) -> CatalogResult<()> {
    if [
        &info.catalog_name,
        &info.catalog_kind,
        &info.adapter_name,
        &info.connection_profile_reference,
    ]
    .iter()
    .any(|v| v.trim().is_empty())
    {
        return Err(CatalogError::InvalidCatalogRegistration);
    }
    if get_catalogs(db, snapshot)
        .await?
        .iter()
        .any(|c| c.catalog_name == info.catalog_name)
    {
        return Err(CatalogError::CatalogAlreadyExists {
            catalog_name: info.catalog_name,
        });
    }
    source_catalog::Entity::insert(source_catalog::ActiveModel {
        row_id: NotSet,
        catalog_id: Set(id),
        begin_snapshot: Set(begin),
        end_snapshot: Set(None),
        catalog_name: Set(info.catalog_name),
        catalog_kind: Set(info.catalog_kind),
        external_catalog_name: Set(info.external_catalog_name),
        adapter_name: Set(info.adapter_name),
        connection_profile_reference: Set(info.connection_profile_reference),
    })
    .exec(db)
    .await?;
    Ok(())
}
