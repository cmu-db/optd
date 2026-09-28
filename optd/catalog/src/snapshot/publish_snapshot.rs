use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, sea_query::Expr};

use crate::{
    CatalogError, CatalogResult,
    entity::{branch, prelude::Branch},
    snapshot::{SnapshotId, SnapshotInfo, get_current_snapshot_info},
};

pub(crate) async fn get_snapshot_for_write<C>(
    db: &C,
    branch_name: &str,
    expected_snapshot_id: SnapshotId,
) -> CatalogResult<SnapshotInfo>
where
    C: ConnectionTrait,
{
    let current = get_current_snapshot_info(db, branch_name)
        .await?
        .ok_or_else(|| CatalogError::BranchNotFound {
            branch_name: branch_name.to_owned(),
        })?;

    if current.snapshot_id != expected_snapshot_id {
        return Err(stale_snapshot_error(
            branch_name,
            expected_snapshot_id,
            current.snapshot_id,
        ));
    }

    Ok(current)
}

pub(crate) async fn publish_snapshot<C>(
    db: &C,
    branch_name: &str,
    expected_snapshot_id: SnapshotId,
    new_snapshot_id: SnapshotId,
) -> CatalogResult<()>
where
    C: ConnectionTrait,
{
    let result = Branch::update_many()
        .col_expr(
            branch::Column::CurrentSnapshotId,
            Expr::value(new_snapshot_id.as_i64()),
        )
        .filter(branch::Column::BranchName.eq(branch_name))
        .filter(branch::Column::CurrentSnapshotId.eq(expected_snapshot_id.as_i64()))
        .exec(db)
        .await?;

    if result.rows_affected != 1 {
        let actual_snapshot_id = get_current_snapshot_info(db, branch_name)
            .await?
            .map(|snapshot| snapshot.snapshot_id)
            .ok_or_else(|| CatalogError::BranchNotFound {
                branch_name: branch_name.to_owned(),
            })?;
        return Err(stale_snapshot_error(
            branch_name,
            expected_snapshot_id,
            actual_snapshot_id,
        ));
    }

    Ok(())
}

fn stale_snapshot_error(
    branch_name: &str,
    expected: SnapshotId,
    actual: SnapshotId,
) -> CatalogError {
    CatalogError::StaleSnapshot {
        branch_name: branch_name.to_owned(),
        expected,
        actual,
    }
}
