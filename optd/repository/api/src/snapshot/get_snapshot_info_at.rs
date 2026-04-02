use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
};

use crate::{entity::snapshot, snapshot::SnapshotInfo};

pub async fn get_snapshot_info_at<C>(
    db: &C,
    time: DateTime<Utc>,
) -> Result<Option<SnapshotInfo>, DbErr>
where
    C: ConnectionTrait,
{
    snapshot::Entity::find()
        .filter(snapshot::Column::SnapshotTime.lte(time))
        .select_only()
        .column(snapshot::Column::SnapshotId)
        .column(snapshot::Column::SchemaVersion)
        .column(snapshot::Column::NextCatalogId)
        .column(snapshot::Column::NextFileId)
        .order_by_desc(snapshot::Column::SnapshotTime)
        .order_by_desc(snapshot::Column::SnapshotId)
        .into_partial_model::<SnapshotInfo>()
        .one(db)
        .await
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use optd_repository_migration::{Migrator, MigratorTrait};
    use sea_orm::{
        ColumnTrait, ConnectionTrait, Database, DatabaseConnection, DbErr, EntityTrait,
        QueryFilter, sea_query::Expr,
    };

    use crate::{
        entity::{prelude::Snapshot, snapshot},
        snapshot::{
            SnapshotInfo, commit_snapshot, get_current_snapshot_info, get_snapshot_info_at,
        },
    };

    fn ts(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).expect("valid unix timestamp")
    }

    async fn setup_snapshots()
    -> Result<(DatabaseConnection, SnapshotInfo, SnapshotInfo, SnapshotInfo), DbErr> {
        let db = Database::connect("sqlite::memory:").await?;
        Migrator::up(&db, None).await?;

        let snapshot_1 = get_current_snapshot_info(&db)
            .await?
            .expect("migration should create an initial snapshot");

        let mut pending_snapshot_2 = snapshot_1.clone();
        pending_snapshot_2.schema_version = 10;
        pending_snapshot_2.next_catalog_id = 11;
        commit_snapshot(&db, pending_snapshot_2).await?;
        let snapshot_2 = get_current_snapshot_info(&db)
            .await?
            .expect("second snapshot should be visible");

        let mut pending_snapshot_3 = snapshot_2.clone();
        pending_snapshot_3.schema_version = 20;
        pending_snapshot_3.next_catalog_id = 21;
        commit_snapshot(&db, pending_snapshot_3).await?;
        let snapshot_3 = get_current_snapshot_info(&db)
            .await?
            .expect("third snapshot should be visible");

        Ok((db, snapshot_1, snapshot_2, snapshot_3))
    }

    async fn set_snapshot_time<C>(
        db: &C,
        snapshot_id: i64,
        time: DateTime<Utc>,
    ) -> Result<(), DbErr>
    where
        C: ConnectionTrait,
    {
        Snapshot::update_many()
            .col_expr(snapshot::Column::SnapshotTime, Expr::value(time))
            .filter(snapshot::Column::SnapshotId.eq(snapshot_id))
            .exec(db)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn get_snapshot_info_at_returns_latest_snapshot_at_or_before_time() -> Result<(), DbErr> {
        let (db, snapshot_1, snapshot_2, snapshot_3) = setup_snapshots().await?;

        set_snapshot_time(&db, snapshot_1.snapshot_id, ts(100)).await?;
        set_snapshot_time(&db, snapshot_2.snapshot_id, ts(200)).await?;
        set_snapshot_time(&db, snapshot_3.snapshot_id, ts(300)).await?;

        assert_eq!(get_snapshot_info_at(&db, ts(50)).await?, None);
        assert_eq!(
            get_snapshot_info_at(&db, ts(100)).await?,
            Some(snapshot_1.clone())
        );
        assert_eq!(
            get_snapshot_info_at(&db, ts(200)).await?,
            Some(snapshot_2.clone())
        );
        assert_eq!(
            get_snapshot_info_at(&db, ts(250)).await?,
            Some(snapshot_2.clone())
        );
        assert_eq!(
            get_snapshot_info_at(&db, ts(400)).await?,
            Some(snapshot_3.clone())
        );

        let tied_time = ts(500);
        set_snapshot_time(&db, snapshot_2.snapshot_id, tied_time).await?;
        set_snapshot_time(&db, snapshot_3.snapshot_id, tied_time).await?;

        assert_eq!(
            get_snapshot_info_at(&db, tied_time).await?,
            Some(snapshot_3)
        );

        Ok(())
    }
}
