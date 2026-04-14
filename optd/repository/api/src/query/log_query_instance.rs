use sea_orm::{ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait};

use crate::entity::{prelude::QueryInstance, query_instance};

use super::{LogQueryInstanceInfo, get_or_create_query_id};

/// Logs a query instance for `sql` at `snapshot_id`.
pub async fn log_query_instance<C>(db: &C, info: LogQueryInstanceInfo) -> Result<i64, DbErr>
where
    C: ConnectionTrait,
{
    let query_id = get_or_create_query_id(db, info.sql).await?;
    let result = QueryInstance::insert(query_instance::ActiveModel {
        query_id: Set(query_id),
        snapshot_id: Set(info.snapshot_id),
        initial_plan: Set(info.initial_plan),
        final_plan: Set(info.final_plan),
        ..Default::default()
    })
    .exec(db)
    .await?;

    Ok(result.last_insert_id)
}
