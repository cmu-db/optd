use sea_orm::{ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait};

use crate::entity::{
    prelude::{QueryInstance, QueryPlan as QueryPlanEntity},
    query_instance, query_plan,
};

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
        ..Default::default()
    })
    .exec(db)
    .await?;

    let query_instance_id = result.last_insert_id;
    if !info.query_plans.is_empty() {
        QueryPlanEntity::insert_many(info.query_plans.into_iter().map(|query_plan| {
            query_plan::ActiveModel {
                query_instance_id: Set(query_instance_id),
                plan: Set(query_plan.plan),
                description: Set(query_plan.description),
                ..Default::default()
            }
        }))
        .exec(db)
        .await?;
    }

    Ok(query_instance_id)
}
