use sea_orm::{ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder};

use crate::entity::{
    prelude::{QueryInstance, QueryPlan as QueryPlanEntity},
    query_instance, query_plan,
};

use super::{QueryInstanceInfo, QueryInstanceSelector, QueryPlanInfo, get_query_by_sql};

/// Returns query instances matching `selector`.
pub async fn get_query_instances<C>(
    db: &C,
    selector: QueryInstanceSelector,
) -> Result<Vec<QueryInstanceInfo>, DbErr>
where
    C: ConnectionTrait,
{
    let query_id = match selector {
        QueryInstanceSelector::QueryId(query_id) => query_id,
        QueryInstanceSelector::Sql(sql) => {
            let Some(query) = get_query_by_sql(db, &sql).await? else {
                return Ok(Vec::new());
            };
            query.query_id
        }
    };

    QueryInstance::find()
        .filter(query_instance::Column::QueryId.eq(query_id))
        .order_by_asc(query_instance::Column::Id)
        .find_with_related(QueryPlanEntity)
        .order_by_asc(query_plan::Column::Id)
        .all(db)
        .await
        .map(|query_instances| {
            query_instances
                .into_iter()
                .map(|(query_instance, query_plans)| QueryInstanceInfo {
                    id: query_instance.id,
                    query_id: query_instance.query_id,
                    snapshot_id: query_instance.snapshot_id,
                    query_time: query_instance.query_time,
                    query_plans: query_plans.into_iter().map(QueryPlanInfo::from).collect(),
                })
                .collect()
        })
}
