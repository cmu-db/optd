use sea_orm::{ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder};

use crate::entity::{prelude::QueryPlan, query_plan};

use super::QueryPlanInfo;

/// Returns query plans for a query instance.
pub async fn get_query_plans<C>(db: &C, query_instance_id: i64) -> Result<Vec<QueryPlanInfo>, DbErr>
where
    C: ConnectionTrait,
{
    QueryPlan::find()
        .filter(query_plan::Column::QueryInstanceId.eq(query_instance_id))
        .order_by_asc(query_plan::Column::Id)
        .all(db)
        .await
        .map(|query_plans| query_plans.into_iter().map(QueryPlanInfo::from).collect())
}
