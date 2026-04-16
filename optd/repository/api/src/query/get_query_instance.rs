use sea_orm::{ConnectionTrait, DbErr, EntityTrait};

use crate::entity::prelude::QueryInstance;

use super::{QueryInstanceInfo, get_query_plans, query_instance_info_from_parts};

/// Returns a query instance by id.
pub async fn get_query_instance<C>(db: &C, id: i64) -> Result<Option<QueryInstanceInfo>, DbErr>
where
    C: ConnectionTrait,
{
    let Some(query_instance) = QueryInstance::find_by_id(id).one(db).await? else {
        return Ok(None);
    };
    let query_plans = get_query_plans(db, id).await?;

    Ok(Some(query_instance_info_from_parts(
        query_instance,
        query_plans,
    )))
}
