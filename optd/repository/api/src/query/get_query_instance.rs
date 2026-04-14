use sea_orm::{ConnectionTrait, DbErr, EntityTrait};

use crate::entity::prelude::QueryInstance;

use super::QueryInstanceInfo;

/// Returns a query instance by id.
pub async fn get_query_instance<C>(db: &C, id: i64) -> Result<Option<QueryInstanceInfo>, DbErr>
where
    C: ConnectionTrait,
{
    QueryInstance::find_by_id(id)
        .one(db)
        .await
        .map(|query_instance| query_instance.map(QueryInstanceInfo::from))
}
