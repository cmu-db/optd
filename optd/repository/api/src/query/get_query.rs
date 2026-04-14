use sea_orm::{ConnectionTrait, DbErr, EntityTrait};

use crate::entity::prelude::Query;

use super::QueryInfo;

/// Returns a query by id.
pub async fn get_query<C>(db: &C, query_id: i64) -> Result<Option<QueryInfo>, DbErr>
where
    C: ConnectionTrait,
{
    Query::find_by_id(query_id)
        .one(db)
        .await
        .map(|query| query.map(QueryInfo::from))
}
