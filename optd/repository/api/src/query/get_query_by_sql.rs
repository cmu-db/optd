use sea_orm::{ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter};

use crate::entity::{prelude::Query, query};

use super::QueryInfo;

/// Returns a query whose SQL exactly matches `sql`.
pub async fn get_query_by_sql<C>(db: &C, sql: &str) -> Result<Option<QueryInfo>, DbErr>
where
    C: ConnectionTrait,
{
    Query::find()
        .filter(query::Column::Sql.eq(sql))
        .one(db)
        .await
        .map(|query| query.map(QueryInfo::from))
}
