use sea_orm::{ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder};

use crate::entity::{prelude::QueryInstance, query_instance};

use super::{QueryInstanceInfo, QueryInstanceSelector, get_query_by_sql};

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
        .all(db)
        .await
        .map(|query_instances| {
            query_instances
                .into_iter()
                .map(QueryInstanceInfo::from)
                .collect()
        })
}
