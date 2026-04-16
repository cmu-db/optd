use sea_orm::{ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QueryOrder};

use crate::entity::{prelude::QueryInstance, query_instance};

use super::{
    QueryInstanceInfo, QueryInstanceSelector, get_query_by_sql, get_query_plans,
    query_instance_info_from_parts,
};

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

    let query_instances = QueryInstance::find()
        .filter(query_instance::Column::QueryId.eq(query_id))
        .order_by_asc(query_instance::Column::Id)
        .all(db)
        .await?;

    let mut infos = Vec::with_capacity(query_instances.len());
    for query_instance in query_instances {
        let query_plans = get_query_plans(db, query_instance.id).await?;
        infos.push(query_instance_info_from_parts(query_instance, query_plans));
    }

    Ok(infos)
}
