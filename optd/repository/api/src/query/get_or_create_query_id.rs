use sea_orm::{ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait};

use crate::entity::{prelude::Query, query};

use super::get_query_by_sql;

/// Returns the query id for an exact SQL match, inserting a new query if needed.
pub async fn get_or_create_query_id<C>(db: &C, sql: String) -> Result<i64, DbErr>
where
    C: ConnectionTrait,
{
    if let Some(query) = get_query_by_sql(db, &sql).await? {
        return Ok(query.query_id);
    }

    let result = Query::insert(query::ActiveModel {
        sql: Set(sql.clone()),
        ..Default::default()
    })
    .exec(db)
    .await;

    match result {
        Ok(result) => Ok(result.last_insert_id),
        Err(err) => {
            if let Some(query) = get_query_by_sql(db, &sql).await? {
                Ok(query.query_id)
            } else {
                Err(err)
            }
        }
    }
}
