use sea_orm::{ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, sea_query::Expr};

use crate::{
    entity::{column, prelude::*, relation},
    relation::DropRelationInfo,
};

pub async fn drop_relation<C>(
    info: DropRelationInfo,
    db: &C,
    end_snapshot: i64,
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    Relation::update_many()
        .col_expr(relation::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(relation::Column::EndSnapshot.is_null())
        .filter(relation::Column::RelationId.eq(info.relation_id))
        .exec(db)
        .await?;

    Column::update_many()
        .col_expr(column::Column::EndSnapshot, Expr::value(end_snapshot))
        .filter(column::Column::EndSnapshot.is_null())
        .filter(column::Column::RelationId.eq(info.relation_id))
        .exec(db)
        .await?;

    Ok(())
}
