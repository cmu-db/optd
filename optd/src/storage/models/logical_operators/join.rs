use diesel::prelude::*;

use crate::storage::{
    models::{common::JoinType, logical_expr::LogicalExprId, rel_group::RelGroupId},
    schema,
};

#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = schema::logical_joins)]
#[diesel(primary_key(logical_expr_id))]
#[diesel(belongs_to(LogicalExpr))]
#[diesel(belongs_to(RelGroup, foreign_key = left))]
#[diesel(belongs_to(RelGroup, foreign_key = right))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalJoin {
    pub logical_expr_id: LogicalExprId,
    /// The type of join to perform.
    pub join_type: JoinType,
    /// The group on the left side of the join.
    pub left: RelGroupId,
    /// The group on the right side of the join.
    pub right: RelGroupId,
}
