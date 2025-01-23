use diesel::prelude::*;

use crate::storage::{
    models::{common::JoinType, physical_expr::PhysicalExprId, rel_group::RelGroupId},
    schema,
};

#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = schema::physical_nljoins)]
#[diesel(primary_key(physical_expr_id))]
#[diesel(belongs_to(PhysicalExpr))]
#[diesel(belongs_to(RelGroup, foreign_key = left))]
#[diesel(belongs_to(RelGroup, foreign_key = right))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalNLJoin {
    pub physical_expr_id: PhysicalExprId,
    /// The type of join to perform.
    pub join_type: JoinType,
    /// The group on the left side of the join.
    pub left: RelGroupId,
    /// The group on the right side of the join.
    pub right: RelGroupId,
    /// The join condition (mocked).
    pub join_cond: String,
}
