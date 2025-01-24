use diesel::prelude::*;

use crate::storage::{
    models::{common::JoinType, rel_group::RelGroupId},
    schema::logical_joins,
};

#[derive(Debug, Queryable, Insertable)]
#[diesel(table_name = logical_joins)]
#[diesel(belongs_to(RelGroup, foreign_key = left))]
#[diesel(belongs_to(RelGroup, foreign_key = right))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalJoin {
    /// The type of join to perform.
    pub join_type: JoinType,
    /// The group on the left side of the join.
    pub left: RelGroupId,
    /// The group on the right side of the join.
    pub right: RelGroupId,
    /// The join condition (mocked).
    pub join_cond: String,
}
