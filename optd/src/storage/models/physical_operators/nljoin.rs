use diesel::prelude::*;

use crate::storage::{
    models::{common::JoinType, rel_group::RelGroupId},
    schema::physical_nljoins,
};

/// A physical nested loop join.
#[derive(Debug, Queryable, Insertable)]
#[diesel(table_name = physical_nljoins)]
#[diesel(belongs_to(RelGroup, foreign_key = left))]
#[diesel(belongs_to(RelGroup, foreign_key = right))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalNLJoin {
    /// The type of join to perform.
    pub join_type: JoinType,
    /// The group on the left side of the join.
    pub left: RelGroupId,
    /// The group on the right side of the join.
    pub right: RelGroupId,
    /// The join condition (mocked).
    pub join_cond: String,
}
