use diesel::prelude::*;

use crate::storage::{
    models::{physical_expr::PhysicalExprId, rel_group::RelGroupId},
    schema,
};

#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = schema::physical_filters)]
#[diesel(primary_key(physical_expr_id))]
#[diesel(belongs_to(LogicalExpr))]
#[diesel(belongs_to(RelGroup, foreign_key = child))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalFilter {
    pub physical_expr_id: PhysicalExprId,
    /// The child group id.
    pub child: RelGroupId,
    /// The filter predicate (e.g. <colA> > 3) (mocked).
    pub predicate: String,
}
