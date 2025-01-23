use diesel::prelude::*;

use crate::storage::{
    models::{logical_expr::LogicalExprId, rel_group::RelGroupId},
    schema,
};

#[derive(Debug, Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = schema::logical_filters)]
#[diesel(primary_key(logical_expr_id))]
#[diesel(belongs_to(LogicalExpr))]
#[diesel(belongs_to(RelGroup, foreign_key = child))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalFilter {
    pub logical_expr_id: LogicalExprId,
    /// The child group id.
    pub child: RelGroupId,
    /// The filter predicate (e.g. <colA> > 3) (mocked).
    pub predicate: String,
}
