use diesel::prelude::*;

use crate::storage::{models::rel_group::RelGroupId, schema::logical_filters};

#[derive(Debug, Queryable, Insertable)]
#[diesel(table_name = logical_filters)]
#[diesel(belongs_to(RelGroup, foreign_key = child))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalFilter {
    /// The child group id.
    pub child: RelGroupId,
    /// The filter predicate (e.g. <colA> > 3) (mocked).
    pub predicate: String,
}
