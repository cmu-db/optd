use diesel::prelude::*;

use crate::storage::{models::rel_group::RelGroupId, schema};

#[derive(Debug, Queryable, Insertable, Selectable)]
#[diesel(table_name = schema::physical_filters)]
#[diesel(belongs_to(RelGroup, foreign_key = child))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalFilter {
    /// The child group id.
    pub child: RelGroupId,
    /// The filter predicate (e.g. <colA> > 3) (mocked).
    pub predicate: String,
}
