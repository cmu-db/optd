use diesel::prelude::*;

use crate::storage::{
    models::{
        logical_expr::{LogicalExprId, LogicalExprStorage, LogicalOp},
        rel_group::RelGroupId,
    },
    schema::logical_filters,
    StorageManager,
};

#[derive(Debug, Queryable, Insertable, Selectable)]
#[diesel(table_name = logical_filters)]
#[diesel(belongs_to(RelGroup, foreign_key = child))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalFilter {
    /// The child group id.
    pub child: RelGroupId,
    /// The filter predicate (e.g. <colA> > 3) (mocked).
    pub predicate: String,
}

impl LogicalOp for LogicalFilter {
    const NAME: &'static str = "LogicalFilter";

    fn get(id: LogicalExprId, storage: &mut StorageManager) -> Self {
        use crate::storage::schema::logical_filters::dsl::*;
        logical_filters
            .filter(logical_expr_id.eq(id))
            .select(LogicalFilter::as_select())
            .first::<LogicalFilter>(&mut storage.conn)
            .expect("failed to get logical filter")
    }
}

impl LogicalExprStorage for LogicalFilter {
    fn id(&self, storage: &mut StorageManager) -> Option<LogicalExprId> {
        use self::logical_filters::dsl::*;
        logical_filters
            .filter(child.eq(self.child))
            .filter(predicate.eq(&self.predicate))
            .select(logical_expr_id)
            .first::<LogicalExprId>(&mut storage.conn)
            .ok()
    }

    fn insert_op(&self, id: LogicalExprId, storage: &mut StorageManager) {
        use crate::storage::schema::logical_filters::dsl::*;
        diesel::insert_into(logical_filters)
            .values((logical_expr_id.eq(id), self))
            .execute(&mut storage.conn)
            .expect("failed to insert new logical filter");
    }

    fn name(&self) -> &'static str {
        <Self as LogicalOp>::NAME
    }
}
