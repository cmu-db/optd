use diesel::prelude::*;

use crate::storage::{
    models::{
        common::JoinType,
        logical_expr::{LogicalExprId, LogicalExprStorage, LogicalOp},
        rel_group::RelGroupId,
    },
    schema::logical_joins,
    StorageManager,
};

#[derive(Debug, Queryable, Insertable, Selectable)]
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

impl LogicalOp for LogicalJoin {
    const NAME: &'static str = "LogicalJoin";

    fn get(id: LogicalExprId, storage: &mut StorageManager) -> Self {
        use crate::storage::schema::logical_joins::dsl::*;
        logical_joins
            .filter(logical_expr_id.eq(id))
            .select(LogicalJoin::as_select())
            .first::<LogicalJoin>(&mut storage.conn)
            .expect("failed to get logical join")
    }
}

impl LogicalExprStorage for LogicalJoin {
    fn id(&self, storage: &mut StorageManager) -> Option<LogicalExprId> {
        use self::logical_joins::dsl::*;
        logical_joins
            .filter(join_type.eq(&self.join_type))
            .filter(left.eq(self.left))
            .filter(right.eq(self.right))
            .filter(join_cond.eq(&self.join_cond))
            .select(logical_expr_id)
            .first::<LogicalExprId>(&mut storage.conn)
            .ok()
    }

    fn insert_op(&self, id: LogicalExprId, storage: &mut StorageManager) {
        use crate::storage::schema::logical_joins::dsl::*;
        diesel::insert_into(logical_joins)
            .values((logical_expr_id.eq(id), self))
            .execute(&mut storage.conn)
            .expect("failed to insert new logical join");
    }

    fn name(&self) -> &'static str {
        <Self as LogicalOp>::NAME
    }
}
