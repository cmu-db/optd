use crate::{
    cir::{
        expressions::{LogicalExpression, OptimizedExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        properties::LogicalProperties,
    },
    error::Error,
};

pub type MemoizeResult<T> = Result<T, Error>;

#[trait_variant::make(Send)]
pub trait Memoize: Send + Sync + 'static {
    async fn get_logical_properties(&self, group_id: GroupId) -> MemoizeResult<LogicalProperties>;

    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Vec<LogicalExpression>>;

    async fn find_logical_expr(
        &self,
        logical_expr: &LogicalExpression,
    ) -> MemoizeResult<Option<GroupId>>;

    async fn create_group_with(
        &self,
        logical_expr: &LogicalExpression,
        props: &LogicalProperties,
    ) -> MemoizeResult<GroupId>;

    async fn merge_groups(&self, from: GroupId, to: GroupId) -> MemoizeResult<()>;

    async fn get_best_optimized_physical_expr(
        &self,
        goal: &Goal,
    ) -> MemoizeResult<Option<OptimizedExpression>>;

    async fn add_physical_expr(&self, physical_expr: &PhysicalExpression) -> MemoizeResult<Goal>;

    async fn merge_goals(&self, from: &Goal, to: &Goal) -> MemoizeResult<()>;
}
