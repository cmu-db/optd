use std::future::Future;

use crate::{
    error::Error,
    ir::{
        expressions::{LogicalExpression, PhysicalExpression},
        goal::{Cost, Goal},
        group::GroupId,
        properties::LogicalProperties,
    },
};

pub type MemoizeResult<T> = Result<T, Error>;

#[trait_variant::make(Send)]
pub trait Memoize: Send + Sync + 'static {
    async fn get_logical_properties(&self, group_id: GroupId) -> MemoizeResult<LogicalProperties>;

    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Vec<LogicalExpression>>;

    async fn add_logical_expr<F, Fut>(
        &self,
        logical_expr: &LogicalExpression,
        derive_properties: F,
    ) -> MemoizeResult<GroupId>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = MemoizeResult<LogicalProperties>> + Send;

    async fn merge_groups(&self, from: GroupId, to: GroupId) -> MemoizeResult<()>;

    async fn get_winning_physical_expr(
        &self,
        goal: &Goal,
    ) -> MemoizeResult<(PhysicalExpression, Cost)>;

    async fn add_physical_expr(&self, physical_expr: &PhysicalExpression) -> MemoizeResult<Goal>;

    async fn merge_goals(&self, from: &Goal, to: &Goal) -> MemoizeResult<()>;
}
