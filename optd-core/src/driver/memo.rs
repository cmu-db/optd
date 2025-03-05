use crate::{
    error::Error,
    ir::{
        expressions::{LogicalExpression, PhysicalExpression},
        goal::{Goal, OptimizationStatus},
        group::{Cost, ExplorationStatus, GroupId},
        properties::LogicalProperties,
    },
};

pub type MemoizeResult<T> = Result<T, Error>;

#[trait_variant::make(Send)]
pub trait Memoize: Send + Sync + 'static {
    async fn set_optimization_status(
        &self,
        goal: &Goal,
        status: OptimizationStatus,
    ) -> MemoizeResult<()>;

    async fn set_exploration_status(
        &self,
        group_id: GroupId,
        status: ExplorationStatus,
    ) -> MemoizeResult<()>;

    async fn get_exploration_status(&self, goal: GroupId) -> MemoizeResult<ExplorationStatus>;

    async fn get_optimization_status(&self, goal: &Goal) -> MemoizeResult<OptimizationStatus>;

    async fn get_logical_properties(&self, group_id: GroupId) -> MemoizeResult<LogicalProperties>;

    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Vec<LogicalExpression>>;

    async fn try_add_logical_expr(
        &self,
        logical_expr: &LogicalExpression,
    ) -> MemoizeResult<Option<GroupId>>;

    async fn create_group_with(
        &self,
        logical_expr: &LogicalExpression,
        properties: &LogicalProperties,
    ) -> MemoizeResult<GroupId>;

    async fn merge_groups(&self, from: GroupId, to: GroupId) -> MemoizeResult<()>;

    async fn get_winning_physical_expr(
        &self,
        goal: &Goal,
    ) -> MemoizeResult<(PhysicalExpression, Cost)>;

    async fn try_add_physical_expr(
        &self,
        physical_expr: &PhysicalExpression,
    ) -> MemoizeResult<Option<Goal>>;

    async fn create_goal_with(&self, physical_expr: &PhysicalExpression) -> MemoizeResult<Goal>;

    async fn merge_goals(&self, from: &Goal, to: &Goal) -> MemoizeResult<()>;
}
