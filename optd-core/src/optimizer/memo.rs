use crate::{
    cir::{
        expressions::{LogicalExpression, OptimizedExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        properties::LogicalProperties,
    },
    error::Error,
};

pub(crate) type MemoizeResult<T> = Result<T, Error>;

/// Results of merge operations for different entity types
pub(crate) enum MergeResult {
    /// Result of merging two groups
    GroupMerge {
        /// Original group ID that was merged
        prev_group_id: GroupId,
        /// New group ID after merging
        new_group_id: GroupId,
        /// New logical expressions added to the group
        expressions: Vec<LogicalExpression>,
    },

    /// Result of merging two goals
    GoalMerge {
        /// Original goal that was merged
        prev_goal: Goal,
        /// New goal after merging
        new_goal: Goal,
        /// New (potential) optimized expression added to the goal
        expression: Option<OptimizedExpression>,
    },
}

#[trait_variant::make(Send)]
pub(crate) trait Memoize: Send + Sync + 'static {
    /// Retrieves logical properties for a group
    async fn get_logical_properties(&self, group_id: GroupId) -> MemoizeResult<LogicalProperties>;

    /// Gets all logical expressions in a group
    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Vec<LogicalExpression>>;

    /// Finds group containing a logical expression, if it exists
    async fn find_logical_expr(
        &self,
        logical_expr: &LogicalExpression,
    ) -> MemoizeResult<Option<GroupId>>;

    /// Creates a new group with a logical expression and properties
    async fn create_group_with(
        &self,
        logical_expr: &LogicalExpression,
        props: &LogicalProperties,
    ) -> MemoizeResult<GroupId>;

    /// Merges groups g1 and g2, creating a new group containing all expressions
    ///
    /// May trigger cascading merges of parent groups & goals.
    /// Returns a vector of merge results for all affected entities.
    async fn merge_groups(&self, g1: GroupId, g2: GroupId) -> MemoizeResult<Vec<MergeResult>>;

    /// Gets the best optimized physical expression for a goal
    async fn get_best_optimized_physical_expr(
        &self,
        goal: &Goal,
    ) -> MemoizeResult<Option<OptimizedExpression>>;

    async fn add_physical_expr(&self, physical_expr: &PhysicalExpression) -> MemoizeResult<Goal>;

    async fn merge_goals(&self, from: &Goal, to: &Goal) -> MemoizeResult<Vec<MergeResult>>;
}
