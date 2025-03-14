use crate::{
    cir::{
        Goal, GroupId, LogicalExpression, LogicalProperties, OptimizedExpression,
        PhysicalExpression,
    },
    error::Error,
};

pub(crate) type MemoizeResult<T> = Result<T, Error>;

/// Results of merge operations for different entity types
#[allow(dead_code)]
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
pub trait Memoize: Send + Sync + 'static {
    /// Retrieves logical properties for a group
    ///
    /// Returns the properties associated with the group or an error if not found.
    async fn get_logical_properties(&self, group_id: &GroupId) -> MemoizeResult<LogicalProperties>;

    /// Gets all logical expressions in a group
    ///
    /// Returns a vector of all logical expressions in the specified group.
    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Vec<LogicalExpression>>;

    /// Finds group containing a logical expression, if it exists
    ///
    /// Returns the group ID if the expression exists, None otherwise.
    async fn find_logical_expr(
        &self,
        logical_expr: &LogicalExpression,
    ) -> MemoizeResult<Option<GroupId>>;

    /// Creates a new group with a logical expression and properties
    ///
    /// Returns the ID of the newly created group.
    async fn create_group(
        &self,
        logical_expr: &LogicalExpression,
        props: &LogicalProperties,
    ) -> MemoizeResult<GroupId>;

    /// Merges groups 1 and 2, creating a new group containing all expressions
    ///
    /// May trigger cascading merges of parent groups & goals.
    /// Returns a vector of merge results for all affected entities.
    async fn merge_groups(
        &self,
        group_1: &GroupId,
        group_2: &GroupId,
    ) -> MemoizeResult<Vec<MergeResult>>;

    /// Gets the best optimized physical expression for a goal
    ///
    /// Retrieves the lowest-cost physical implementation found so far for the goal.
    /// Returns None if no optimized expression exists for the goal.
    async fn get_best_optimized_physical_expr(
        &self,
        goal: &Goal,
    ) -> MemoizeResult<Option<OptimizedExpression>>;

    /// Gets all physical expressions in a goal
    ///
    /// Returns a vector of physical expressions in the specified goal.
    /// If the goal doesn't exist, returns an empty vector.
    async fn get_all_physical_exprs(&self, goal: &Goal) -> MemoizeResult<Vec<PhysicalExpression>>;

    /// Searches for a physical expression in the memo
    ///
    /// Returns the associated Goal if the physical expression exists.
    /// Returns None if the expression isn't found in any goal.
    async fn find_physical_expr(
        &self,
        physical_expr: &PhysicalExpression,
    ) -> MemoizeResult<Option<Goal>>;

    /// Creates a new goal associated with the provided physical expression
    ///
    /// Allocates a new unique group ID for the goal and initializes it with empty properties.
    /// This creates an isolated goal that may later participate in the memo structure
    /// through merges with equivalent goals during optimization.
    async fn create_goal(&self, physical_expr: &PhysicalExpression) -> MemoizeResult<Goal>;

    /// Merges goals 1 and 2, creating a new goal containing all expressions
    ///
    /// May trigger cascading merges of parent entities.
    /// Returns a vector of merge results for all affected entities.
    async fn merge_goals(&self, goal_1: &Goal, goal_2: &Goal) -> MemoizeResult<Vec<MergeResult>>;

    /// Adds an optimized physical expression to a goal
    ///
    /// Returns whether the optimized expression is now the best expression for the goal,
    /// allowing callers to determine if this expression should be propagated to subscribers.
    async fn add_optimized_physical_expr(
        &self,
        goal: &Goal,
        optimized_expr: &OptimizedExpression,
    ) -> MemoizeResult<bool>;
}
