use std::collections::HashMap;

use crate::{
    cir::{
        expressions::{LogicalExpression, OptimizedExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        properties::{LogicalProperties, PhysicalProperties},
        rules::{ImplementationRule, TransformationRule},
    },
    error::Error,
};

/// Type alias for results returned by Memoize trait methods
pub type MemoizeResult<T> = Result<T, Error>;

/// Status of a rule application or costing operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// There exist ongoing events that may affect the operation
    Dirty,

    /// Operation is up to date with no pending events that could affect it
    Clean,
}

/// Results of merge operations for different entity types
pub(crate) enum MergeResult {
    /// Result of merging two groups
    GroupMerge {
        /// Original group ID that was merged
        prev_group_id: GroupId,

        /// Representative group ID after merging
        representative_group_id: GroupId,

        /// New logical expressions added to the group
        new_expressions: Vec<LogicalExpression>,
    },

    /// Result of merging two goals
    GoalMerge {
        /// Original goal that was merged
        prev_goal: Goal,

        /// Representative goal after merging
        representative_goal: Goal,

        /// New (potential) optimized expression added to the goal
        new_expression: Option<OptimizedExpression>,

        /// New equivalent goals created during the merge process
        new_equivalent_goals: HashMap<GroupId, Vec<PhysicalProperties>>,
    },
}

/// Core interface for memo-based query optimization
///
/// This trait defines the operations needed to store, retrieve, and manipulate
/// the memo data structure that powers the dynamic programming approach to
/// query optimization. It tracks logical and physical expressions, properties,
/// and optimization status.
#[trait_variant::make(Send)]
pub trait Memoize: Send + Sync + 'static {
    //
    // Logical expression and group operations
    //

    /// Retrieves logical properties for a group
    ///
    /// Returns the properties associated with the group or an error if not found.
    async fn get_logical_properties(&self, group_id: GroupId) -> MemoizeResult<LogicalProperties>;

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
        &mut self,
        logical_expr: &LogicalExpression,
        props: &LogicalProperties,
    ) -> MemoizeResult<GroupId>;

    /// Merges groups 1 and 2, creating a new group containing all expressions
    ///
    /// May trigger cascading merges of parent groups & goals.
    /// Returns a vector of merge results for all affected entities.
    async fn merge_groups(
        &mut self,
        group_1: GroupId,
        group_2: GroupId,
    ) -> MemoizeResult<Vec<MergeResult>>;

    //
    // Physical expression and goal operations
    //

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
    async fn get_all_physical_exprs(&self, goal: &Goal) -> MemoizeResult<Vec<PhysicalExpression>>;

    /// Gets all goals in the same equivalence class
    ///
    /// Returns all goals that are equivalent to the given goal, grouped by their
    /// group ID with associated physical properties. This allows for exploring
    /// all equivalent implementations across multiple groups.
    async fn get_equivalent_goals(
        &self,
        goal: &Goal,
    ) -> MemoizeResult<HashMap<GroupId, Vec<PhysicalProperties>>>;

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
    async fn create_goal(&mut self, physical_expr: &PhysicalExpression) -> MemoizeResult<Goal>;

    /// Merges goals 1 and 2, creating a new goal containing all expressions
    ///
    /// May trigger cascading merges of parent entities.
    /// Returns a vector of merge results for all affected entities.
    async fn merge_goals(
        &mut self,
        goal_1: &Goal,
        goal_2: &Goal,
    ) -> MemoizeResult<Vec<MergeResult>>;

    /// Adds an optimized physical expression to a goal
    ///
    /// Returns whether the optimized expression is now the best expression for the goal,
    /// allowing callers to determine if this expression should be propagated to subscribers.
    async fn add_optimized_physical_expr(
        &mut self,
        goal: &Goal,
        optimized_expr: &OptimizedExpression,
    ) -> MemoizeResult<bool>;

    //
    // Rule and costing status operations
    //

    /// Checks the status of applying a transformation rule on a logical expression
    ///
    /// Returns `Status::Dirty` if there are ongoing events that may affect the transformation
    /// or `Status::Clean` if the transformation does not need to be re-evaluated.
    /// Returns None if the transformation rule has not been applied yet on the logical expression.
    async fn get_transformation_status(
        &self,
        logical_expr: &LogicalExpression,
        rule: &TransformationRule,
    ) -> MemoizeResult<Option<Status>>;

    /// Sets the status of applying a transformation rule on a logical expression
    async fn set_transformation_status(
        &mut self,
        logical_expr: &LogicalExpression,
        rule: &TransformationRule,
        status: Status,
    ) -> MemoizeResult<()>;

    /// Checks the status of applying an implementation rule on a logical expression
    ///
    /// Returns `Status::Dirty` if there are ongoing events that may affect the transformation
    /// or `Status::Clean` if the implementation does not need to be re-evaluated.
    /// Returns None if the implementation rule has not been applied yet on the logical expression.
    async fn get_implementation_status(
        &self,
        logical_expr: &LogicalExpression,
        rule: &ImplementationRule,
    ) -> MemoizeResult<Option<Status>>;

    /// Sets the status of applying an implementation rule on a logical expression
    async fn set_implementation_status(
        &mut self,
        logical_expr: &LogicalExpression,
        rule: &ImplementationRule,
        status: Status,
    ) -> MemoizeResult<()>;

    /// Checks the status of costing a physical expression
    ///
    /// Returns `Status::Dirty` if there are ongoing events that may affect the costing
    /// or `Status::Clean` if the costing does not need to be re-evaluated.
    /// Returns None if the physical expression has not been costed.
    async fn get_costing_status(
        &self,
        physical_expr: &PhysicalExpression,
    ) -> MemoizeResult<Option<Status>>;

    /// Sets the status of costing a physical expression
    async fn set_costing_status(
        &mut self,
        physical_expr: &PhysicalExpression,
        status: Status,
    ) -> MemoizeResult<()>;
}
