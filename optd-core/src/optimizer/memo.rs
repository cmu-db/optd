use crate::{
    cir::{
        expressions::{
            LogicalExpression, LogicalExpressionId, PhysicalExpression, PhysicalExpressionId,
        },
        goal::{Cost, Goal, GoalId},
        group::GroupId,
        properties::LogicalProperties,
        rules::{ImplementationRule, TransformationRule},
    },
    error::Error,
};

/// Type alias for results returned by Memoize trait methods
pub(crate) type MemoizeResult<T> = Result<T, Error>;

/// Status of a rule application or costing operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// There exist ongoing jobs that may generate more expressions or costs from this expression.
    Dirty,

    /// Expression is fully explored or costed with no pending jobs that could add anything new.
    Clean,
}

/// Results of merge operations for different entity types.
pub enum MergeResult {
    /// Result of merging two groups.
    GroupMerge {
        /// Groups that were merged along with their expressions.
        /// All expressions listed in `merged_exprs` must be contained within these groups.
        merged_groups: Vec<(GroupId, Vec<LogicalExpressionId>)>,

        /// ID of the new representative group.
        new_repr_group: GroupId,

        /// Expressions that were merged as a result of the group merge.
        /// These logical expressions are guaranteed to be part of the groups
        /// listed in `merged_groups`.
        merged_exprs: Vec<(Vec<LogicalExpressionId>, LogicalExpressionId)>,
    },

    /// Result of merging two goals.
    GoalMerge {
        /// Goals that were merged along with their expressions.
        /// All expressions listed in `merged_exprs` must be contained within these goals.
        merged_goals: Vec<(GoalId, Vec<PhysicalExpressionId>)>,

        /// ID of the new representative goal.
        new_repr_goal: GoalId,

        /// Expressions that were merged as a result of the goal merge.
        /// These physical expressions are guaranteed to be part of the goals
        /// listed in `merged_goals`.
        merged_exprs: Vec<(Vec<PhysicalExpressionId>, PhysicalExpressionId)>,
    },
}

/// Core interface for memo-based query optimization.
///
/// This trait defines the operations needed to store, retrieve, and manipulate
/// the memo data structure that powers the dynamic programming approach to
/// query optimization. The memo stores logical and physical expressions by their IDs,
/// manages expression properties, and tracks optimization status.
#[trait_variant::make(Send)]
pub trait Memoize: Send + Sync + 'static {
    //
    // Logical expression and group operations.
    //

    /// Retrieves logical properties for a group ID.
    ///
    /// # Parameters
    /// * `group_id` - ID of the group to retrieve properties for.
    ///
    /// # Returns
    /// The properties associated with the group or an error if not found.
    async fn get_logical_properties(&self, group_id: GroupId) -> MemoizeResult<LogicalProperties>;

    /// Gets all logical expression IDs in a group.
    ///
    /// # Parameters
    /// * `group_id` - ID of the group to retrieve expressions from.
    ///
    /// # Returns
    /// A vector of logical expression IDs in the specified group.
    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Vec<LogicalExpressionId>>;

    /// Finds group containing a logical expression ID, if it exists.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to find.
    ///
    /// # Returns
    /// The group ID if the expression exists, None otherwise.
    async fn find_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<Option<GroupId>>;

    /// Creates a new group with a logical expression ID and properties.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to add to the group.
    /// * `props` - Logical properties for the group.
    ///
    /// # Returns
    /// The ID of the newly created group.
    async fn create_group(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        props: &LogicalProperties,
    ) -> MemoizeResult<GroupId>;

    /// Merges groups 1 and 2, unifying them under a common representative.
    ///
    /// May trigger cascading merges of parent groups & goals.
    ///
    /// # Parameters
    /// * `group_1` - ID of the first group to merge.
    /// * `group_2` - ID of the second group to merge.
    ///
    /// # Returns
    /// A vector of merge results for all affected entities.
    async fn merge_groups(
        &mut self,
        group_1: GroupId,
        group_2: GroupId,
    ) -> MemoizeResult<Vec<MergeResult>>;

    //
    // Physical expression and goal operations.
    //

    /// Gets the best optimized physical expression ID for a goal ID.
    ///
    /// # Parameters
    /// * `goal_id` - ID of the goal to retrieve the best expression for.
    ///
    /// # Returns
    /// The ID of the lowest-cost physical implementation found so far for the goal,
    /// along with its cost. Returns None if no optimized expression exists.
    async fn get_best_optimized_physical_expr(
        &self,
        goal_id: GoalId,
    ) -> MemoizeResult<Option<(PhysicalExpressionId, Cost)>>;

    /// Gets all physical expression IDs in a goal.
    ///
    /// # Parameters
    /// * `goal_id` - ID of the goal to retrieve expressions from.
    ///
    /// # Returns
    /// A vector of physical expression IDs in the specified goal.
    async fn get_all_physical_exprs(
        &self,
        goal_id: GoalId,
    ) -> MemoizeResult<Vec<PhysicalExpressionId>>;

    /// Gets all goal IDs in the same equivalence class.
    ///
    /// # Parameters
    /// * `goal_id` - ID of the goal to find equivalents for.
    ///
    /// # Returns
    /// A vector of goal IDs that are equivalent to the given goal.
    async fn get_equivalent_goals(&self, goal_id: GoalId) -> MemoizeResult<Vec<GoalId>>;

    /// Searches for a physical expression ID in the memo.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to find.
    ///
    /// # Returns
    /// The associated Goal ID if the physical expression exists,
    /// None if the expression isn't found in any goal.
    async fn find_physical_expr(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<Option<GoalId>>;

    /// Creates a new goal associated with the provided physical expression ID.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to associate with the goal.
    ///
    /// # Returns
    /// The ID of the newly created goal.
    async fn create_goal(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<GoalId>;

    /// Merges goals 1 and 2, unifying them under a common representative.
    ///
    /// May trigger cascading merges of parent entities.
    ///
    /// # Parameters
    /// * `goal_id_1` - ID of the first goal to merge.
    /// * `goal_id_2` - ID of the second goal to merge.
    ///
    /// # Returns
    /// A vector of merge results for all affected entities.
    async fn merge_goals(
        &mut self,
        goal_id_1: GoalId,
        goal_id_2: GoalId,
    ) -> MemoizeResult<Vec<MergeResult>>;

    /// Updates the cost of a physical expression ID.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to update.
    ///
    /// # Returns
    /// A tuple containing:
    /// - Whether the expression is now the best for its goal.
    /// - The goal ID this expression belongs to.
    async fn update_physical_expr_cost(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<(bool, GoalId)>;

    //
    // Rule and costing status operations.
    //

    /// Checks the status of applying a transformation rule on a logical expression ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to check.
    /// * `rule` - Transformation rule to check status for.
    ///
    /// # Returns
    /// `Status::Dirty` if there are ongoing events that may affect the transformation,
    /// `Status::Clean` if the transformation does not need to be re-evaluated.
    async fn get_transformation_status(
        &self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
    ) -> MemoizeResult<Status>;

    /// Sets the status of applying a transformation rule on a logical expression ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to update.
    /// * `rule` - Transformation rule to set status for.
    /// * `status` - The new status to set.
    async fn set_transformation_status(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
        status: Status,
    ) -> MemoizeResult<()>;

    /// Checks the status of applying an implementation rule on a logical expression ID and goal ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to check.
    /// * `goal_id` - ID of the goal to check against.
    /// * `rule` - Implementation rule to check status for.
    ///
    /// # Returns
    /// `Status::Dirty` if there are ongoing events that may affect the implementation,
    /// `Status::Clean` if the implementation does not need to be re-evaluated.
    async fn get_implementation_status(
        &self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
    ) -> MemoizeResult<Status>;

    /// Sets the status of applying an implementation rule on a logical expression ID and goal ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to update.
    /// * `goal_id` - ID of the goal to update against.
    /// * `rule` - Implementation rule to set status for.
    /// * `status` - The new status to set.
    async fn set_implementation_status(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
        status: Status,
    ) -> MemoizeResult<()>;

    /// Checks the status of costing a physical expression ID.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to check.
    ///
    /// # Returns
    /// `Status::Dirty` if there are ongoing events that may affect the costing,
    /// `Status::Clean` if the costing does not need to be re-evaluated.
    async fn get_cost_status(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<Status>;

    /// Sets the status of costing a physical expression ID.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to update.
    /// * `status` - The new status to set.
    async fn set_cost_status(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        status: Status,
    ) -> MemoizeResult<()>;

    //
    // ID conversion and materialization operations.
    //

    /// Gets or creates a goal ID for a given goal.
    ///
    /// # Parameters
    /// * `goal` - The goal to get or create an ID for.
    ///
    /// # Returns
    /// The ID of the goal.
    async fn get_goal_id(&mut self, goal: &Goal) -> MemoizeResult<GoalId>;

    /// Materializes a goal from its ID.
    ///
    /// # Parameters
    /// * `goal_id` - ID of the goal to materialize.
    ///
    /// # Returns
    /// The materialized goal.
    async fn materialize_goal(&self, goal_id: GoalId) -> MemoizeResult<Goal>;

    /// Gets or creates a logical expression ID for a given logical expression.
    ///
    /// # Parameters
    /// * `logical_expr` - The logical expression to get or create an ID for.
    ///
    /// # Returns
    /// The ID of the logical expression.
    async fn get_logical_expr_id(
        &mut self,
        logical_expr: &LogicalExpression,
    ) -> MemoizeResult<LogicalExpressionId>;

    /// Materializes a logical expression from its ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to materialize.
    ///
    /// # Returns
    /// The materialized logical expression.
    async fn materialize_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<LogicalExpression>;

    /// Gets or creates a physical expression ID for a given physical expression.
    ///
    /// # Parameters
    /// * `physical_expr` - The physical expression to get or create an ID for.
    ///
    /// # Returns
    /// The ID of the physical expression.
    async fn get_physical_expr_id(
        &mut self,
        physical_expr: &PhysicalExpression,
    ) -> MemoizeResult<PhysicalExpressionId>;

    /// Materializes a physical expression from its ID.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to materialize.
    ///
    /// # Returns
    /// The materialized physical expression.
    async fn materialize_physical_expr(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<PhysicalExpression>;
}
