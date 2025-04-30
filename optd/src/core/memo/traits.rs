use super::{ForwardResult, MergeResult, OptimizeStateResult, TaskStatus};
use crate::core::cir::*;

/// The main interface for tracking state needed by the optimizer. This includes the memo table and
/// state needed for the task graph.
pub trait OptimizerState: Memo + Materialize + TaskGraphState {}

/// The interface for a `Group` of logical expressions.
///
/// Implementors of this trait should be able to track the logical expressions belonging to this
/// `Group` via [`LogicalExpressionId`], as well as the derived [`LogicalProperties`] and related
/// [`Goal`]s via [`GoalId`]s.
pub trait Group {
    /// Creates a new `Group` from a new [`LogicalExpressionId`].
    fn new_from_logical_expression(id: LogicalExpressionId) -> Self;

    /// Retrieves an iterator of [`LogicalExpressionId`] contained in the `Group`.
    fn logical_expressions(&self) -> impl Iterator<Item = LogicalExpressionId>;

    /// Checks if the `Group` contains a logical expression by ID.
    fn contains_logical_expression(&self, id: LogicalExpressionId) -> bool;

    /// Adds a logical expression to a `Group`.
    fn add_logical_expression(&mut self, id: LogicalExpressionId);

    /// Removes a logical expression to a `Group`.
    fn remove_logical_expression(&mut self, id: LogicalExpressionId);

    /// Retrieves the logical properties of a `Group`.
    fn logical_properties(&self) -> Option<LogicalProperties>;

    /// Replaces the logical properties for a `Group`.
    fn replace_logical_properties(&mut self, props: LogicalProperties)
    -> Option<LogicalProperties>;

    /// The IDs of the [`Goal`]s that are dependent on this `Group`.
    fn goals(&self) -> impl Iterator<Item = GoalId>;

    /// Add a related [`GoalId`] to this `Group`.
    fn add_goal(&mut self, goal_id: GoalId);
}

/// The interface for an optimizer memoization (memo) table.
///
/// This trait mainly describes operations related to groups, goals, logical and physical
/// expressions, and finding representative nodes of the union-find substructures.
#[trait_variant::make(Send)]
pub trait Memo {
    /// The associated type needed for managing `Group` data.
    type GroupState: Group;

    /// Retrives the `GroupState` data given the group's ID.
    async fn get_group(&self, group_id: GroupId) -> &Self::GroupState;

    /// Mutably retrives the `GroupState` data given the group's ID.
    async fn get_group_mut(&mut self, group_id: GroupId) -> &mut Self::GroupState;

    /// Finds the representative group of a given group. The representative is usually tracked via a
    /// Union-Find data structure.
    ///
    /// If the input group is already the representative, then the returned [`GroupId`] is equal to
    /// the input [`GroupId`].
    async fn find_repr_group(&self, group_id: GroupId) -> GroupId;

    /// Finds the representative goal of a given goal. The representative is usually tracked via a
    /// Union-Find data structure.
    ///
    /// If the input goal is already the representative, then the returned [`GoalId`] is equal to
    /// the input [`GoalId`].
    async fn find_repr_goal(&self, goal_id: GoalId) -> GoalId;

    /// Finds the representative logical expression of a given expression. The representative is
    /// usually tracked via a Union-Find data structure.
    ///
    /// If the input expression is already the representative, then the returned
    /// [`LogicalExpressionId`] is equal to the input [`LogicalExpressionId`].
    async fn find_repr_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> LogicalExpressionId;

    /// Finds the representative physical expression of a given expression. The representative is
    /// usually tracked via a Union-Find data structure.
    ///
    /// If the input expression is already the representative, then the returned
    /// [`PhysicalExpressionId`] is equal to the input [`PhysicalExpressionId`].
    async fn find_repr_physical_expr(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> PhysicalExpressionId;

    /// Finds the ID of the representative group containing the given logical expression ID.
    ///
    /// If there is no `Group` that contains the input logical expression ID, this returns `None`.
    async fn find_group_of_logical_expression(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> Option<GroupId>;

    /// Creates a new group given a new [`LogicalExpressionId`].
    ///
    /// Returns The ID of the newly created group.
    async fn create_group(&mut self, logical_expr_id: LogicalExpressionId) -> GroupId;

    /// Merges two groups, unifying them under a common representative group.
    ///
    /// This function can trigger cascading (recursive) merges of parent groups & goals.
    ///
    /// TODO(connor): Clean up
    /// # Returns
    /// Merge results for all affected entities including newly dirtied
    /// transformations, implementations and costings.
    ///
    /// Should panic if the groups are equal (instead of returning an option)
    async fn merge_groups(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> OptimizeStateResult<MergeResult>;

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
    ) -> OptimizeStateResult<Option<(PhysicalExpressionId, Cost)>>;

    /// Gets all members of a goal, which can be physical expressions or other goals.
    ///
    /// # Parameters
    /// * `goal_id` - ID of the goal to retrieve members from.
    ///
    /// # Returns
    /// A vector of goal members, each being either a physical expression ID or another goal ID.
    async fn get_all_goal_members(&self, goal_id: GoalId)
    -> OptimizeStateResult<Vec<GoalMemberId>>;

    /// Adds a member to a goal.
    ///
    /// # Parameters
    /// * `goal_id` - ID of the goal to add the member to.
    /// * `member` - The member to add, either a physical expression ID or another goal ID.
    ///
    /// # Returns
    /// True if the member was added to the goal, or false if it already existed.
    async fn add_goal_member(
        &mut self,
        goal_id: GoalId,
        member: GoalMemberId,
    ) -> OptimizeStateResult<Option<ForwardResult>>;

    /// Updates the cost of a physical expression ID.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to update.
    /// * `new_cost` - New cost to assign to the physical expression.
    ///
    /// # Returns
    /// Whether the cost of the expression has improved.
    async fn update_physical_expr_cost(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        new_cost: Cost,
    ) -> OptimizeStateResult<Option<ForwardResult>>;

    async fn get_physical_expr_cost(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> OptimizeStateResult<Option<Cost>>;
}

#[trait_variant::make(Send)]
pub trait Materialize {
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
    async fn get_goal_id(&mut self, goal: &Goal) -> OptimizeStateResult<GoalId>;

    /// Materializes a goal from its ID.
    ///
    /// # Parameters
    /// * `goal_id` - ID of the goal to materialize.
    ///
    /// # Returns
    /// The materialized goal.
    async fn materialize_goal(&self, goal_id: GoalId) -> OptimizeStateResult<Goal>;

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
    ) -> OptimizeStateResult<LogicalExpressionId>;

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
    ) -> OptimizeStateResult<LogicalExpression>;

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
    ) -> OptimizeStateResult<PhysicalExpressionId>;

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
    ) -> OptimizeStateResult<PhysicalExpression>;
}

/// Core interface for memo-based query optimization.
///
/// This trait defines the operations needed to store, retrieve, and manipulate
/// the memo data structure that powers the dynamic programming approach to
/// query optimization. The memo stores logical and physical expressions by their IDs,
/// manages expression properties, and tracks optimization status.
#[trait_variant::make(Send)]
pub trait TaskGraphState {
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
    ) -> OptimizeStateResult<TaskStatus>;

    /// Sets the status of a transformation rule as clean on a logical expression ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to update.
    /// * `rule` - Transformation rule to set status for.
    async fn set_transformation_clean(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
    ) -> OptimizeStateResult<()>;

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
    ) -> OptimizeStateResult<TaskStatus>;

    /// Sets the status of an implementation rule as clean on a logical expression ID and goal ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to update.
    /// * `goal_id` - ID of the goal to update against.
    /// * `rule` - Implementation rule to set status for.
    async fn set_implementation_clean(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
    ) -> OptimizeStateResult<()>;

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
    ) -> OptimizeStateResult<TaskStatus>;

    /// Sets the status of costing a physical expression ID as clean.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to update.
    async fn set_cost_clean(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
    ) -> OptimizeStateResult<()>;

    /// Adds a dependency between a transformation rule application and a group.
    ///
    /// This registers that the application of the transformation rule on the logical expression
    /// depends on the group. When the group changes, the transformation status should be set to dirty.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression the rule is applied to.
    /// * `rule` - Transformation rule that depends on the group.
    /// * `group_id` - ID of the group that the transformation depends on.
    async fn add_transformation_dependency(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
        group_id: GroupId,
    ) -> OptimizeStateResult<()>;

    /// Adds a dependency between an implementation rule application and a group.
    ///
    /// This registers that the application of the implementation rule on the logical expression
    /// for a specific goal depends on the group. When the group changes, the implementation status
    /// should be set to dirty.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression the rule is applied to.
    /// * `goal_id` - ID of the goal the implementation targets.
    /// * `rule` - Implementation rule that depends on the group.
    /// * `group_id` - ID of the group that the implementation depends on.
    async fn add_implementation_dependency(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
        group_id: GroupId,
    ) -> OptimizeStateResult<()>;

    /// Adds a dependency between costing a physical expression and a goal.
    ///
    /// This registers that the costing of the physical expression depends on the goal.
    /// When the goal changes, the costing status should be set to dirty.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to cost.
    /// * `goal_id` - ID of the goal that the costing depends on.
    async fn add_cost_dependency(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        goal_id: GoalId,
    ) -> OptimizeStateResult<()>;
}
