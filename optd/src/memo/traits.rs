use super::{MemoResult, MergeProducts, PropagateBestExpression, TaskStatus};
use crate::core::cir::*;

/// A helper trait to help facilitate finding the representative IDs of elements.
#[trait_variant::make(Send)]
pub trait Representative {
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
}

/// A helper trait to help facilitate the materialization and creation of objects in the memo table.
#[trait_variant::make(Send)]
pub trait Materialize {
    /// Retrieves the ID of a [`Goal`]. If the [`Goal`] does not already exist in the memo table,
    /// creates a new [`Goal`] and returns a fresh [`GoalId`].
    async fn get_goal_id(&mut self, goal: &Goal) -> MemoResult<GoalId>;

    /// Materializes a [`Goal`] from its [`GoalId`].
    async fn materialize_goal(&self, goal_id: GoalId) -> MemoResult<Goal>;

    /// Retrieves the ID of a [`LogicalExpression`]. If the [`LogicalExpression`] does not already
    /// exist in the memo table, creates a new [`LogicalExpression`] and returns a fresh
    /// [`LogicalExpressionId`].
    async fn get_logical_expr_id(
        &mut self,
        logical_expr: &LogicalExpression,
    ) -> MemoResult<LogicalExpressionId>;

    /// Materializes a [`LogicalExpression`] from its [`LogicalExpressionId`].
    async fn materialize_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoResult<LogicalExpression>;

    /// Retrieves the ID of a [`PhysicalExpression`]. If the [`PhysicalExpression`] does not already
    /// exist in the memo table, creates a new [`PhysicalExpression`] and returns a fresh
    /// [`PhysicalExpressionId`].
    async fn get_physical_expr_id(
        &mut self,
        physical_expr: &PhysicalExpression,
    ) -> MemoResult<PhysicalExpressionId>;

    /// Materializes a [`PhysicalExpression`] from its [`PhysicalExpressionId`].
    async fn materialize_physical_expr(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoResult<PhysicalExpression>;
}

/// The interface for an optimizer memoization (memo) table.
///
/// This trait mainly describes operations related to groups, goals, logical and physical
/// expressions, and finding representative nodes of the union-find substructures.
#[trait_variant::make(Send)]
pub trait Memo: Representative + Materialize + TaskGraphState + Sync + 'static {
    /// Retrieves logical properties for a group ID.
    ///
    /// # Parameters
    /// * `group_id` - ID of the group to retrieve properties for.
    ///
    /// # Returns
    /// The properties associated with the group or an error if not found.
    async fn get_logical_properties(
        &self,
        group_id: GroupId,
    ) -> MemoResult<Option<LogicalProperties>>;

    /// Sets logical properties for a group ID.
    ///
    /// # Parameters
    /// * `group_id` - ID of the group to set properties for.
    /// * `props` - The logical properties to associate with the group.
    ///
    /// # Returns
    /// A result indicating success or failure of the operation.
    async fn set_logical_properties(
        &mut self,
        group_id: GroupId,
        props: LogicalProperties,
    ) -> MemoResult<()>;

    /// Gets all logical expression IDs in a group (only representative IDs).
    ///
    /// # Parameters
    /// * `group_id` - ID of the group to retrieve expressions from.
    ///
    /// # Returns
    /// A vector of logical expression IDs in the specified group.
    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoResult<Vec<LogicalExpressionId>>;

    /// Gets any logical expression ID in a group.
    async fn get_any_logical_expr(&self, group_id: GroupId) -> MemoResult<LogicalExpressionId>;

    /// Finds group containing a logical expression ID, if it exists.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to find.
    ///
    /// # Returns
    /// The group ID if the expression exists, None otherwise.
    async fn find_logical_expr_group(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoResult<Option<GroupId>>;

    /// Creates a new group with a logical expression ID and properties.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to add to the group.
    /// * `props` - Logical properties for the group.
    ///
    /// # Returns
    /// The ID of the newly created group.
    async fn create_group(&mut self, logical_expr_id: LogicalExpressionId) -> MemoResult<GroupId>;

    /// Merges groups 1 and 2, unifying them under a common representative.
    ///
    /// May trigger cascading merges of parent groups & goals.
    ///
    /// # Parameters
    /// * `group_id_1` - ID of the first group to merge.
    /// * `group_id_2` - ID of the second group to merge.
    ///
    /// # Returns
    /// Merge results for all affected entities including newly dirtied
    /// transformations, implementations and costings.
    async fn merge_groups(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> MemoResult<Option<MergeProducts>>;

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
    ) -> MemoResult<Option<(PhysicalExpressionId, Cost)>>;

    /// Gets all members of a goal, which can be physical expressions or other goals.
    ///
    /// # Parameters
    /// * `goal_id` - ID of the goal to retrieve members from.
    ///
    /// # Returns
    /// A vector of goal members, each being either a physical expression ID or another goal ID.
    async fn get_all_goal_members(&self, goal_id: GoalId) -> MemoResult<Vec<GoalMemberId>>;

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
    ) -> MemoResult<Option<PropagateBestExpression>>;

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
    ) -> MemoResult<Option<PropagateBestExpression>>;

    async fn get_physical_expr_cost(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoResult<Option<Cost>>;
}

/// Rule and costing status operations.
///
/// TODO(connor): Clean up docs.
#[trait_variant::make(Send)]
pub trait TaskGraphState {
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
    ) -> MemoResult<TaskStatus>;

    /// Sets the status of a transformation rule as clean on a logical expression ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to update.
    /// * `rule` - Transformation rule to set status for.
    async fn set_transformation_clean(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
    ) -> MemoResult<()>;

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
    ) -> MemoResult<TaskStatus>;

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
    ) -> MemoResult<()>;

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
    ) -> MemoResult<TaskStatus>;

    /// Sets the status of costing a physical expression ID as clean.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to update.
    async fn set_cost_clean(&mut self, physical_expr_id: PhysicalExpressionId) -> MemoResult<()>;

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
    ) -> MemoResult<()>;

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
    ) -> MemoResult<()>;

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
    ) -> MemoResult<()>;
}
