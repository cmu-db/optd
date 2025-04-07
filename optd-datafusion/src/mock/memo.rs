use optd_core::cir::*;
use optd_core::memo::{Memoize, MemoizeResult, MergeResult, Status};

#[derive(Debug)]
pub struct MockMemo;

impl MockMemo {
    pub async fn new() -> Self {
        Self
    }
}

impl Memoize for MockMemo {
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
    async fn get_logical_properties(&self, _group_id: GroupId) -> MemoizeResult<LogicalProperties> {
        todo!()
    }

    /// Gets all logical expression IDs in a group (only representative IDs).
    ///
    /// # Parameters
    /// * `group_id` - ID of the group to retrieve expressions from.
    ///
    /// # Returns
    /// A vector of logical expression IDs in the specified group.
    async fn get_all_logical_exprs(
        &self,
        _group_id: GroupId,
    ) -> MemoizeResult<Vec<LogicalExpressionId>> {
        todo!()
    }

    /// Finds group containing a logical expression ID, if it exists.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to find.
    ///
    /// # Returns
    /// The group ID if the expression exists, None otherwise.
    async fn find_logical_expr_group(
        &self,
        _logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<Option<GroupId>> {
        todo!()
    }

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
        _logical_expr_id: LogicalExpressionId,
        _props: &LogicalProperties,
    ) -> MemoizeResult<GroupId> {
        todo!()
    }

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
        _group_id_1: GroupId,
        _group_id_2: GroupId,
    ) -> MemoizeResult<MergeResult> {
        todo!()
    }

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
        _goal_id: GoalId,
    ) -> MemoizeResult<Option<(PhysicalExpressionId, Cost)>> {
        todo!()
    }

    /// Gets all physical expression IDs in a goal (only representative IDs).
    ///
    /// # Parameters
    /// * `goal_id` - ID of the goal to retrieve expressions from.
    ///
    /// # Returns
    /// A vector of physical expression IDs in the specified goal.
    async fn get_all_physical_exprs(
        &self,
        _goal_id: GoalId,
    ) -> MemoizeResult<Vec<PhysicalExpressionId>> {
        todo!()
    }

    /// Gets all members of a goal, which can be physical expressions or other goals.
    ///
    /// # Parameters
    /// * `goal_id` - ID of the goal to retrieve members from.
    ///
    /// # Returns
    /// A vector of goal members, each being either a physical expression ID or another goal ID.
    async fn get_all_goal_members(&self, _goal_id: GoalId) -> MemoizeResult<Vec<GoalMemberId>> {
        todo!()
    }

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
        _goal_id: GoalId,
        _member: GoalMemberId,
    ) -> MemoizeResult<bool> {
        todo!()
    }

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
        _physical_expr_id: PhysicalExpressionId,
        _new_cost: Cost,
    ) -> MemoizeResult<bool> {
        todo!()
    }

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
        _logical_expr_id: LogicalExpressionId,
        _rule: &TransformationRule,
    ) -> MemoizeResult<Status> {
        todo!()
    }

    /// Sets the status of a transformation rule as clean on a logical expression ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to update.
    /// * `rule` - Transformation rule to set status for.
    async fn set_transformation_clean(
        &mut self,
        _logical_expr_id: LogicalExpressionId,
        _rule: &TransformationRule,
    ) -> MemoizeResult<()> {
        todo!()
    }

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
        _logical_expr_id: LogicalExpressionId,
        _goal_id: GoalId,
        _rule: &ImplementationRule,
    ) -> MemoizeResult<Status> {
        todo!()
    }

    /// Sets the status of an implementation rule as clean on a logical expression ID and goal ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to update.
    /// * `goal_id` - ID of the goal to update against.
    /// * `rule` - Implementation rule to set status for.
    async fn set_implementation_clean(
        &mut self,
        _logical_expr_id: LogicalExpressionId,
        _goal_id: GoalId,
        _rule: &ImplementationRule,
    ) -> MemoizeResult<()> {
        todo!()
    }

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
        _physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<Status> {
        todo!()
    }

    /// Sets the status of costing a physical expression ID as clean.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to update.
    async fn set_cost_clean(
        &mut self,
        _physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<()> {
        todo!()
    }

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
        _logical_expr_id: LogicalExpressionId,
        _rule: &TransformationRule,
        _group_id: GroupId,
    ) -> MemoizeResult<()> {
        todo!()
    }

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
        _logical_expr_id: LogicalExpressionId,
        _goal_id: GoalId,
        _rule: &ImplementationRule,
        _group_id: GroupId,
    ) -> MemoizeResult<()> {
        todo!()
    }

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
        _physical_expr_id: PhysicalExpressionId,
        _goal_id: GoalId,
    ) -> MemoizeResult<()> {
        todo!()
    }

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
    async fn get_goal_id(&mut self, _goal: &Goal) -> MemoizeResult<GoalId> {
        todo!()
    }

    /// Materializes a goal from its ID.
    ///
    /// # Parameters
    /// * `goal_id` - ID of the goal to materialize.
    ///
    /// # Returns
    /// The materialized goal.
    async fn materialize_goal(&self, _goal_id: GoalId) -> MemoizeResult<Goal> {
        todo!()
    }

    /// Gets or creates a logical expression ID for a given logical expression.
    ///
    /// # Parameters
    /// * `logical_expr` - The logical expression to get or create an ID for.
    ///
    /// # Returns
    /// The ID of the logical expression.
    async fn get_logical_expr_id(
        &mut self,
        _logical_expr: &LogicalExpression,
    ) -> MemoizeResult<LogicalExpressionId> {
        todo!()
    }

    /// Materializes a logical expression from its ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to materialize.
    ///
    /// # Returns
    /// The materialized logical expression.
    async fn materialize_logical_expr(
        &self,
        _logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<LogicalExpression> {
        todo!()
    }

    /// Gets or creates a physical expression ID for a given physical expression.
    ///
    /// # Parameters
    /// * `physical_expr` - The physical expression to get or create an ID for.
    ///
    /// # Returns
    /// The ID of the physical expression.
    async fn get_physical_expr_id(
        &mut self,
        _physical_expr: &PhysicalExpression,
    ) -> MemoizeResult<PhysicalExpressionId> {
        todo!()
    }

    /// Materializes a physical expression from its ID.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to materialize.
    ///
    /// # Returns
    /// The materialized physical expression.
    async fn materialize_physical_expr(
        &self,
        _physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<PhysicalExpression> {
        todo!()
    }

    //
    // Representative ID operations.
    //

    /// Finds the representative group ID for a given group ID.
    ///
    /// # Parameters
    /// * `group_id` - The group ID to find the representative for.
    ///
    /// # Returns
    /// The representative group ID (which may be the same as the input if
    /// it's already the representative).
    async fn find_repr_group(&self, _group_id: GroupId) -> MemoizeResult<GroupId> {
        todo!()
    }

    /// Finds the representative goal ID for a given goal ID.
    ///
    /// # Parameters
    /// * `goal_id` - The goal ID to find the representative for.
    ///
    /// # Returns
    /// The representative goal ID (which may be the same as the input if
    /// it's already the representative).
    async fn find_repr_goal(&self, _goal_id: GoalId) -> MemoizeResult<GoalId> {
        todo!()
    }

    /// Finds the representative logical expression ID for a given logical expression ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - The logical expression ID to find the representative for.
    ///
    /// # Returns
    /// The representative logical expression ID (which may be the same as the input if
    /// it's already the representative).
    async fn find_repr_logical_expr(
        &self,
        _logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<LogicalExpressionId> {
        todo!()
    }

    /// Finds the representative physical expression ID for a given physical expression ID.
    ///
    /// # Parameters
    /// * `physical_expr_id` - The physical expression ID to find the representative for.
    ///
    /// # Returns
    /// The representative physical expression ID (which may be the same as the input if
    /// it's already the representative).
    async fn find_repr_physical_expr(
        &self,
        _physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<PhysicalExpressionId> {
        todo!()
    }
}
