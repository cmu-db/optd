use super::{ForwardResult, OptimizeStateResult, MergeResult, Status};
use crate::core::cir::*;

pub trait OptimizerState: Memo + Materialize + TaskState {}

//
// Logical expression and group operations.
//
//
// Physical expression and goal operations.
//
#[trait_variant::make(Send)]
pub trait Memo {
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
    ) -> OptimizeStateResult<Option<LogicalProperties>>;

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
    ) -> OptimizeStateResult<()>;

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
    ) -> OptimizeStateResult<Vec<LogicalExpressionId>>;

    /// Gets any logical expression ID in a group.
    async fn get_any_logical_expr(&self, group_id: GroupId) -> OptimizeStateResult<LogicalExpressionId>;

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
    ) -> OptimizeStateResult<Option<GroupId>>;

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
    ) -> OptimizeStateResult<GroupId>;

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
    ) -> OptimizeStateResult<Option<MergeResult>>;

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
    async fn get_all_goal_members(&self, goal_id: GoalId) -> OptimizeStateResult<Vec<GoalMemberId>>;

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

    /// Finds the representative group ID for a given group ID.
    ///
    /// # Parameters
    /// * `group_id` - The group ID to find the representative for.
    ///
    /// # Returns
    /// The representative group ID (which may be the same as the input if
    /// it's already the representative).
    async fn find_repr_group(&self, group_id: GroupId) -> OptimizeStateResult<GroupId>;

    /// Finds the representative goal ID for a given goal ID.
    ///
    /// # Parameters
    /// * `goal_id` - The goal ID to find the representative for.
    ///
    /// # Returns
    /// The representative goal ID (which may be the same as the input if
    /// it's already the representative).
    async fn find_repr_goal(&self, goal_id: GoalId) -> OptimizeStateResult<GoalId>;

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
        logical_expr_id: LogicalExpressionId,
    ) -> OptimizeStateResult<LogicalExpressionId>;

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
        physical_expr_id: PhysicalExpressionId,
    ) -> OptimizeStateResult<PhysicalExpressionId>;
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
pub trait TaskState {
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
    ) -> OptimizeStateResult<Status>;

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
    ) -> OptimizeStateResult<Status>;

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
    ) -> OptimizeStateResult<Status>;

    /// Sets the status of costing a physical expression ID as clean.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to update.
    async fn set_cost_clean(&mut self, physical_expr_id: PhysicalExpressionId)
    -> OptimizeStateResult<()>;

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
