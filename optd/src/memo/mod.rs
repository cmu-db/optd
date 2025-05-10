use crate::cir::{
    Cost, Goal, GoalId, GoalMemberId, GroupId, LogicalExpression, LogicalExpressionId,
    LogicalProperties, PhysicalExpression, PhysicalExpressionId,
};
use error::MemoResult;
use std::collections::HashSet;

pub(crate) mod error;
pub mod memory;

pub(crate) use error::MemoError;
pub use memory::MemoryMemo;

/// Result of merging two groups.
#[derive(Debug)]
pub struct MergeGroupProduct {
    /// ID of the new representative group id.
    pub new_repr_group_id: GroupId,

    /// Groups that were merged along with their expressions.
    pub merged_groups: Vec<GroupId>,
}

/// Result of merging two goals.
#[derive(Debug)]
pub struct MergeGoalProduct {
    /// ID of the new representative goal id.
    pub new_repr_goal_id: GoalId,

    /// Goals that were merged along with their potential best costed expression.
    pub merged_goals: Vec<GoalId>,
}

/// Results of merge operations, including group and goal merges.
#[derive(Debug, Default)]
pub struct MergeProducts {
    /// Group merge results.
    pub group_merges: Vec<MergeGroupProduct>,

    /// Goal merge results.
    pub goal_merges: Vec<MergeGoalProduct>,
}

/// A helper trait to help facilitate finding the representative IDs of elements.
#[trait_variant::make(Send)]
pub trait Representative {
    /// Finds the representative group of a given group. The representative is usually tracked via a
    /// Union-Find data structure.
    ///
    /// If the input group is already the representative, then the returned [`GroupId`] is equal to
    /// the input [`GroupId`].
    async fn find_repr_group_id(&self, group_id: GroupId) -> MemoResult<GroupId>;

    /// Finds the representative goal of a given goal. The representative is usually tracked via a
    /// Union-Find data structure.
    ///
    /// If the input goal is already the representative, then the returned [`GoalId`] is equal to
    /// the input [`GoalId`].
    async fn find_repr_goal_id(&self, goal_id: GoalId) -> MemoResult<GoalId>;

    /// Finds the representative logical expression of a given expression. The representative is
    /// usually tracked via a Union-Find data structure.
    ///
    /// If the input expression is already the representative, then the returned
    /// [`LogicalExpressionId`] is equal to the input [`LogicalExpressionId`].
    async fn find_repr_logical_expr_id(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoResult<LogicalExpressionId>;

    /// Finds the representative physical expression of a given expression. The representative is
    /// usually tracked via a Union-Find data structure.
    ///
    /// If the input expression is already the representative, then the returned
    /// [`PhysicalExpressionId`] is equal to the input [`PhysicalExpressionId`].
    async fn find_repr_physical_expr_id(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoResult<PhysicalExpressionId>;
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
pub trait Memo: Representative + Materialize + Sync + 'static {
    /// Retrieves logical properties for a group ID.
    ///
    /// # Parameters
    /// * `group_id` - ID of the group to retrieve properties for.
    ///
    /// # Returns
    /// The properties associated with the group or an error if not found.
    async fn get_logical_properties(&self, group_id: GroupId) -> MemoResult<LogicalProperties>;

    /// Gets all logical expression IDs in a group (only representative IDs).
    ///
    /// # Parameters
    /// * `group_id` - ID of the group to retrieve expressions from.
    ///
    /// # Returns
    /// A set of logical expression IDs in the specified group.
    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoResult<HashSet<LogicalExpressionId>>;

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
    async fn create_group(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        props: &LogicalProperties,
    ) -> MemoResult<GroupId>;

    /// Merges groups 1 and 2, unifying them under a common representative.
    ///
    /// May trigger cascading merges of parent groups & goals.
    ///
    /// # Parameters
    /// * `group_id_1` - ID of the first group to merge.
    /// * `group_id_2` - ID of the second group to merge.
    ///
    /// # Returns
    /// Merge results for all affected entities.
    async fn merge_groups(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> MemoResult<MergeProducts>;

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
    async fn add_goal_member(&mut self, goal_id: GoalId, member: GoalMemberId) -> MemoResult<bool>;

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
    ) -> MemoResult<bool>;

    /// Gets the cost of a physical expression ID.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to retrieve the cost for.
    ///
    /// # Returns
    /// The cost of the physical expression, or None if it doesn't exist.
    async fn get_physical_expr_cost(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoResult<Option<Cost>>;
}
