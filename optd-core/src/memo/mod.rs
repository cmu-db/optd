#[cfg(test)]
pub mod memory;
mod merge_repr;

use std::collections::{HashMap, HashSet};

use async_recursion::async_recursion;

use crate::cir::{
    Cost, Goal, GoalId, GoalMemberId, GroupId, ImplementationRule, LogicalExpression,
    LogicalExpressionId, LogicalProperties, PhysicalExpression, PhysicalExpressionId,
    TransformationRule,
};

/// Type alias for results returned by Memoize trait methods
pub type MemoizeResult<T> = Result<T, MemoizeError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoizeError {
    /// Error indicating that a group ID was not found in the memo.
    GroupNotFound(GroupId),

    /// Error indicating that a goal ID was not found in the memo.
    GoalNotFound(GoalId),

    /// Error indicating that a logical expression ID was not found in the memo.
    LogicalExprNotFound(LogicalExpressionId),

    /// Error indicating that a physical expression ID was not found in the memo.
    PhysicalExprNotFound(PhysicalExpressionId),

    /// Error indicating that there is no logical expression in the group.
    NoLogicalExprInGroup(GroupId),
}

/// Status of a rule application or costing operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// There exist ongoing jobs that may generate more expressions or costs from this expression.
    Dirty,

    /// Expression is fully explored or costed with no pending jobs that could add anything new.
    Clean,
}

/// Result of merging two groups.
#[derive(Debug)]
pub struct MergeGroupResult {
    /// ID of the new representative group id.
    pub new_repr_group_id: GroupId,
    pub old_non_repr_group_id: GroupId,

    // A Key assumption here is that all expressions here are representative expressions i.e. the children group IDs of the expressions are all repr group IDs.
    /// All expressions in the merged group.
    pub all_exprs_in_merged_group: HashSet<LogicalExpressionId>,

    // A Key assumption here is that all expressions here are representative expressions i.e. the children group IDs of the expressions are all repr group IDs.
    /// The expressions that were in the old non-repr group.
    pub old_non_repr_group_exprs: HashSet<LogicalExpressionId>,

    // A Key assumption here is that all expressions here are representative expressions i.e. the children group IDs of the expressions are all repr group IDs.
    /// The expressions that are in the repr group.
    pub new_repr_group_exprs: HashSet<LogicalExpressionId>,
}

impl MergeGroupResult {
    /// Creates a new MergeGroupResult instance.
    ///
    /// # Parameters
    /// * `merged_groups` - Groups that were merged along with their expressions.
    /// * `new_repr_group_id` - ID of the new representative group id.
    pub fn new(new_repr_group_id: GroupId, old_non_repr_group_id: GroupId) -> Self {
        Self {
            new_repr_group_id,
            old_non_repr_group_id,
            all_exprs_in_merged_group: HashSet::new(),
            old_non_repr_group_exprs: HashSet::new(),
            new_repr_group_exprs: HashSet::new(),
        }
    }
}

/// Result of merging two goals.
#[derive(Debug)]
pub struct MergeGoalResult {
    /// The best costed expression for all merged goals combined.
    pub best_expr: Option<(PhysicalExpressionId, Cost)>,

    /// ID of the new representative goal id.
    pub new_repr_goal_id: GoalId,

    /// ID of the old non-representative goal id.
    pub old_non_repr_goal_id: GoalId,

    /// Whether the representative goal contained the best costed expression before merging.
    pub repr_goal_seen_best_expr_before_merge: bool,

    /// Whether the non-representative goal contained the best costed expression before merging.
    pub non_repr_goal_seen_best_expr_before_merge: bool,

    /// All members in the merged goal, unlike for group merge results, these may not necessarily be representative IDs.
    /// The reasoning for this is that there is an edge case where the merging two groups results in 2 pairs of goals being merged.
    /// However, if one fo the goals is a member of the other goals, then we cannot guarantee the order in which the merge will happen.
    /// Hence, we cannot guarantee that the members are representative IDs.
    pub members: HashSet<GoalMemberId>,
}

/// Result of merging two cost expressions.
#[derive(Debug)]
pub struct MergePhysicalExprResult {
    /// The new representative physical expression id.
    pub repr_physical_expr: PhysicalExpressionId,

    /// Physical expressions that were stale
    pub stale_physical_exprs: HashSet<PhysicalExpressionId>,
}

/// Results of merge operations with newly dirtied expressions.
#[derive(Debug, Default)]
pub struct MergeResult {
    /// Group merge results.
    pub group_merges: Vec<MergeGroupResult>,

    /// Goal merge results.
    pub goal_merges: Vec<MergeGoalResult>,

    /// Physical expression merge results.
    pub physical_expr_merges: Vec<MergePhysicalExprResult>,
    // /// Transformations that were marked as dirty and need new application.
    // pub dirty_transformations: Vec<(LogicalExpressionId, TransformationRule)>,

    // /// Implementations that were marked as dirty and need new application.
    // pub dirty_implementations: Vec<(LogicalExpressionId, GoalId, ImplementationRule)>,

    // /// Costings that were marked as dirty and need recomputation.
    // pub dirty_costings: Vec<PhysicalExpressionId>,
}

pub struct ForwardResult {
    pub physical_expr_id: PhysicalExpressionId,
    pub best_cost: Cost,
    pub goals_forwarded: HashSet<GoalId>,
}

impl ForwardResult {
    pub fn new(physical_expr_id: PhysicalExpressionId, best_cost: Cost) -> Self {
        Self {
            physical_expr_id,
            best_cost,
            goals_forwarded: HashSet::new(),
        }
    }
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
    async fn get_logical_properties(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Option<LogicalProperties>>;

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
    ) -> MemoizeResult<()>;

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
    ) -> MemoizeResult<Vec<LogicalExpressionId>>;

    /// Gets any logical expression ID in a group.
    async fn get_any_logical_expr(&self, group_id: GroupId) -> MemoizeResult<LogicalExpressionId>;

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
    ) -> MemoizeResult<GroupId>;

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
    ) -> MemoizeResult<Option<MergeResult>>;

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

    /// Gets all members of a goal, which can be physical expressions or other goals.
    ///
    /// # Parameters
    /// * `goal_id` - ID of the goal to retrieve members from.
    ///
    /// # Returns
    /// A vector of goal members, each being either a physical expression ID or another goal ID.
    async fn get_all_goal_members(&self, goal_id: GoalId) -> MemoizeResult<Vec<GoalMemberId>>;

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
    ) -> MemoizeResult<Option<ForwardResult>>;

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
    ) -> MemoizeResult<Option<ForwardResult>>;

    async fn get_physical_expr_cost(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<Option<Cost>>;

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

    /// Sets the status of a transformation rule as clean on a logical expression ID.
    ///
    /// # Parameters
    /// * `logical_expr_id` - ID of the logical expression to update.
    /// * `rule` - Transformation rule to set status for.
    async fn set_transformation_clean(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
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

    /// Sets the status of costing a physical expression ID as clean.
    ///
    /// # Parameters
    /// * `physical_expr_id` - ID of the physical expression to update.
    async fn set_cost_clean(&mut self, physical_expr_id: PhysicalExpressionId)
    -> MemoizeResult<()>;

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
    ) -> MemoizeResult<()>;

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
    ) -> MemoizeResult<()>;

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
    async fn find_repr_group(&self, group_id: GroupId) -> MemoizeResult<GroupId>;

    /// Finds the representative goal ID for a given goal ID.
    ///
    /// # Parameters
    /// * `goal_id` - The goal ID to find the representative for.
    ///
    /// # Returns
    /// The representative goal ID (which may be the same as the input if
    /// it's already the representative).
    async fn find_repr_goal(&self, goal_id: GoalId) -> MemoizeResult<GoalId>;

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
    ) -> MemoizeResult<LogicalExpressionId>;

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
    ) -> MemoizeResult<PhysicalExpressionId>;
}
