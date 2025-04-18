use crate::core::{
    cir::{
        Cost, Goal, GoalId, GoalMemberId, GroupId, ImplementationRule, LogicalExpression,
        LogicalExpressionId, LogicalProperties, PhysicalExpression, PhysicalExpressionId,
        TransformationRule,
    },
    error::Error,
};

/// Type alias for results returned by Memoize trait methods
pub type MemoizeResult<T> = Result<T, Error>;

/// Status of a rule application or costing operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// There exist ongoing jobs that may generate more expressions or costs from this expression.
    Dirty,

    /// Expression is fully explored or costed with no pending jobs that could add anything new.
    Clean,
}

/// Information about a merged group, including its ID and expressions
#[derive(Debug)]
pub struct MergedGroupInfo {
    /// ID of the merged group
    pub group_id: GroupId,

    /// All logical expressions in this group
    pub expressions: Vec<LogicalExpressionId>,
}

/// Result of merging two groups.
#[derive(Debug)]
pub struct MergeGroupResult {
    /// Groups that were merged along with their expressions.
    pub merged_groups: Vec<MergedGroupInfo>,

    /// ID of the new representative group id.
    pub new_repr_group_id: GroupId,
}

/// Information about a merged goal, including its ID and expressions
#[derive(Debug)]
pub struct MergedGoalInfo {
    /// ID of the merged goal
    pub goal_id: GoalId,

    /// The best costed expression for this goal, if any
    pub best_expr: Option<(PhysicalExpressionId, Cost)>,

    /// All members in this goal, which can be physical expressions or references to other goals
    pub members: Vec<GoalMemberId>,
}

/// Result of merging two goals.
#[derive(Debug)]
pub struct MergeGoalResult {
    /// Goals that were merged along with their potential best costed expression.
    pub merged_goals: Vec<MergedGoalInfo>,

    /// ID of the new representative goal id.
    pub new_repr_goal_id: GoalId,
}

/// Results of merge operations with newly dirtied expressions.
#[derive(Debug)]
pub struct MergeResult {
    /// Group merge results.
    pub group_merges: Vec<MergeGroupResult>,

    /// Goal merge results.
    pub goal_merges: Vec<MergeGoalResult>,

    /// Transformations that were marked as dirty and need new application.
    pub dirty_transformations: Vec<(LogicalExpressionId, TransformationRule)>,

    /// Implementations that were marked as dirty and need new application.
    pub dirty_implementations: Vec<(LogicalExpressionId, GoalId, ImplementationRule)>,

    /// Costings that were marked as dirty and need recomputation.
    pub dirty_costings: Vec<PhysicalExpressionId>,
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
        props: &LogicalProperties,
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
    ) -> MemoizeResult<MergeResult>;

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

    /// Gets all physical expression IDs in a goal (only representative IDs).
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
    ) -> MemoizeResult<bool>;

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
    ) -> MemoizeResult<bool>;

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
