use crate::core::cir::*;
use std::collections::{HashMap, HashSet};

/// The status of rule application or costing operation in the task graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// There exist ongoing jobs that may generate more expressions or costs from this expression.
    Dirty,
    /// Expression is fully explored or costed with no pending jobs that could add anything new.
    Clean,
}

/// The result of merging two groups.
#[derive(Debug)]
pub struct MergeGroupResult {
    /// ID of the new representative group id.
    pub new_repr_group_id: GroupId,
    /// Groups that were merged along with their expressions.
    pub merged_groups: HashMap<GroupId, Vec<LogicalExpressionId>>,
}

/// Information about a merged goal, including its ID and expressions
#[derive(Debug)]
pub struct MergedGoalInfo {
    /// ID of the merged goal
    pub goal_id: GoalId,

    /// Whether this goal contained the best costed expression before merging.
    pub seen_best_expr_before_merge: bool,

    /// All members in this goal, which can be physical expressions or references to other goals
    pub members: Vec<GoalMemberId>,
}

/// Result of merging two goals.
#[derive(Debug)]
pub struct MergeGoalResult {
    /// Goals that were merged along with their potential best costed expression.
    pub merged_goals: HashMap<GoalId, MergedGoalInfo>,

    /// The best costed expression for all merged goals combined.
    pub best_expr: Option<(PhysicalExpressionId, Cost)>,

    /// ID of the new representative goal id.
    pub new_repr_goal_id: GoalId,
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

// impl ForwardResult {
//     pub fn new(physical_expr_id: PhysicalExpressionId, best_cost: Cost) -> Self {
//         Self {
//             physical_expr_id,
//             best_cost,
//             goals_forwarded: HashSet::new(),
//         }
//     }
// }
