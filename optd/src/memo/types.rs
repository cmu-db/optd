use crate::cir::*;
use std::collections::HashSet;

/// The result of merging two groups.
///
/// TODO(sarvesh): More detailed docs.
#[derive(Debug)]
pub struct GroupMergeProduct {
    /// ID of the new representative group id.
    pub repr_group_id: GroupId,

    /// TODO(sarvesh): More detailed docs.
    pub non_repr_group_id: GroupId,

    /// All expressions in the merged group.
    ///
    /// A key assumption here is that all expressions here are representative expressions (i.e. the
    /// children group IDs of the expressions are all representative group IDs).
    pub all_exprs_in_merged_group: HashSet<LogicalExpressionId>,

    /// The expressions that were in the old non-representative group.
    ///
    /// A key assumption here is that all expressions here are representative expressions (i.e. the
    /// children group IDs of the expressions are all representative group IDs).
    pub non_repr_group_exprs: HashSet<LogicalExpressionId>,

    /// The expressions that are in the representative group.
    ///
    /// A key assumption here is that all expressions here are representative expressions (i.e. the
    /// children group IDs of the expressions are all representative group IDs).
    pub repr_group_exprs: HashSet<LogicalExpressionId>,
}

impl GroupMergeProduct {
    /// Creates a new MergeGroupResult instance.
    ///
    /// # Parameters
    /// * `merged_groups` - Groups that were merged along with their expressions.
    /// * `new_repr_group_id` - ID of the new representative group id.
    pub fn new(repr_group_id: GroupId, non_repr_group_id: GroupId) -> Self {
        Self {
            repr_group_id,
            non_repr_group_id,
            all_exprs_in_merged_group: HashSet::new(),
            non_repr_group_exprs: HashSet::new(),
            repr_group_exprs: HashSet::new(),
        }
    }
}

/// The result of merging two goals.
#[derive(Debug)]
pub struct GoalMergeProduct {
    /// The best costed expression for all merged goals combined.
    pub best_expr: Option<(PhysicalExpressionId, Cost)>,

    /// The ID of the new representative goal.
    pub repr_goal_id: GoalId,

    /// The ID of the old non-representative goal.
    pub non_repr_goal_id: GoalId,

    /// Whether the representative goal contained the best costed expression before merging.
    pub repr_goal_seen_best_expr_before_merge: bool,

    /// Whether the non-representative goal contained the best costed expression before merging.
    pub non_repr_goal_seen_best_expr_before_merge: bool,

    /// All members in the merged goal.
    ///
    /// Unlike for group merge results, these may not necessarily be representative IDs.
    ///
    /// The reasoning for this is that there is an edge case where the merging of two groups results
    /// in 2 pairs of goals being merged. TODO(sarvesh): Be more specific, or give an example.
    ///
    /// However, if one of the goals is a member of the other goals, then we cannot guarantee the
    /// order in which the merge will happen.
    ///
    /// Hence, we cannot guarantee that the members are representative IDs.
    pub members: HashSet<GoalMemberId>,
}

/// The result of merging two cost expressions.
#[derive(Debug)]
pub struct PhysicalMergeProduct {
    /// The new representative physical expression ID.
    pub repr_physical_expr: PhysicalExpressionId,

    /// The non-representative physical expression id.
    pub non_repr_physical_exprs: PhysicalExpressionId,
}

/// The result of merge operations with newly dirtied expressions.
///
/// TODO(sarvesh): Why do we not have products for logical expression merges?
#[derive(Debug, Default)]
pub struct MergeProducts {
    /// Group merge results.
    pub group_merges: Vec<GroupMergeProduct>,

    /// Goal merge results.
    pub goal_merges: Vec<GoalMergeProduct>,

    /// Physical expression merge results.
    pub physical_expr_merges: Vec<PhysicalMergeProduct>,
}
