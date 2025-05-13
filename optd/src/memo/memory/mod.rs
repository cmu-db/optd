//! The definition and implementation of the in-memory memo table.

use super::{Memo, MemoBase, MergeProducts, Representative};
use crate::cir::*;
use hashbrown::{HashMap, HashSet};

mod helpers;
mod implementation;
mod materialize;
mod representative;
mod union_find;

use union_find::UnionFind;

/// The never type. Used for ensuring that the in-memory memo table never raises an error.
#[derive(Debug)]
pub enum Infallible {}

/// This defines the error type for the in-memory implementation of the memo table.
impl MemoBase for MemoryMemo {
    /// In-memory operations cannot fail.
    type MemoError = Infallible;
}

/// An in-memory implementation of the memo table.
#[derive(Default)]
pub struct MemoryMemo {
    // Groups.
    /// Key is always a representative ID.
    group_info: HashMap<GroupId, GroupInfo>,

    // Goals.
    /// Key is always a representative ID.
    id_to_goal: HashMap<GoalId, Goal>,
    /// Each representative goal is mapped to its id, for faster lookups.
    goal_to_id: HashMap<Goal, GoalId>,

    // Expressions.
    /// Key is always a representative ID.
    id_to_logical_expr: HashMap<LogicalExpressionId, LogicalExpression>,
    /// Each representantive expression is mapped to its id, for faster lookups.
    logical_expr_to_id: HashMap<LogicalExpression, LogicalExpressionId>,

    // Indexes: only deal with representative IDs, but speeds up most queries.
    /// To speed up expr->group lookup, we maintain a mapping from logical expression IDs to group IDs.
    logical_id_to_group_index: HashMap<LogicalExpressionId, GroupId>,
    /// To speed up recursive merges, we maintain a mapping from group IDs to all logical expression IDs
    /// that contain a reference to this group. The value logical_expr_ids may *NOT* be a representative ID.
    group_referencing_exprs_index: HashMap<GroupId, HashSet<LogicalExpressionId>>,

    /// The shared next unique id to be used for goals, groups, logical expressions, and physical expressions.
    next_shared_id: i64,

    /// Representatives of groups, goals, and expression ids, so that we can process old IDs.
    repr_group_id: UnionFind<GroupId>,
    repr_goal_id: UnionFind<GoalId>,
    repr_logical_expr_id: UnionFind<LogicalExpressionId>,
    repr_physical_expr_id: UnionFind<PhysicalExpressionId>,
}

/// Information about a group:
/// - All logical expressions in this group (always representative IDs).
/// - Logical properties of this group.
#[derive(Clone, Debug)]
struct GroupInfo {
    expressions: HashSet<LogicalExpressionId>,
    logical_properties: LogicalProperties,
}
