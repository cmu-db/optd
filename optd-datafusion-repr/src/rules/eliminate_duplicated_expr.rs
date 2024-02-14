use std::collections::HashMap;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{ConstantType, LogicalEmptyRelation, OptRelNode, OptRelNodeTyp};

use super::macros::define_rule;

define_rule!(
    EliminateDuplicatedSortExprRule,
    apply_eliminate_duplicated_sort_expr,
    (Sort, child, [cond])
);

