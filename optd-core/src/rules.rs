mod ir;

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::{
    optimizer::Optimizer,
    rel_node::{RelNode, RelNodeTyp},
};

pub use ir::RuleMatcher;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OptimizeType {
    Cascades,
    Heuristics,
}

impl Display for OptimizeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cascades => write!(f, "cascades"),
            Self::Heuristics => write!(f, "heuristics"),
        }
    }
}

pub struct RuleWrapper<T: RelNodeTyp, O: Optimizer<T>> {
    pub rule: Arc<dyn Rule<T, O>>,
    pub optimize_type: OptimizeType,
}

impl<T: RelNodeTyp, O: Optimizer<T>> RuleWrapper<T, O> {
    pub fn new(rule: Arc<dyn Rule<T, O>>, optimizer_type: OptimizeType) -> Self {
        Self {
            rule,
            optimize_type: optimizer_type,
        }
    }
    pub fn new_cascades(rule: Arc<dyn Rule<T, O>>) -> Arc<Self> {
        Arc::new(Self {
            rule,
            optimize_type: OptimizeType::Cascades,
        })
    }
    pub fn new_heuristic(rule: Arc<dyn Rule<T, O>>) -> Arc<Self> {
        Arc::new(Self {
            rule,
            optimize_type: OptimizeType::Heuristics,
        })
    }
    pub fn rule(&self) -> Arc<dyn Rule<T, O>> {
        self.rule.clone()
    }
    pub fn optimize_type(&self) -> OptimizeType {
        self.optimize_type
    }
}

// TODO: docs, possible renames.
// TODO: Why do we have all of these match types? Seems like possible overkill.
pub trait Rule<T: RelNodeTyp, O: Optimizer<T>>: 'static + Send + Sync {
    fn matcher(&self) -> &RuleMatcher<T>;
    fn apply(&self, optimizer: &O, input: HashMap<usize, RelNode<T>>) -> Vec<RelNode<T>>;
    fn name(&self) -> &'static str;
    fn is_impl_rule(&self) -> bool {
        false
    }
}
