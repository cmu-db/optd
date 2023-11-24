mod ir;

use std::collections::HashMap;

use crate::{
    optimizer::Optimizer,
    rel_node::{RelNode, RelNodeTyp},
};

pub use ir::RuleMatcher;

pub trait Rule<T: RelNodeTyp, O: Optimizer<T>>: 'static + Send + Sync {
    fn matcher(&self) -> &RuleMatcher<T>;
    fn apply(&self, optimizer: &O, input: HashMap<usize, RelNode<T>>) -> Vec<RelNode<T>>;
    fn name(&self) -> &'static str;
    fn is_impl_rule(&self) -> bool {
        false
    }
}
