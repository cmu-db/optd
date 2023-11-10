mod ir;

use std::collections::HashMap;

use crate::rel_node::{RelNode, RelNodeTyp};

pub use ir::{OneOrMany, RuleMatcher};

pub trait Rule<T: RelNodeTyp> {
    fn matcher(&self) -> &RuleMatcher<T>;
    fn apply(&self, input: HashMap<usize, OneOrMany<RelNode<T>>>) -> Vec<RelNode<T>>;
    fn name(&self) -> &'static str;
    fn is_impl_rule(&self) -> bool {
        false
    }
}
