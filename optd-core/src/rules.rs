mod filter_join;
mod ir;
mod join_assoc;
mod join_commute;
mod physical;

use std::collections::HashMap;

use crate::rel_node::{RelNode, RelNodeTyp};

pub use filter_join::FilterJoinPullUpRule;
pub use ir::{OneOrMany, RuleMatcher};
pub use join_assoc::{JoinAssocLeftRule, JoinAssocRightRule};
pub use join_commute::JoinCommuteRule;
pub use physical::PhysicalConversionRule;

pub trait Rule<T: RelNodeTyp> {
    fn matcher(&self) -> &RuleMatcher<T>;
    fn apply(&self, input: HashMap<usize, OneOrMany<RelNode<T>>>) -> Vec<RelNode<T>>;
    fn name(&self) -> &'static str;
    fn is_impl_rule(&self) -> bool {
        false
    }
}
