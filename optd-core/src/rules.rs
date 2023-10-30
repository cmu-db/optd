mod filter_join;
mod join_commute;
mod physical;

use crate::rel_node::{RelNodeRef, RelNodeTyp, Value};

pub use filter_join::FilterJoinRule;
pub use join_commute::{JoinAssocRule, JoinCommuteRule};
pub use physical::PhysicalConversionRule;

pub trait Rule<T: RelNodeTyp> {
    fn matches(&self, typ: T, data: Option<Value>) -> bool;
    fn apply(&self, input: RelNodeRef<T>) -> Vec<RelNodeRef<T>>;
    fn name(&self) -> &'static str;
    fn is_impl_rule(&self) -> bool {
        false
    }
}
