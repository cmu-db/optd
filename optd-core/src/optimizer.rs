use anyhow::Result;

use crate::{
    property::PropertyBuilder,
    rel_node::{RelNodeRef, RelNodeTyp},
};

pub trait Optimizer<T: RelNodeTyp> {
    fn optimize(&mut self, root_rel: RelNodeRef<T>) -> Result<RelNodeRef<T>>;
    fn get_property<P: PropertyBuilder<T>>(&self, root_rel: RelNodeRef<T>, idx: usize) -> P::Prop;
}
