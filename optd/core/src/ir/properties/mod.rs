mod cardinality;
mod output_columns;
mod required;
mod tuple_ordering;

use std::sync::{Arc, OnceLock};

pub use cardinality::*;
pub use output_columns::OutputColumns;
pub use required::Required;
pub use tuple_ordering::*;

use crate::ir::context::IRContext;

#[derive(Debug, Default)]
pub struct OperatorProperties {
    pub output_columns: OnceLock<OutputColumns>,
    pub cardinality: OnceLock<Cardinality>,
}

#[derive(Debug, Default)]
pub struct ScalarProperties;

pub trait PropertyMarker {
    type Output;
}

pub trait Derive<P: PropertyMarker> {
    fn derive_by_compute(&self, ctx: &IRContext) -> P::Output;

    fn derive(&self, ctx: &IRContext) -> P::Output {
        self.derive_by_compute(ctx)
    }
}

pub trait GetProperty {
    fn get_property<P>(&self, ctx: &IRContext) -> P::Output
    where
        Self: Derive<P>,
        P: PropertyMarker,
    {
        self.derive(ctx)
    }
}

impl GetProperty for crate::ir::Operator {}

pub trait TrySatisfy<P> {
    fn try_satisfy(&self, property: &P, ctx: &IRContext) -> Option<Arc<[P]>>;
}
