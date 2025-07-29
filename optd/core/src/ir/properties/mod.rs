mod cardinality;
mod output_columns;
pub mod required;
mod tuple_ordering;

use std::sync::{Arc, OnceLock};

pub use cardinality::*;
pub use output_columns::OutputColumns;
pub use tuple_ordering::*;

use crate::ir::context::IRContext;

#[derive(Debug, Default)]
pub struct OperatorProperties {
    pub output_columns: OnceLock<OutputColumns>,
    pub cardinality: OnceLock<Cardinality>,
}

#[derive(Debug, Default)]
pub struct ScalarProperties;

pub trait PropertyMarker {}

pub trait Derive<P: PropertyMarker> {
    fn derive_by_compute(&self, ctx: &IRContext) -> P;

    fn derive(&self, ctx: &IRContext) -> P {
        self.derive_by_compute(ctx)
    }
}

pub trait GetProperty {
    fn get_property<P>(&self, ctx: &IRContext) -> P
    where
        Self: Derive<P>,
        P: PropertyMarker,
    {
        self.derive(ctx)
    }
}

pub trait TrySatisfy<P: PropertyMarker> {
    fn try_satisfy(&self, property: &P, ctx: &IRContext) -> Option<Arc<[P]>>;
}
