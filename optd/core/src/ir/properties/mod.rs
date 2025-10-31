mod cardinality;
mod eq_columns;
mod output_columns;
mod output_schema;
mod required;
mod restriction;
mod tuple_ordering;

use std::sync::{Arc, OnceLock};

pub use cardinality::*;
pub use output_columns::OutputColumns;
pub use required::Required;
pub use restriction::ColumnRestrictions;
pub use tuple_ordering::*;

use crate::ir::{ColumnSet, context::IRContext};

#[derive(Debug, Default)]
pub struct OperatorProperties {
    pub output_columns: OnceLock<Arc<ColumnSet>>,
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
