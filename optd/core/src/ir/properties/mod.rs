//! This module defines the properties of IR nodes, including operators and
//! scalars. It provides traits and implementations for deriving and
//! retrieving properties such as output schema, cardinality, required
//! properties, and output columns.
//!
//! The structure of an operator (such as LogicalJoin) is
//! {
//!     Common:
//!         InputOperators: [ ... ]
//!         InputScalars: [ ... ]
//!         Properties: { Cached properties for quick access }
//!     Metadata: operator specific information
//! }
//!
//! The saved properties are different for operators and scalars. Not all
//! properties need to be saved / cached into the node. Some properties can be
//! derived on the fly using the Derive trait.

mod cardinality;
mod output_columns;
mod output_schema;
mod predicate_summary;
mod required;
mod tuple_ordering;

use std::sync::{Arc, OnceLock};

pub use cardinality::*;
pub use output_columns::OutputColumns;
pub use predicate_summary::{
    GroupPredicate, PredicateSummary, RangeConstraint, ValueRef, date_days_to_year,
    date_millis_to_year, derive_value_ref,
};
pub use required::Required;
pub use tuple_ordering::*;

use crate::ir::{ColumnSet, context::IRContext};

/// Common properties structure for IR operators.
///
/// Every field is a `OnceLock` so the property is computed at most once per
/// operator instance. The `predicate_summary` slot is also what carries
/// lineage + predicate state across memo `Group` placeholders — see
/// `docs/predicate-summary-lineage.md.memo`.
#[derive(Debug, Default)]
pub struct OperatorProperties {
    pub output_columns: OnceLock<Arc<ColumnSet>>,
    pub cardinality: OnceLock<Cardinality>,
    /// Cached lineage + aggregated predicate state (see
    /// [`predicate_summary::PredicateSummary`]). Populated lazily on first
    /// derive; also warmed eagerly at memo-insert time so `Group`
    /// placeholders have a usable summary.
    pub predicate_summary: OnceLock<Arc<PredicateSummary>>,
}

/// Common properties structure for IR scalars.
#[derive(Debug, Default)]
pub struct ScalarProperties;

/// Marker trait for properties.
pub trait PropertyMarker {
    type Output;
}

/// Deriving properties allows computing properties based on the operator and
/// context. Each property type should implement this trait for operators to
/// allow properties to be derived while in the operator form.
pub trait Derive<P: PropertyMarker> {
    /// Implement this method to compute the property and return the result
    fn derive_by_compute(&self, ctx: &IRContext) -> P::Output;

    /// Implement this method to cache and retrieve the property if it can be
    /// cached, else call derive_by_compute.
    fn derive(&self, ctx: &IRContext) -> P::Output {
        self.derive_by_compute(ctx)
    }
}

/// Trait for retrieving properties from operators using the derive mechanism.
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

/// Trait for checking if an operator can satisfy a given property.
pub trait TrySatisfy<P> {
    fn try_satisfy(&self, property: &P, ctx: &IRContext) -> crate::error::Result<Option<Arc<[P]>>>;
}
