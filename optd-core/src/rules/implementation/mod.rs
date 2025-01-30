//! This module contains the implementation rule trait / API, as well as the rules that implement
//! said trait.
//!
//! TODO(connor): Add more docs.

use crate::expression::{LogicalExpression, PhysicalExpression};

/// The interface for implementation rules, which help convert logical plans into physical
/// (executable) query plans.
#[trait_variant::make(Send)]
#[allow(dead_code)]
pub trait ImplementationRule {
    /// Checks if the given expression matches the given rule and applies the implementation.
    ///
    /// If the input expression does not match the current rule's pattern, then this method returns
    /// `None`. Otherwise, it applies the implementation and returns the corresponding
    /// [`PhysicalExpression`].
    ///
    /// Implementation rules are defined to be a mapping from a logical operator to a physical
    /// operator. This method is used by the rule engine to check if an implementation / algorithm
    /// can be applied to a logical expression in the memo table. If so, it emits a new physical
    /// expression, representing a executor in a query execution engine.
    ///
    /// One of the main differences between `ImplementationRule` and [`TransformationRule`] is that
    /// transformation rules can create several new partially materialized logical plans, which
    /// necessitates creating a partial logical plan binding (encoded as [`PartialLogicalPlan`])
    /// before actually applying the transformation. Additionally, the transformation rules have to
    /// materialize child expressions of the input expression in order to match several layers of
    /// operators in a logical plan, which implementation rules do not need.
    ///
    /// [`TransformationRule`]: crate::rules::transformation::TransformationRule
    /// [`PartialLogicalPlan`]: crate::plan::partial_logical_plan::PartialLogicalPlan
    fn check_and_apply(&self, expr: LogicalExpression) -> Option<PhysicalExpression>;
}

pub mod hash_join;
pub mod physical_filter;
pub mod table_scan;
