//! This module contains the [`PhysicalPlan`] type, which is the representation of a physical
//! execution plan that can be sent to a query execution engine.
//!
//! See the documentation for [`PhysicalPlan`] for more information.

use super::scalar_plan::ScalarPlan;
use crate::operator::relational::physical::PhysicalOperator;
use std::sync::Arc;

/// A representation of a physical query plan DAG (directed acyclic graph).
///
/// A physical plan consists of only physical operators and scalars.
///
/// The root of the plan DAG _cannot_ be a scalar operator (and thus for now can only be a physical
/// operator).
///
/// TODO(connor): add more docs.
#[derive(Clone)]
pub struct PhysicalPlan {
    /// Represents the current physical operator that is the root of the current subplan.
    ///
    /// Note that the children of the operator are other plans, which means that this data structure
    /// is an in-memory DAG (directed acyclic graph) of physical operators.
    pub node: Arc<PhysicalOperator<PhysicalPlan, ScalarPlan>>,
}
