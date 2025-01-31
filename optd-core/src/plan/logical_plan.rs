//! This module contains the [`LogicalPlan`] type, which is the representation of a logical query
//! plan from SQL.
//!
//! See the documentation for [`LogicalPlan`] for more information.

use super::scalar_plan::ScalarPlan;
use crate::operator::relational::logical::LogicalOperator;
use std::sync::Arc;

/// A representation of a logical query plan DAG (directed acyclic graph).
///
/// A logical plan consists of only (materialized) logical operators and scalars.
///
/// The root of the plan DAG _cannot_ be a scalar operator (and thus for now can only be a logical
/// operator).
///
/// TODO(connor): add more docs.
#[derive(Clone)]
pub struct LogicalPlan {
    /// Represents the current logical operator that is the root of the current subplan.
    ///
    /// Note that the children of the operator are other plans, which means that this data structure
    /// is an in-memory DAG (directed acyclic graph) of logical operators.
    pub node: Arc<LogicalOperator<LogicalPlan, ScalarPlan>>,
}
