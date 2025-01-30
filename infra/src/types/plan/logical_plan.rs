//! This module contains the [`LogicalPlan`] type, which is the representation of a logical query
//! plan from SQL.

use super::scalar_plan::ScalarPlan;
use crate::types::operator::relational::logical::LogicalOperator;
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
    pub node: Arc<LogicalOperator<LogicalPlan, ScalarPlan>>,
}
