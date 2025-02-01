//! TODO(everyone): Figure out what exactly a `ScalarPlan` is (tree? DAG? always materialized?)

use crate::operator::scalar::ScalarOperator;
use std::sync::Arc;

/// A representation of a scalar query plan DAG (directed acyclic graph).
#[derive(Clone)]
pub struct ScalarPlan {
    /// Represents the current scalar operator that is the root of the current scalar subtree.
    ///
    /// TODO(connor): Figure out if scalar plans can be a DAG
    pub node: Arc<ScalarOperator<ScalarPlan>>,
}
