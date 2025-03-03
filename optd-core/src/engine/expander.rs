//! Module for group expansion in the evaluation system.
//!
//! Provides functionality for expanding group references into concrete
//! operator implementations during expression evaluation. Group expansion
//! bridges the gap between abstract group references in the expression
//! tree and their materialized forms in the optimizer's memo structure.
//!
//! The `Expander` trait defines the core interface for performing
//! expansion operations on logical groups, scalar groups, and physical goals.

use crate::driver::{cascades::Driver, memo::Memoize};
use optd_dsl::analyzer::hir::{GroupId, PhysicalGoal, Value};
use std::{
    future::{self, Future},
    sync::Arc,
};

/// Defines operations for expanding group references into concrete expressions.
///
/// Serves as a bridge between the evaluation engine and the optimizer, allowing
/// access to materialized expressions when encountering group references during
/// evaluation.
pub(crate) trait Expander: Clone + Send + Sync + 'static {
    /// Expands a logical group into all possible logical operator expressions.
    fn expand_logical_group(
        &self,
        group_id: GroupId,
    ) -> impl std::future::Future<Output = Vec<Value>> + Send;

    /// Expands a scalar group into all possible scalar operator expressions.
    fn expand_scalar_group(&self, group_id: GroupId) -> impl Future<Output = Vec<Value>> + Send;

    /// Expands a physical goal into its optimal physical implementation.
    fn expand_physical_goal(
        &self,
        physical_goal: &PhysicalGoal,
    ) -> impl Future<Output = Value> + Send;
}

/// Implementation of the `Expander` trait for the memoized driver.
///
/// This allows the Driver<M> to act as a bridge between the memo-based optimizer
/// and the expression evaluation system. It provides mechanisms to:
/// 1. Retrieve all logical expressions from a group in the memo
/// 2. Retrieve all scalar expressions from a group in the memo
/// 3. Obtain the optimal physical implementation for a given physical goal
///
/// These methods are essential for the evaluation engine to materialize abstract
/// group references into concrete expressions during rule application and optimization.
impl<M: Memoize> Expander for Arc<Driver<M>> {
    async fn expand_logical_group(&self, _group_id: GroupId) -> Vec<Value> {
        todo!()
    }

    async fn expand_scalar_group(&self, _group_id: GroupId) -> Vec<Value> {
        todo!()
    }

    async fn expand_physical_goal(&self, _physical_goal: &PhysicalGoal) -> Value {
        todo!()
    }
}
