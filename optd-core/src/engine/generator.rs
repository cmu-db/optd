use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use optd_dsl::analyzer::hir::{Goal, GroupId, Value};

/// A continuation function that processes a Value and returns a Future.
pub type Continuation = 
    Arc<dyn Fn(Value) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;

/// Defines operations for expanding references in the query plan using CPS.
///
/// This trait serves as a bridge between the evaluation engine and the optimizer,
/// providing access to materialized expressions and properties stored in the memo.
/// It uses continuation-passing style (CPS) to handle non-deterministic evaluation.
#[trait_variant::make(Send)]
pub trait Generator: Clone + Send + Sync + 'static {
    /// Returns a value directly to the caller without further processing.
    ///
    /// This is the terminal operation in CPS that delivers a final result.
    async fn emit(&self, value: Value);
    
    /// Expands a logical group and passes each expression to the continuation.
    ///
    /// Instead of returning a stream, this function invokes the provided continuation
    /// for each expression in the group.
    /// 
    /// # Parameters
    /// * `group_id` - The ID of the group to expand
    /// * `k` - The continuation to process each expression in the group
    async fn yield_group(&self, group_id: GroupId, k: Continuation);
    
    /// Expands a physical goal and passes each implementation to the continuation.
    ///
    /// Processes physical implementations that satisfy the goal, invoking the continuation
    /// for each valid implementation.
    /// 
    /// # Parameters
    /// * `physical_goal` - The goal describing required properties
    /// * `k` - The continuation to process each implementation
    async fn yield_goal(&self, physical_goal: &Goal, k: Continuation);
}