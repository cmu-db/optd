use super::{JobId, OptimizerMessage};
use crate::{
    bridge::{
        from_cir::{partial_logical_to_value, partial_physical_to_value},
        into_cir::{hir_goal_to_cir, hir_group_id_to_cir},
    },
    engine::{
        generator::{Continuation, Generator},
        LogicalExprContinuation, OptimizedExprContinuation,
    },
};
use futures::{channel::mpsc::Sender, SinkExt};
use optd_dsl::analyzer::hir::{Goal, GroupId};
use std::sync::Arc;

/// Implementation of the Generator trait that connects the engine to the optimizer.
///
/// This generator provides the engine with access to memo-stored expressions and properties
/// by communicating with the optimizer through channels. It translates between the HIR
/// representation used by the engine and the CIR representation used by the optimizer.
#[derive(Clone, Debug)]
pub(super) struct OptimizerGenerator {
    /// Channel for sending messages to the optimizer
    pub message_tx: Sender<OptimizerMessage>,

    /// Current job ID for linking subscriptions to active jobs
    pub current_job_id: JobId,
}

impl OptimizerGenerator {
    /// Creates a new generator with the given message channel and job ID.
    ///
    /// # Parameters
    /// * `message_tx` - Channel for sending messages to the optimizer
    /// * `job_id` - Job ID to use for subscriptions
    ///
    /// # Returns
    /// A new generator with the specified configuration
    pub(super) fn new(message_tx: Sender<OptimizerMessage>, job_id: JobId) -> Self {
        Self {
            message_tx,
            current_job_id: job_id,
        }
    }
}

impl Generator for OptimizerGenerator {
    /// Expands a logical group and passes each expression to the continuation.
    ///
    /// This function communicates with the optimizer to retrieve all expressions
    /// in a group and invokes the provided continuation for each expression.
    ///
    /// # Parameters
    /// * `group_id` - The ID of the group to expand
    /// * `k` - The continuation to process each expression in the group
    async fn yield_group(&self, group_id: GroupId, k: Continuation) {
        // Convert HIR group ID to CIR representation
        let cir_group_id = hir_group_id_to_cir(&group_id);

        // Create a logical expression continuation that will invoke the provided continuation
        let continuation: LogicalExprContinuation = Arc::new(move |expr| {
            let k = k.clone();
            let value = partial_logical_to_value(&expr.into());

            Box::pin(async move {
                k(value).await;
            })
        });

        // Clone the message sender and create the subscription request
        let mut message_tx = self.message_tx.clone();

        // Send the subscription request to the optimizer
        message_tx
            .send(OptimizerMessage::SubscribeGroup(
                cir_group_id,
                continuation,
                self.current_job_id,
            ))
            .await
            .expect("Failed to send group subscription - channel closed");
    }

    /// Expands a physical goal and passes each implementation to the continuation.
    ///
    /// This function communicates with the optimizer to retrieve implementations
    /// for a goal and invokes the provided continuation for each implementation.
    ///
    /// # Parameters
    /// * `physical_goal` - The goal describing required properties
    /// * `k` - The continuation to process each implementation
    async fn yield_goal(&self, physical_goal: &Goal, k: Continuation) {
        // Convert HIR goal to CIR representation
        let cir_goal = hir_goal_to_cir(physical_goal);

        // Create an optimized expression continuation that will invoke the provided continuation
        let continuation: OptimizedExprContinuation = Arc::new(move |expr| {
            let k = k.clone();
            let value = partial_physical_to_value(&expr.0.into());

            Box::pin(async move {
                k(value).await;
            })
        });

        // Clone the message sender and create the subscription request
        let mut message_tx = self.message_tx.clone();

        // Send the subscription request to the optimizer
        message_tx
            .send(OptimizerMessage::SubscribeGoal(
                cir_goal,
                continuation,
                self.current_job_id,
            ))
            .await
            .expect("Failed to send goal subscription - channel closed");
    }
}
