use super::OptimizerMessage;
use crate::bridge::{
    from_cir::{partial_logical_to_value, partial_physical_to_value},
    into_cir::{hir_goal_to_cir, hir_group_id_to_cir},
};
use futures::{
    SinkExt, StreamExt,
    channel::mpsc::{self, Sender},
};
use optd_dsl::{
    analyzer::hir::{Goal, GroupId, Value},
    engine::{Continuation, Generator},
};

/// Implementation of the Generator trait that connects the engine to the optimizer.
///
/// This generator provides the engine with access to memo-stored expressions and properties
/// by communicating with the optimizer through channels. It translates between the HIR
/// representation used by the engine and the CIR representation used by the optimizer.
#[derive(Clone, Debug)]
pub struct OptimizerGenerator {
    pub message_tx: Sender<OptimizerMessage>,
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
    async fn yield_group(&self, group_id: GroupId, k: Continuation<Value>) {
        // Create a channel to receive expressions from the optimizer
        let (tx, mut rx) = mpsc::channel(0);

        // Convert HIR group ID to CIR representation
        let cir_group_id = hir_group_id_to_cir(&group_id);

        // Clone the message sender and create the subscription request
        let mut message_tx = self.message_tx.clone();

        // Send the subscription request to the optimizer
        message_tx
            .send(OptimizerMessage::SubscribeGroup(cir_group_id, tx))
            .await
            .expect("Failed to send group subscription - channel closed");

        // Process each expression as it arrives
        while let Some(expr) = rx.next().await {
            // Convert the expression to the HIR value representation
            let value = partial_logical_to_value(&expr.into());

            // Invoke the continuation with the value
            k(value).await;
        }
    }

    /// Expands a physical goal and passes each implementation to the continuation.
    ///
    /// This function communicates with the optimizer to retrieve implementations
    /// for a goal and invokes the provided continuation for each implementation.
    ///
    /// # Parameters
    /// * `physical_goal` - The goal describing required properties
    /// * `k` - The continuation to process each implementation
    async fn yield_goal(&self, physical_goal: &Goal, k: Continuation<Value>) {
        // Create a channel to receive optimized expressions from the optimizer
        let (tx, mut rx) = mpsc::channel(0);

        // Convert HIR goal to CIR representation
        let cir_goal = hir_goal_to_cir(physical_goal);

        // Clone the message sender and create the subscription request
        let mut message_tx = self.message_tx.clone();

        // Send the subscription request to the optimizer
        message_tx
            .send(OptimizerMessage::SubscribeGoal(cir_goal, tx))
            .await
            .expect("Failed to send goal subscription - channel closed");

        // Process each optimized expression as it arrives
        while let Some(expr) = rx.next().await {
            // Convert the expression to the HIR value representation
            let value = partial_physical_to_value(&expr.0.into());

            // Invoke the continuation with the value
            k(value).await;
        }
    }
}
