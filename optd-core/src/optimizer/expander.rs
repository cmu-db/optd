use super::OptimizerMessage;
use crate::{
    bridge::{
        from_cir::{
            logical_properties_to_value, partial_logical_to_value, partial_physical_to_value,
        },
        into_cir::{hir_goal_to_cir, hir_group_id_to_cir},
    },
    engine::{
        generator::{Continuation, Generator},
        utils::streams::ValueStream,
    },
};
use futures::{
    channel::{
        mpsc::{self, Sender},
        oneshot,
    },
    stream, SinkExt, StreamExt,
};
use optd_dsl::analyzer::hir::{Goal, GroupId, Value};

/// Implementation of the Expander trait that connects the engine to the optimizer.
///
/// This expander provides the engine with access to memo-stored expressions and properties
/// by communicating with the optimizer through channels. It translates between the HIR
/// representation used by the engine and the CIR representation used by the optimizer.
#[derive(Clone)]
pub struct OptimizerExpander {
    pub message_tx: Sender<OptimizerMessage>,
}

impl Generator for OptimizerExpander {
    /// Expands a logical group and passes each expression to the continuation.
    ///
    /// Instead of returning a stream, this function invokes the provided continuation
    /// for each expression in the group, enabling more efficient processing of
    /// multiple evaluation paths.
    ///
    /// # Parameters
    /// * `group_id` - The ID of the group to expand
    /// * `k` - The continuation to process each expression in the group
    async fn yield_group(&self, group_id: GroupId, k: Continuation) {}

    /// Expands a physical goal and passes each implementation to the continuation.
    ///
    /// Processes physical implementations that satisfy the goal, invoking the continuation
    /// for each valid implementation, typically in order of increasing cost.
    ///
    /// # Parameters
    /// * `physical_goal` - The goal describing required properties
    /// * `k` - The continuation to process each implementation
    async fn yield_goal(&self, physical_goal: &Goal, k: Continuation) {}

    /*fn expand_all_exprs(&self, group_id: GroupId) -> ValueStream {
        let (tx, rx) = mpsc::channel(0);
        let mut message_tx = self.message_tx.clone();
        let cir_group_id = hir_group_id_to_cir(&group_id);
        let request = OptimizerMessage::SubscribeGroup(cir_group_id, tx);

        let send_request = async move {
            message_tx
                .send(request)
                .await
                .expect("Failed to send group subscription - channel closed");
            rx.map(|expr| Ok(partial_logical_to_value(&expr.into())))
        };

        stream::once(send_request).flatten().boxed()
    }

    fn expand_optimized_expr(&self, physical_goal: &Goal) -> ValueStream {
        let (tx, rx) = mpsc::channel(0);
        let mut message_tx = self.message_tx.clone();
        let cir_goal = hir_goal_to_cir(physical_goal);
        let request = OptimizerMessage::SubscribeGoal(cir_goal, tx);

        let send_request = async move {
            message_tx
                .send(request)
                .await
                .expect("Failed to send goal subscription - channel closed");
            rx.map(|expr| Ok(partial_physical_to_value(&expr.0.into())))
        };

        stream::once(send_request).flatten().boxed()
    }*/
}
