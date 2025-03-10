use super::EngineRequest;
use crate::{
    bridge::{
        from_cir::{
            logical_properties_to_value, partial_logical_to_value, partial_physical_to_value,
        },
        into_cir::{hir_goal_to_cir, hir_group_id_to_cir},
    },
    engine::{expander::Expander, utils::streams::ValueStream},
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
    pub engine_request_tx: Sender<EngineRequest>,
}

impl Expander for OptimizerExpander {
    fn expand_all_exprs(&self, group_id: GroupId) -> ValueStream {
        let (tx, rx) = mpsc::channel(0);
        let mut engine_request_tx = self.engine_request_tx.clone();
        let cir_group_id = hir_group_id_to_cir(&group_id);
        let request = EngineRequest::SubscribeGroup(cir_group_id, tx);

        let send_request = async move {
            engine_request_tx
                .send(request)
                .await
                .expect("Failed to send group subscription - channel closed");
            rx.map(|expr| Ok(partial_logical_to_value(&expr.into())))
        };

        stream::once(send_request).flatten().boxed()
    }

    fn expand_optimized_expr(&self, physical_goal: &Goal) -> ValueStream {
        let (tx, rx) = mpsc::channel(0);
        let mut engine_request_tx = self.engine_request_tx.clone();
        let cir_goal = hir_goal_to_cir(physical_goal);
        let request = EngineRequest::SubscribeGoal(cir_goal, tx);

        let send_request = async move {
            engine_request_tx
                .send(request)
                .await
                .expect("Failed to send goal subscription - channel closed");
            rx.map(|expr| Ok(partial_physical_to_value(&expr.0.into())))
        };

        stream::once(send_request).flatten().boxed()
    }

    async fn expand_properties(&self, group_id: GroupId) -> Value {
        let (tx, rx) = oneshot::channel();
        let mut engine_request_tx = self.engine_request_tx.clone();
        let cir_group_id = hir_group_id_to_cir(&group_id);
        let request = EngineRequest::DeriveProperties(cir_group_id, tx);

        engine_request_tx
            .send(request)
            .await
            .expect("Failed to send properties request - channel closed");
        let properties = rx
            .await
            .expect("Failed to receive properties - sender dropped");

        logical_properties_to_value(&properties)
    }
}
