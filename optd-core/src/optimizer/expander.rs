use super::SubscriptionRequest;
use crate::engine::{
    bridge::{
        from_cir::{partial_logical_to_value, partial_physical_to_value},
        into_cir::{hir_goal_to_cir, hir_group_id_to_cir},
    },
    utils::streams::ValueStream,
};
use futures::{
    channel::mpsc::{self, Sender},
    SinkExt,
};
use futures::{stream, StreamExt};
use optd_dsl::analyzer::hir::{Goal as HIRGoal, GroupId as HIRGroupId, Value};

/// Defines operations for expanding group references into concrete expressions.
///
/// Serves as a bridge between the evaluation engine and the optimizer, allowing
/// access to materialized expressions when encountering group references during
/// evaluation.
#[trait_variant::make(Send)]
pub trait Expander: Clone + Send + Sync + 'static {
    /// Expands a logical group into a stream of logical operator expressions.
    fn expand_all_exprs(&self, group_id: HIRGroupId) -> ValueStream;

    /// Expands a physical goal into a stream of physical implementations.
    fn expand_winning_expr(&self, physical_goal: &HIRGoal) -> ValueStream;

    /// Expands a logical group into its corresponding logical properties.
    async fn expand_properties(&self, group_id: HIRGroupId) -> Value;
}

/// Expander implementation that communicates with the optimizer via channels
#[derive(Clone)]
pub struct OptimizerExpander {
    pub subscription_sender: Sender<SubscriptionRequest>,
}

impl Expander for OptimizerExpander {
    fn expand_all_exprs(&self, group_id: HIRGroupId) -> ValueStream {
        let (expr_sender, expr_receiver) = mpsc::channel(0);
        let mut subscription_sender = self.subscription_sender.clone();

        let cir_group_id = hir_group_id_to_cir(&group_id);
        let request = SubscriptionRequest::Group(cir_group_id, expr_sender);

        let send_request = async move {
            subscription_sender
                .send(request)
                .await
                .expect("Failed to send group subscription - channel closed");

            expr_receiver
                .map(|expr| Ok(partial_logical_to_value(&expr.into())))
                .boxed()
        };

        stream::once(send_request).flatten().boxed()
    }

    fn expand_winning_expr(&self, physical_goal: &HIRGoal) -> ValueStream {
        let (expr_sender, expr_receiver) = mpsc::channel(0);
        let mut subscription_sender = self.subscription_sender.clone();

        let cir_goal = hir_goal_to_cir(physical_goal);
        let request = SubscriptionRequest::Goal(cir_goal, expr_sender);

        let send_request = async move {
            subscription_sender
                .send(request)
                .await
                .expect("Failed to send goal subscription - channel closed");

            expr_receiver
                .map(|expr| Ok(partial_physical_to_value(&expr.into())))
                .boxed()
        };

        stream::once(send_request).flatten().boxed()
    }

    async fn expand_properties(&self, _group_id: HIRGroupId) -> Value {
        todo!("will need another channel in here")
    }
}
