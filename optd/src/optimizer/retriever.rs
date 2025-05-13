use super::OptimizerMessage;
use crate::dsl::analyzer::hir::{GroupId, Value};
use crate::dsl::utils::retriever::Retriever;
use crate::optimizer::hir_cir::from_cir::logical_properties_to_value;
use crate::optimizer::hir_cir::into_cir::hir_group_id_to_cir;
use async_trait::async_trait;
use tokio::sync::mpsc::{self, Sender};

/// Implementation of the Retriever trait that communicates with the optimizer
/// through a channel to retrieve properties for expression groups.
pub struct OptimizerRetriever {
    /// Sender for communicating with the optimizer.
    message_tx: Sender<OptimizerMessage>,
}

impl OptimizerRetriever {
    /// Creates a new OptimizerRetriever instance.
    pub fn new(message_tx: Sender<OptimizerMessage>) -> Self {
        Self { message_tx }
    }
}

#[async_trait]
impl Retriever for OptimizerRetriever {
    /// Asynchronously retrieves properties for a given expression group by sending
    /// a retrieval message to the optimizer and awaiting the response.
    ///
    /// # Parameters
    /// * `group_id` - The identifier of the expression group whose properties are being retrieved
    ///
    /// # Returns
    /// A `Value` containing the logical properties associated with the specified group
    async fn get_properties(&self, group_id: GroupId) -> Value {
        use OptimizerMessage::*;

        let (response_tx, mut response_rx) = mpsc::channel(0);

        self.message_tx
            .send(Retrieve(hir_group_id_to_cir(&group_id), response_tx))
            .await
            .expect("Failed to send properties retrieval request - channel closed");
        let properties = response_rx
            .recv()
            .await
            .expect("Failed to receive logical properties - channel closed");

        logical_properties_to_value(&properties)
    }
}
