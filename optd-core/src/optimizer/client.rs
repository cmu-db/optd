use futures::{
    SinkExt, StreamExt,
    channel::{mpsc, oneshot},
};

use crate::{
    cir::{LogicalPlan, PhysicalPlan},
    error::Error,
    memo::Memoize,
};

/// Unique identifier for a query instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryInstanceId(pub i64);

pub struct QueryInstance {
    /// The logical plan associated with this query instance.
    id: QueryInstanceId,
    logical_plan: LogicalPlan,
    client_tx: mpsc::Sender<ClientMessage>,
    physical_plan_rx: mpsc::Receiver<PhysicalPlan>,
}

impl QueryInstance {
    /// Gets the id of this query instance.
    pub fn id(&self) -> QueryInstanceId {
        self.id
    }

    /// Gets the logical plan for this query instance.
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }

    /// Receives the best physical plan for this query instance.
    pub async fn recv_best_plan(&mut self) -> Result<PhysicalPlan, Error> {
        let physical_plan = self
            .physical_plan_rx
            .next()
            .await
            .ok_or_else(|| Error::Placeholder)?;
        Ok(physical_plan)
    }
}

impl Drop for QueryInstance {
    fn drop(&mut self) {
        let message = ClientMessage::Complete {
            query_instance_id: self.id,
        };
        let mut client_tx = self.client_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = client_tx.send(message).await {
                eprintln!("Failed to send complete message: {}", e);
            }
        });
    }
}

pub struct Client<M: Memoize> {
    tx: mpsc::Sender<ClientMessage>,
    handle: tokio::task::JoinHandle<M>,
}

impl<M: Memoize> Client<M> {
    pub fn new(tx: mpsc::Sender<ClientMessage>, handle: tokio::task::JoinHandle<M>) -> Self {
        Self { tx, handle }
    }

    pub async fn create_query_instance(
        &mut self,
        logical_plan: LogicalPlan,
    ) -> Result<QueryInstance, Error> {
        let (id_tx, id_rx) = oneshot::channel();
        let (physical_plan_tx, physical_plan_rx) = mpsc::channel(0);
        let message = ClientMessage::Init {
            logical_plan: logical_plan.clone(),
            physical_plan_tx,
            id_tx,
        };
        self.tx.send(message).await?;
        let id = id_rx.await?;

        Ok(QueryInstance {
            id,
            logical_plan,
            physical_plan_rx,
            client_tx: self.tx.clone(),
        })
    }

    /// Shutdown the optimizer, returning the memo table.
    pub async fn shutdown(mut self) -> Result<M, Error> {
        self.tx.send(ClientMessage::Shutdown).await?;
        let memo = self.handle.await.map_err(|_| Error::Placeholder)?;
        Ok(memo)
    }
}

pub enum ClientMessage {
    /// Message to initiate optimization of a query instance.
    Init {
        /// The logical plan to be ingested.
        logical_plan: LogicalPlan,

        physical_plan_tx: mpsc::Sender<PhysicalPlan>,
        /// The channel to send the query instance ID back to the caller.
        id_tx: oneshot::Sender<QueryInstanceId>,
    },
    /// Message to indicate that the client is done with the query instance.
    Complete {
        /// The query instance ID to be completed.
        query_instance_id: QueryInstanceId,
    },
    Shutdown,
}
