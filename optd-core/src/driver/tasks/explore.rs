use std::sync::Arc;

use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex,
};
use tokio::task;

use crate::{
    driver::{cascades::Driver, memo::Memoize},
    error::Error,
    ir::{expressions::LogicalExpression, group::GroupId},
};

struct ExploreGroupTask<M: Memoize> {
    group_id: GroupId,
    subscribers: Vec<Sender<Option<LogicalExpression>>>,
    memo: M,
    consumer: Arc<Mutex<Receiver<Option<LogicalExpression>>>>,
    producer: Sender<Option<LogicalExpression>>,
}

impl<M: Memoize> ExploreGroupTask<M> {
    pub fn new(group_id: GroupId, memo: M) -> Arc<Self> {
        let (producer, consumer) = mpsc::channel(100);
        let task = Arc::new(Self {
            group_id,
            subscribers: Vec::new(),
            memo,
            // TODO(sarvesh): This mutex is a hack to make the consumer thread safe. It is not actually needed
            consumer: Arc::new(Mutex::new(consumer)),
            producer,
        });

        let spawn_task = task.clone();
        task::spawn(async move {
            let task = spawn_task.clone();
            task.explore_group().await;
        });

        task
    }

    pub async fn subscribe_to_explore_group(
        self: &mut Self,
    ) -> Result<Receiver<Option<LogicalExpression>>, Error> {
        // TODO(sarvesh): get rid of this magic number to indicate how many logical expressions can we buffer at max
        let (tx, rx) = mpsc::channel(100);

        // bring the new subscriber up to date with the latest logical expressions
        // TODO(sarvesh): this is probably an expensive blocking call? However, I don't think it makes sense to make the memo table streamable.
        let logical_exprs = self.memo.get_all_logical_exprs(self.group_id).await?;
        for logical_expr in logical_exprs {
            tx.send(Some(logical_expr));
        }

        // Start streaming the latest logical expressions to the new subscriber
        self.subscribers.push(tx);

        Ok(rx)
    }

    pub async fn explore_group(self: Arc<Self>) -> Result<(), Error> {
        let logical_exprs = self.memo.get_all_logical_exprs(self.group_id).await?;
        for logical_expr in logical_exprs {
            for subscriber in self.subscribers.iter() {
                subscriber.send(Some(logical_expr.clone()));
            }
        }
        // TODO: spawn logical expression exploration tasks
        // TODO(sarvesh): The arc mutex is not technically needed here because explore_group is called only once in the new function.
        let mut consumer = self.consumer.lock().await;
        while let Some(logical_expr) = consumer.recv().await {
            for subscriber in self.subscribers.iter() {
                subscriber.send(logical_expr.clone());
            }
        }
        Ok(())
    }

    // The logical expression tasks will call this function to get a producer to send logical expressions to the explore group
    pub async fn publish_to_explore_group(self: Arc<Self>) -> Result<Sender<Option<LogicalExpression>>, Error> {
        Ok(self.producer.clone())
    }
}
