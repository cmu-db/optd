use super::{
    egest::egest_best_plan, ingest::LogicalIngest, memo::Memoize, OptimizeRequest, Optimizer,
    OptimizerMessage, PendingMessage,
};
use crate::cir::{
    expressions::{LogicalExpression, OptimizedExpression},
    goal::Goal,
    group::GroupId,
    plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan, PhysicalPlan},
    properties::{LogicalProperties, PhysicalProperties},
};
use futures::{
    channel::{
        mpsc::{self, Sender},
        oneshot,
    },
    SinkExt, StreamExt,
};
use OptimizerMessage::*;

impl<M: Memoize> Optimizer<M> {
    /// This method initiates the optimization process for a logical plan and streams
    /// results back to the client as they become available.
    pub(super) async fn process_optimize_request(
        &mut self,
        logical_plan: LogicalPlan,
        response_tx: Sender<PhysicalPlan>,
    ) {
        match self.try_ingest_logical(logical_plan.clone().into()).await {
            LogicalIngest::Success(group_id, _) => {
                // Plan was ingested successfully, subscribe to the goal
                let goal = Goal(group_id, PhysicalProperties(None));
                let (expr_tx, mut expr_rx) = mpsc::channel(0);
                self.subscribe_to_goal(goal, expr_tx).await;

                let mut message_tx = self.message_tx.clone();

                tokio::spawn(async move {
                    // Forward optimized expressions to the client
                    while let Some(expr) = expr_rx.next().await {
                        message_tx
                            .send(EgestOptimized(expr, response_tx.clone()))
                            .await
                            .expect("Failed to send optimized expression");
                    }
                });
            }
            LogicalIngest::NeedsDependencies(dependencies) => {
                // Store the request as a pending message that will be processed
                // once all dependencies are resolved
                let pending_message = PendingMessage {
                    message: OptimizeRequest(OptimizeRequest {
                        logical_plan,
                        response_tx,
                    }),
                    pending_dependencies: dependencies,
                };

                self.pending_messages.push(pending_message);
            }
        }
    }

    /// This method handles converting an optimized expression to a complete physical plan
    /// and sending it to the client through the provided channel.
    pub(super) async fn process_egest_optimized(
        &self,
        expr: OptimizedExpression,
        mut response_tx: Sender<PhysicalPlan>,
    ) {
        let plan = egest_best_plan(&self.memo, &expr)
            .await
            .expect("Failed to egest plan from memo")
            .expect("Expression has not been recursively optimized");

        tokio::spawn(async move {
            response_tx
                .send(plan)
                .await
                .expect("Failed to send physical plan");
        });
    }

    /// This method handles new logical plan alternatives discovered through
    /// transformation rule application.
    pub(super) async fn process_new_logical_partial(
        &mut self,
        plan: PartialLogicalPlan,
        group_id: GroupId,
    ) {
        match self.try_ingest_logical(plan.clone()).await {
            LogicalIngest::Success(new_group_id, is_new_expr) => {
               if new_group_id != group_id {
                    self.memo
                        .merge_groups(group_id, new_group_id)
                        .await
                        .expect("Failed to merge groups");
                }

                if is_new_expr {
                    // send to subscribers

                }
            }
            LogicalIngest::NeedsDependencies(dependencies) => {
                // Store the request as a pending message that will be processed
                // once all dependencies are resolved
                let pending_message = PendingMessage {
                    message: NewLogicalPartial(plan, group_id),
                    pending_dependencies: dependencies,
                };

                self.pending_messages.push(pending_message);
            }
        }
    }

    /// This method handles new physical implementations discovered through
    /// implementation rule application.
    pub(super) async fn process_new_physical_partial(
        &mut self,
        plan: PartialPhysicalPlan,
        goal: Goal,
    ) {
        todo!()
    }

    /// Process a new optimized expression
    ///
    /// This method handles fully optimized physical expressions with cost information.
    pub(super) async fn process_new_optimized_expression(
        &mut self,
        expr: OptimizedExpression,
        goal: Goal,
    ) {
        todo!()
    }

    /// This method handles group creation for expressions with derived properties
    /// and updates any pending messages that depend on this group.
    pub(super) async fn process_create_group(
        &mut self,
        properties: LogicalProperties,
        expression: LogicalExpression,
        job_id: i64,
    ) {
        self.memo
            .create_group_with(&expression, &properties)
            .await
            .expect("Failed to create group");

        self.resolve_dependencies(job_id).await;
    }

    /// Sends existing logical expressions for the group to the subscriber
    /// and initiates exploration of the group if it hasn't been explored yet.
    pub(super) async fn process_group_subscription(
        &mut self,
        group_id: GroupId,
        sender: Sender<LogicalExpression>,
    ) {
        self.subscribe_to_group(group_id, sender).await;
    }

    /// Sends the best existing physical expression for the goal to the subscriber
    /// and initiates implementation of the goal if it hasn't been launched yet.
    pub(super) async fn process_goal_subscription(
        &mut self,
        goal: Goal,
        sender: Sender<OptimizedExpression>,
    ) {
        self.subscribe_to_goal(goal, sender).await;
    }

    /// Retrieves the logical properties for the given group from the memo
    /// and sends them back to the requestor through the provided oneshot channel.
    pub(super) async fn process_retrieve_properties(
        &self,
        group_id: GroupId,
        sender: oneshot::Sender<LogicalProperties>,
    ) {
        let props = self
            .memo
            .get_logical_properties(group_id)
            .await
            .expect("Failed to get logical properties");

        tokio::spawn(async move {
            sender
                .send(props)
                .expect("Failed to send properties - channel closed");
        });
    }

    /// This method is called when a group creation job completes. It updates all
    /// pending messages that were waiting for this job and processes any that
    /// are now ready (have no more pending dependencies).
    async fn resolve_dependencies(&mut self, completed_job_id: i64) {
        // Update dependencies and collect ready messages
        let ready_indices: Vec<_> = self
            .pending_messages
            .iter_mut()
            .enumerate()
            .filter_map(|(i, pending)| {
                pending.pending_dependencies.remove(&completed_job_id);
                pending.pending_dependencies.is_empty().then_some(i)
            })
            .collect();

        // Process all ready messages (in reverse order to avoid index issues when removing)
        for i in ready_indices.iter().rev() {
            // Take ownership of the message
            let pending = self.pending_messages.swap_remove(*i);

            // Re-send the message to be processed
            let mut message_tx = self.message_tx.clone();
            tokio::spawn(async move {
                message_tx
                    .send(pending.message)
                    .await
                    .expect("Failed to re-send ready message");
            });
        }
    }
}
