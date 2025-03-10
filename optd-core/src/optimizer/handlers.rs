use super::{
    egest::egest_best_plan, ingest::IngestionResult, memo::Memoize, OptimizeRequest, Optimizer,
    OptimizerMessage, PendingMessage,
};
use crate::{
    capture,
    cir::{
        expressions::{LogicalExpression, OptimizedExpression},
        goal::Goal,
        group::GroupId,
        plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan, PhysicalPlan},
        properties::{LogicalProperties, PhysicalProperties},
    },
};
use futures::{
    channel::{
        mpsc::{self, Sender},
        oneshot,
    },
    SinkExt, Stream, StreamExt,
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
            IngestionResult::Success(group_id, _) => {
                // Plan was ingested successfully, subscribe to the goal
                let goal = Goal(group_id, PhysicalProperties(None));
                let mut expr_stream = self.subscribe_to_goal(goal);

                let mut message_tx = self.message_tx.clone();

                tokio::spawn(async move {
                    // Forward optimized expressions to the client
                    while let Some(expr) = expr_stream.next().await {
                        message_tx
                            .send(EgestOptimized(expr, response_tx.clone()))
                            .await
                            .expect("Failed to send optimized expression");
                    }
                });
            }
            IngestionResult::NeedsDependencies(dependencies) => {
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
        todo!()
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
        mut sender: Sender<LogicalExpression>,
    ) {
        todo!();
    }

    /// Sends the best existing physical expression for the goal to the subscriber
    /// and initiates implementation of the goal if it hasn't been launched yet.
    pub(super) async fn process_goal_subscription(
        &mut self,
        goal: Goal,
        mut sender: Sender<OptimizedExpression>,
    ) {
        todo!();
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

    // HELPERS

    /// Explores a logical group by applying transformation rules
    ///
    /// This method triggers the application of transformation rules to
    /// the logical expressions in a group, generating new equivalent
    /// logical expressions.
    fn explore_group(&mut self, group_id: GroupId) {
        // Mark the group as exploring
        self.exploring_groups.insert(group_id);

        // Set up exploration of the group
        let (expr_tx, mut expr_rx) = mpsc::channel(0);

        let transformations = self.rule_book.get_transformations().to_vec();

        let mut message_tx = self.message_tx.clone();
        let engine = self.engine.clone();

        // Spawn a task to explore the group
        tokio::spawn(async move {
            // Subscribe to the group
            message_tx
                .send(SubscribeGroup(group_id, expr_tx))
                .await
                .expect("Failed to send group subscription request");

            while let Some(expr) = expr_rx.next().await {
                let plan: PartialLogicalPlan = expr.into();

                for rule in &transformations {
                    let rule_name = rule.0.clone();

                    tokio::spawn(capture!([engine, plan, message_tx], async move {
                        engine
                            .match_and_apply_logical_rule(&rule_name, &plan)
                            .inspect(|result| {
                                if let Err(err) = result {
                                    eprintln!("Error applying rule {}: {:?}", rule_name, err);
                                }
                            })
                            .filter_map(|result| async move { result.ok() })
                            .for_each(|transformed_plan| {
                                let mut result_tx = message_tx.clone();
                                async move {
                                    if let Err(err) = result_tx
                                        .send(NewLogicalPartial(transformed_plan, group_id))
                                        .await
                                    {
                                        eprintln!("Failed to send transformation result: {}", err);
                                    }
                                }
                            })
                            .await;
                    }));
                }
            }
        });
    }

    /// Explores a goal by applying implementation rules
    ///
    /// This method triggers the application of implementation rules to
    /// logical expressions in the goal's group, generating physical
    /// implementations for the goal.
    fn explore_goal(&mut self, goal: Goal) {
        self.exploring_goals.insert(goal.clone());

        // Set up implementation of the goal
        let (expr_tx, mut expr_rx) = mpsc::channel(0);

        let implementations = self.rule_book.get_implementations().to_vec();

        let props = goal.1.clone();
        let mut message_tx = self.message_tx.clone();
        let engine = self.engine.clone();

        // Spawn a task to implement the goal
        tokio::spawn(async move {
            // Subscribe to the group
            message_tx
                .send(SubscribeGroup(goal.0, expr_tx))
                .await
                .expect("Failed to send group subscription request for goal");

            while let Some(expr) = expr_rx.next().await {
                let plan: PartialLogicalPlan = expr.into();

                for rule in &implementations {
                    let rule_name = rule.0.clone();

                    tokio::spawn(capture!(
                        [engine, plan, message_tx, goal, props],
                        async move {
                            engine
                                .match_and_apply_implementation_rule(&rule_name, &plan, &props)
                                .inspect(|result| {
                                    if let Err(err) = result {
                                        eprintln!(
                                            "Error applying implementation rule {}: {:?}",
                                            rule_name, err
                                        );
                                    }
                                })
                                .filter_map(|result| async move { result.ok() })
                                .for_each(|physical_plan| {
                                    let mut result_tx = message_tx.clone();
                                    let goal = goal.clone();
                                    async move {
                                        if let Err(err) = result_tx
                                            .send(NewPhysicalPartial(physical_plan, goal))
                                            .await
                                        {
                                            eprintln!(
                                                "Failed to send implementation result: {}",
                                                err
                                            );
                                        }
                                    }
                                })
                                .await;
                        }
                    ));
                }
            }
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

    /// Subscribe to optimized expressions for a specific goal.
    /// If group hasn't started to explore, launch exploration.
    fn subscribe_to_goal(&mut self, goal: Goal) -> impl Stream<Item = OptimizedExpression> {
        let (expr_tx, expr_rx) = mpsc::channel(0);

        // Add the sender to goal subscribers list
        self.goal_subscribers
            .entry(goal.clone())
            .or_default()
            .push(expr_tx.clone());

        // Launch exploration if this goal isn't being explored yet
        if !self.exploring_goals.contains(&goal) {
            self.explore_goal(goal.clone());
        }

        // Send the subscription request to the engine
        let mut message_tx = self.message_tx.clone();
        let goal_clone = goal.clone();
        let expr_tx_clone = expr_tx.clone();

        tokio::spawn(async move {
            message_tx
                .send(SubscribeGoal(goal_clone, expr_tx_clone))
                .await
                .expect("Failed to send goal subscription - channel closed");
        });

        expr_rx
    }

    /// Subscribe to logical expressions in a specific group.
    /// If group hasn't started to explore, launch exploration.
    fn subscribe_to_group(&mut self, group_id: GroupId) -> impl Stream<Item = LogicalExpression> {
        let (expr_tx, expr_rx) = mpsc::channel(0);

        // Add the sender to group subscribers list
        self.group_subscribers
            .entry(group_id)
            .or_default()
            .push(expr_tx.clone());

        // Launch exploration if this group isn't being explored yet
        if !self.exploring_groups.contains(&group_id) {
            self.explore_group(group_id);
        }

        // Send the subscription request to the engine
        let mut message_tx = self.message_tx.clone();
        let expr_tx_clone = expr_tx.clone();

        tokio::spawn(async move {
            message_tx
                .send(SubscribeGroup(group_id, expr_tx_clone))
                .await
                .expect("Failed to send group subscription - channel closed");
        });

        expr_rx
    }
}
