use super::{
    ingest::LogicalIngest,
    memo::{Memoize, MergeResult},
    OptimizeRequest, Optimizer, OptimizerMessage, PendingMessage,
};
use crate::{
    capture,
    cir::{
        expressions::{LogicalExpression, OptimizedExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        operators::Child,
        plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan, PhysicalPlan},
        properties::{LogicalProperties, PhysicalProperties},
    },
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
        match self.try_ingest_logical(&logical_plan.clone().into()).await {
            LogicalIngest::Success(group_id) => {
                // Plan was ingested successfully, subscribe to the goal
                let goal = Goal(group_id, PhysicalProperties(None));
                let goal = self.goal_repr.find(&goal);

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
        let plan = self
            .egest_best_plan(&expr)
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
        let group_id = self.group_repr.find(&group_id);

        match self.try_ingest_logical(&plan).await {
            LogicalIngest::Success(new_group_id) if new_group_id != group_id => {
                // Perform the merge in the memo and process all results
                let merge_results = self
                    .memo
                    .merge_groups(&group_id, &new_group_id)
                    .await
                    .expect("Failed to merge groups");

                for result in merge_results {
                    self.handle_merge_result(result).await;
                }
            }
            LogicalIngest::Success(_) => {
                // Group already exists, nothing to merge
            }
            LogicalIngest::NeedsDependencies(dependencies) => {
                // Store as pending message to process after dependencies are resolved
                self.pending_messages.push(PendingMessage {
                    message: NewLogicalPartial(plan, group_id),
                    pending_dependencies: dependencies,
                });
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
        let goal = self.goal_repr.find(&goal);
        let new_goal = self.try_ingest_physical(&plan).await;

        // If the goals are different, merge them
        if new_goal != goal {
            // Perform the merge in the memo and process all results
            let merge_results = self
                .memo
                .merge_goals(&goal, &new_goal)
                .await
                .expect("Failed to merge goals");

            for result in merge_results {
                self.handle_merge_result(result).await;
            }
        }
    }

    /// This method handles fully optimized physical expressions with cost information.
    pub(super) async fn process_new_optimized_expression(
        &mut self,
        expr: OptimizedExpression,
        goal: Goal,
    ) {
        // Update the expression & goal to use representative goals
        let goal = self.goal_repr.find(&goal);
        let expr = self.normalize_optimized_expression(&expr);

        // Add the optimized expression to the memo
        let new_best = self
            .memo
            .add_optimized_physical_expr(&goal, &expr)
            .await
            .expect("Failed to add optimized physical expression");

        // If this is the new best expression found so far for this goal,
        // notify all subscribers
        if new_best {
            let subscribers = self
                .goal_subscribers
                .get(&goal)
                .cloned()
                .unwrap_or_default();

            for mut subscriber in subscribers {
                tokio::spawn(capture!([expr], async move {
                    subscriber
                        .send(expr)
                        .await
                        .expect("Failed to send optimized expression");
                }));
            }
        }
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
            .create_group(&expression, &properties)
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
        let group_id = self.group_repr.find(&group_id);
        self.subscribe_to_group(group_id, sender).await;
    }

    /// Sends the best existing physical expression for the goal to the subscriber
    /// and initiates implementation of the goal if it hasn't been launched yet.
    pub(super) async fn process_goal_subscription(
        &mut self,
        goal: Goal,
        sender: Sender<OptimizedExpression>,
    ) {
        let goal = self.goal_repr.find(&goal);
        self.subscribe_to_goal(goal, sender).await;
    }

    /// Retrieves the logical properties for the given group from the memo
    /// and sends them back to the requestor through the provided oneshot channel.
    pub(super) async fn process_retrieve_properties(
        &mut self,
        group_id: GroupId,
        sender: oneshot::Sender<LogicalProperties>,
    ) {
        let group_id = self.group_repr.find(&group_id);
        let props = self
            .memo
            .get_logical_properties(&group_id)
            .await
            .expect("Failed to get logical properties");

        tokio::spawn(async move {
            sender
                .send(props)
                .expect("Failed to send properties - channel closed");
        });
    }

    /// Helper method to handle different types of merge results
    ///
    /// This method processes the results of group and goal merges, updating
    /// representatives, subscribers, and exploration status appropriately.
    async fn handle_merge_result(&mut self, result: MergeResult) {
        match result {
            MergeResult::GroupMerge {
                prev_group_id,
                new_group_id,
                expressions,
            } => {
                // Update representative tracking
                self.group_repr.merge(&prev_group_id, &new_group_id);

                // Get subscribers for the previous group
                let subscribers = self
                    .group_subscribers
                    .remove(&prev_group_id)
                    .unwrap_or_default();

                // Send expressions to all subscribers
                for expr in &expressions {
                    for mut subscriber in subscribers.clone() {
                        tokio::spawn(capture!([expr], async move {
                            subscriber
                                .send(expr)
                                .await
                                .expect("Failed to send logical expression");
                        }));
                    }
                }

                // Add subscribers to new group
                self.group_subscribers
                    .entry(new_group_id)
                    .or_default()
                    .extend(subscribers);

                // Inherit exploration status
                if self.exploring_groups.remove(&prev_group_id) {
                    self.exploring_groups.insert(new_group_id);
                }
            }
            MergeResult::GoalMerge {
                prev_goal,
                new_goal,
                expression,
            } => {
                // Update goal representative
                self.goal_repr.merge(&prev_goal, &new_goal);

                // Get subscribers for the previous goal
                let subscribers = self.goal_subscribers.remove(&prev_goal).unwrap_or_default();

                // Send optimized expression if present
                if let Some(expr) = &expression {
                    for mut subscriber in subscribers.clone() {
                        tokio::spawn(capture!([expr], async move {
                            subscriber
                                .send(expr)
                                .await
                                .expect("Failed to send optimized expression");
                        }));
                    }
                }

                // Add subscribers to new goal
                self.goal_subscribers
                    .entry(new_goal.clone())
                    .or_default()
                    .extend(subscribers);

                // Inherit exploration status
                if self.exploring_goals.remove(&prev_goal) {
                    self.exploring_goals.insert(new_goal);
                }
            }
        }
    }

    /// Helper method to resolve dependencies after a group creation job completes
    ///
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

    /// Helper method to normalize an optimized expression by updating all child goals
    /// to use their representative goals.
    ///
    /// This ensures consistency in the memo by always working with canonical representatives.
    fn normalize_optimized_expression(&self, expr: &OptimizedExpression) -> OptimizedExpression {
        let normalized_children = expr
            .0
            .children
            .iter()
            .map(|child| match child {
                Child::Singleton(goal) => {
                    let goal = self.goal_repr.find(goal);
                    Child::Singleton(goal)
                }
                Child::VarLength(goals) => {
                    let goals = goals.iter().map(|goal| self.goal_repr.find(goal)).collect();
                    Child::VarLength(goals)
                }
            })
            .collect();

        OptimizedExpression(
            PhysicalExpression {
                tag: expr.0.tag.clone(),
                data: expr.0.data.clone(),
                children: normalized_children,
            },
            expr.1,
        )
    }
}
