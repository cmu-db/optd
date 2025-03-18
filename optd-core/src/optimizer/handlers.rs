use super::{
    ingest::{LogicalIngest, PhysicalIngest},
    jobs::JobKind,
    memo::{Memoize, MergeResult},
    tasks::{OptimizePlanResult, TaskKind},
    JobId, OptimizeRequest, Optimizer, OptimizerMessage, PendingMessage,
};
use crate::{
    cir::{
        expressions::{LogicalExpression, OptimizedExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        operators::Child,
        plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan, PhysicalPlan},
        properties::LogicalProperties,
    },
    engine::{LogicalExprContinuation, OptimizedExprContinuation},
};
use futures::{
    channel::{mpsc::Sender, oneshot},
    SinkExt,
};
use JobKind::*;
use OptimizerMessage::*;

impl<M: Memoize> Optimizer<M> {
    /// This method initiates the optimization process for a logical plan by launching
    /// an optimization task. It may need dependencies.
    pub(super) async fn process_optimize_request(
        &mut self,
        plan: LogicalPlan,
        response_tx: Sender<PhysicalPlan>,
    ) {
        if let OptimizePlanResult::NeedsDependencies(pending_dependencies) = self
            .launch_optimize_plan_task(plan.clone(), response_tx.clone())
            .await
        {
            // Store the request as a pending message that will be processed
            // once all create group dependencies are resolved.
            let pending_message = PendingMessage {
                message: OptimizeRequestWrapper(OptimizeRequest { plan, response_tx }),
                pending_dependencies,
            };

            self.pending_messages.push(pending_message);
        }
    }

    /// This method handles new logical plan alternatives discovered through
    /// transformation rule application. It may need dependencies. It might
    /// trigger group & goal merge (whic will triger notifications)
    pub(super) async fn process_new_logical_partial(
        &mut self,
        plan: PartialLogicalPlan,
        group_id: GroupId,
        job_id: JobId,
    ) {
        let group_id = self.group_repr.find(&group_id);
        let related_task_id = self.running_jobs[&job_id].0;

        match self.try_ingest_logical(&plan, related_task_id).await {
            LogicalIngest::Success(new_group_id) if new_group_id != group_id => {
                // Perform the merge in the memo and process all results
                let merge_results = self
                    .memo
                    .merge_groups(group_id, new_group_id)
                    .await
                    .expect("Failed to merge groups");

                for result in merge_results {
                    self.handle_merge_result(result).await;
                }
            }
            LogicalIngest::Success(_) => {
                // Group already exists, nothing to merge
            }
            LogicalIngest::NeedsDependencies(pending_dependencies) => {
                // Store as pending message to process after dependencies are resolved
                self.pending_messages.push(PendingMessage {
                    message: NewLogicalPartial(plan, group_id, job_id),
                    pending_dependencies,
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
        job_id: JobId,
    ) {
        let goal = self.goal_repr.find(&goal);
        let related_task_id = self.running_jobs[&job_id].0;

        let PhysicalIngest {
            goal: new_goal,
            new_expression: new_expr,
        } = self.try_ingest_physical(&plan).await;

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

            // If an expression was just created, its status is always dirty
            if let Some(expr) = new_expr {
                self.launch_cost_expression_task(expr, related_task_id);
            }
        }
    }

    /// This method handles fully optimized physical expressions with cost information.
    ///
    /// When a new optimized expression is found, it's added to the memo. If it becomes
    /// the new best expression for its goal, continuations are notified and client
    /// tasks receive the egested physical plan.
    pub(super) async fn process_new_optimized_expr(
        &mut self,
        expression: OptimizedExpression,
        goal: Goal,
    ) {
        // Update the expression & goal to use representative goals
        let goal = self.goal_repr.find(&goal);
        let expression = self.normalize_optimized_expression(&expression);

        // Add the optimized expression to the memo
        let new_best = self
            .memo
            .add_optimized_physical_expr(&goal, &expression)
            .await
            .expect("Failed to add optimized physical expression");

        // If this is the new best expression found so far for this goal,
        // schedule continuation jobs for all subscribers, and send to clients
        if new_best {
            self.schedule_optimized_continuations(&goal, expression.clone());
            self.egest_to_subscribers(&goal, expression).await;
        }
    }

    /// This method handles group creation for expressions with derived properties
    /// and updates any pending messages that depend on this group.
    pub(super) async fn process_create_group(
        &mut self,
        properties: LogicalProperties,
        expression: LogicalExpression,
        job_id: JobId,
    ) {
        self.memo
            .create_group(&expression, &properties)
            .await
            .expect("Failed to create group");

        self.resolve_dependencies(job_id).await;
    }

    /// Registers a continuation for receiving logical expressions from a group.
    /// The continuation will receive notifications about both existing and new expressions.
    pub(super) async fn process_group_subscription(
        &mut self,
        group_id: GroupId,
        continuation: LogicalExprContinuation,
        job_id: JobId,
    ) {
        let group_id = self.group_repr.find(&group_id);
        let related_task_id = self.running_jobs[&job_id].0;

        // Register the continuation directly with the task
        match &mut self
            .tasks
            .get_mut(&related_task_id)
            .expect("Task does not exist")
            .kind
        {
            TaskKind::TransformExpression(task) => {
                task.register_continuation(group_id, continuation.clone())
            }
            TaskKind::ImplementExpression(task) => {
                task.register_continuation(group_id, continuation.clone())
            }
            _ => panic!("Task type cannot produce group subscription"),
        }

        // Subscribe to future expressions and bootstrap with existing ones
        let expressions = self
            .subscribe_task_to_group(group_id, related_task_id)
            .await;
        for expr in expressions {
            self.schedule_job(
                related_task_id,
                ContinueWithLogical(expr, continuation.clone()),
            );
        }
    }

    /// Registers a continuation for receiving optimized physical expressions for a goal.
    /// The continuation will be notified about the best existing expression and any better ones found.
    pub(super) async fn process_goal_subscription(
        &mut self,
        goal: Goal,
        continuation: OptimizedExprContinuation,
        job_id: JobId,
    ) {
        let goal = self.goal_repr.find(&goal);
        let related_task_id = self.running_jobs[&job_id].0;

        // Verify task type and register the continuation
        match &mut self
            .tasks
            .get_mut(&related_task_id)
            .expect("Task does not exist")
            .kind
        {
            TaskKind::CostExpression(task) => {
                task.register_continuation(goal.clone(), continuation.clone())
            }
            _ => panic!("Only cost tasks can subscribe to goals"),
        }

        // Subscribe to future optimized expressions and bootstrap with current best
        if let Some(best_expr) = self
            .subscribe_task_to_goal(goal.clone(), related_task_id)
            .await
        {
            self.schedule_job(
                related_task_id,
                ContinueWithOptimized(best_expr, continuation),
            );
        }
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
            .get_logical_properties(group_id)
            .await
            .expect("Failed to get logical properties");

        // We don't want to make a job out of this, as it is merely a way to unblock
        // an existing pending job. We send it to the channel without blocking the
        // main co-routine.
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
        todo!()
    }

    /// Helper method to resolve dependencies after a group creation job completes
    ///
    /// This method is called when a group creation job completes. It updates all
    /// pending messages that were waiting for this job and processes any that
    /// are now ready (have no more pending dependencies).
    async fn resolve_dependencies(&mut self, completed_job_id: JobId) {
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

            // Re-send the message to be processed in a new co-routine to not block the
            // main co-routine.
            let mut message_tx = self.message_tx.clone();
            tokio::spawn(async move {
                message_tx
                    .send(pending.message)
                    .await
                    .expect("Failed to re-send ready message");
            });
        }
    }

    /// Helper method to normalize an optimized expression by updating all children goals
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
