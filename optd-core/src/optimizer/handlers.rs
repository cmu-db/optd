use super::{
    EngineMessage, EngineMessageKind, JobId, OptimizeRequest, Optimizer, PendingMessage, Task,
    TaskId, ingest::LogicalIngest, tasks::SourceTaskId,
};
use crate::{
    cir::{
        Cost, Goal, GoalId, GoalMemberId, GroupId, LogicalExpressionId, LogicalPlan,
        LogicalProperties, PartialLogicalPlan, PartialPhysicalPlan, PhysicalExpressionId,
        PhysicalPlan, PhysicalProperties,
    },
    error::Error,
    memo::Memoize,
};
use EngineMessageKind::*;
use LogicalIngest::*;
use futures::{
    SinkExt,
    channel::{mpsc::Sender, oneshot},
};
use optd_dsl::{
    analyzer::hir::Value,
    engine::{Continuation, EngineResponse},
};

impl<M: Memoize> Optimizer<M> {
    /// This method initiates the optimization process for a logical plan by launching
    /// an optimization task. It may need dependencies.
    ///
    /// # Parameters
    /// * `plan` - The logical plan to optimize.
    /// * `response_tx` - Channel to send the resulting physical plan.
    /// * `task_id` - ID of the task that initiated this request.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during processing.
    pub(super) async fn process_optimize_request(
        &mut self,
        plan: LogicalPlan,
        response_tx: Sender<PhysicalPlan>,
        task_id: TaskId,
    ) -> Result<(), Error> {
        match self.probe_ingest_logical_plan(&plan.clone().into()).await? {
            Found(group_id) => {
                // The goal represents what we want to achieve: optimize the root group
                // with no specific physical properties required.
                let goal = Goal(group_id, PhysicalProperties(None));
                let goal_id = self.memo.get_goal_id(&goal).await?;

                // This ensures the task will be notified when optimized expressions
                // for this goal are found.
                self.subscribe_task_to_goal(goal_id, task_id).await?;
            }
            Missing(logical_exprs) => {
                // Store the request as a pending message that will be processed
                // once all create task dependencies are resolved.
                let pending_dependencies = logical_exprs
                    .iter()
                    .cloned()
                    .map(|logical_expr_id| {
                        self.schedule_job(
                            task_id,
                            JobKind::DeriveLogicalProperties(logical_expr_id),
                        )
                    })
                    .collect();

                let pending_message = PendingMessage {
                    message: EngineMessage::new(
                        JobId(-1),
                        OptimizeRequestWrapper(OptimizeRequest { plan, response_tx }, task_id),
                    ),

                    pending_dependencies,
                };

                self.pending_messages.push(pending_message);
            }
        }

        Ok(())
    }

    /// This method handles new logical plan alternatives discovered through
    /// transformation rule application.
    ///
    /// # Parameters
    /// * `plan` - The partial logical plan to process.
    /// * `group_id` - ID of the group associated with this plan.
    /// * `job_id` - ID of the job that generated this plan.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during processing.
    pub(super) async fn process_new_logical_partial(
        &mut self,
        plan: PartialLogicalPlan,
        group_id: GroupId,
        job_id: JobId,
    ) -> Result<(), Error> {
        let group_id = self.memo.find_repr_group(group_id).await?;
        match self.probe_ingest_logical_plan(&plan).await? {
            Found(new_group_id) => {
                if new_group_id != group_id {
                    // Atomically perform the merge in the memo and process all results.
                    let merge_results = self.memo.merge_groups(group_id, new_group_id).await?;
                    self.handle_merge_result(merge_results).await?;
                }
            }
            Missing(logical_exprs) => {
                // Store the request as a pending message that will be processed
                // once all create task dependencies are resolved.
                let related_task_id = self.running_jobs[&job_id].0;
                let pending_dependencies = logical_exprs
                    .iter()
                    .cloned()
                    .map(|logical_expr_id| {
                        self.schedule_job(
                            related_task_id,
                            JobKind::DeriveLogicalProperties(logical_expr_id),
                        )
                    })
                    .collect();

                let pending_message = PendingMessage {
                    message: EngineMessage::new(job_id, NewLogicalPartial(plan, group_id)),
                    pending_dependencies,
                };

                self.pending_messages.push(pending_message);
            }
        }

        Ok(())
    }

    /// This method handles new physical implementations discovered through
    /// implementation rule application.
    ///
    /// # Parameters
    /// * `plan` - The partial physical plan to process.
    /// * `goal_id` - ID of the goal associated with this plan.
    /// * `job_id` - ID of the job that generated this plan.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during processing.
    pub(super) async fn process_new_physical_partial(
        &mut self,
        plan: PartialPhysicalPlan,
        goal_id: GoalId,
        job_id: JobId,
    ) -> Result<(), Error> {
        let goal_id = self.memo.find_repr_goal(goal_id).await?;

        let member = self.probe_ingest_physical_plan(&plan).await?;

        self.receive_new_goal_member(member, goal_id, job_id).await?;

        Ok(())
    }

    /// This method handles fully optimized physical expressions with cost information.
    ///
    /// When a new optimized expression is found, it's added to the memo. If it becomes
    /// the new best expression for its goal, continuations are notified and and clients
    /// receive the corresponding egested plan.
    ///
    /// # Parameters
    /// * `expression_id` - ID of the physical expression to process.
    /// * `cost` - Cost information for the expression.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during processing.
    pub(super) async fn process_new_costed_physical(
        &mut self,
        expression_id: PhysicalExpressionId,
        cost: Cost,
    ) -> Result<(), Error> {
        let expression_id = self.memo.find_repr_physical_expr(expression_id).await?;
        let new_best = self
            .memo
            .update_physical_expr_cost(expression_id, cost)
            .await?;

        // If this is the new best expression found so far for this goal,
        // schedule continuation jobs for all subscribers and send to clients.
        if new_best {
            // TODO(Alexis): Needs to send to parents.
            // self.schedule_optimized_continuations(goal_id, expression_id, cost);
            // self.egest_to_subscribers(goal_id, expression_id).await?;
        }

        Ok(())
    }

    /// This method handles group creation for expressions with derived properties
    /// and updates any pending messages that depend on this group.
    ///
    /// # Parameters
    /// * `expression_id` - ID of the logical expression to create a group for.
    /// * `properties` - Logical properties associated with the expression.
    /// * `job_id` - ID of the job that initiated this request.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during processing.
    pub(super) async fn process_create_group(
        &mut self,
        expression_id: LogicalExpressionId,
        properties: &LogicalProperties,
        job_id: JobId,
    ) -> Result<(), Error> {
        self.memo.create_group(expression_id, properties).await?;
        self.resolve_dependencies(job_id).await;
        Ok(())
    }

    /// Registers a continuation for receiving logical expressions from a group.
    /// The continuation will receive notifications about both existing and new expressions.
    ///
    /// # Parameters
    /// * `group_id` - ID of the group to subscribe to.
    /// * `continuation` - Continuation to call when new expressions are found.
    /// * `job_id` - ID of the job that initiated this request.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during processing.
    pub(super) async fn process_group_subscription(
        &mut self,
        group_id: GroupId,
        continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,
        task_id: TaskId,
    ) -> Result<(), Error> {
        let fork_task_id = self
            .create_fork_logical_task(group_id, continuation, task_id)
            .await?;

        match self.tasks.get_mut(&task_id).unwrap() {
            Task::ImplementExpression(task) => {
                task.add_fork_in(fork_task_id);
                self.memo
                    .add_implementation_dependency(
                        task.logical_expr_id,
                        task.goal_id,
                        &task.rule,
                        group_id,
                    )
                    .await?;
            }
            Task::TransformExpression(task) => {
                task.add_fork_in(fork_task_id);
                self.memo
                    .add_transformation_dependency(task.logical_expr_id, &task.rule, group_id);
            }
            Task::ContinueWithLogical(task) => {
                // TODO(yuchen): track dependencies back to the root transform/implement task.
                task.add_fork_in(fork_task_id);
            }
            task => {
                panic!("Task type cannot produce group subscription: {:?}", task);
            }
        }
        Ok(())
    }

    /// Registers a continuation for receiving optimized physical expressions for a goal.
    /// The continuation will be notified about the best existing expression and any better ones found.
    ///
    /// # Parameters
    /// * `goal` - The goal to subscribe to.
    /// * `continuation` - Continuation to call when new optimized expressions are found.
    /// * `job_id` - ID of the job that initiated this request.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during processing.
    pub(super) async fn process_goal_subscription(
        &mut self,
        goal: &Goal,
        continuation: Continuation<Value, EngineResponse<EngineMessageKind>>,
        task_id: TaskId,
    ) -> Result<(), Error> {
        let goal_id = self.memo.get_goal_id(goal).await?;

        let fork_task_id = self
            .create_fork_costed_task(goal_id, continuation, task_id)
            .await?;

        match self.tasks.get_mut(&task_id).unwrap() {
            Task::CostExpression(task) => {
                task.add_fork_in(fork_task_id);
                self.memo
                    .add_cost_dependency(task.physical_expr_id, goal_id)
                    .await?;
            }
            Task::ContinueWithCosted(task) => {
                // TODO(yuchen): track dependencies back to the root cost expression.
                task.add_fork_in(fork_task_id);
            }
            task => {
                panic!("Task type cannot produce goal subscription: {:?}", task);
            }
        }

        Ok(())
    }

    // TODO(yuchen): resolve dependencies.
    pub(super) async fn process_new_properties(
        &mut self,
        group_id: GroupId,
        properties: LogicalProperties,
        job_id: JobId,
    ) -> Result<(), Error> {
        // Update the logical properties in the memo.
        self.memo
            .set_logical_properties(group_id, properties)
            .await?;

        // Resolve dependencies for any pending messages that were waiting for this group to be created.
        self.resolve_dependencies(job_id).await;
        Ok(())
    }

    /// Retrieves the logical properties for the given group from the memo
    /// and sends them back to the requestor through the provided oneshot channel.
    ///
    /// # Parameters
    /// * `group_id` - ID of the group to retrieve properties for.
    /// * `sender` - Channel to send the properties through.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during processing.
    pub(super) async fn process_retrieve_properties(
        &mut self,
        group_id: GroupId,
        sender: oneshot::Sender<LogicalProperties>,
    ) -> Result<(), Error> {
        let props = self.memo.get_logical_properties(group_id).await?;

        // We don't want to make a job out of this, as it is merely a way to unblock
        // an existing pending job. We send it to the channel without blocking the
        // main co-routine.
        tokio::spawn(async move {
            sender
                .send(props)
                .expect("Failed to send properties - channel closed.");
        });

        Ok(())
    }

    /// Helper method to resolve dependencies after a group creation job completes.
    ///
    /// This method is called when a group creation job completes. It updates all
    /// pending messages that were waiting for this job and processes any that
    /// are now ready (have no more pending dependencies).
    ///
    /// # Parameters
    /// * `completed_job_id` - ID of the completed job.
    async fn resolve_dependencies(&mut self, completed_job_id: JobId) {
        // Update dependencies and collect ready messages.
        let ready_indices: Vec<_> = self
            .pending_messages
            .iter_mut()
            .enumerate()
            .filter_map(|(i, pending)| {
                pending.pending_dependencies.remove(&completed_job_id);
                pending.pending_dependencies.is_empty().then_some(i)
            })
            .collect();

        // Process all ready messages (in reverse order to avoid index issues when removing).
        for i in ready_indices.iter().rev() {
            let pending = self.pending_messages.swap_remove(*i);

            // Re-send the message to be processed in a new co-routine to not block the
            // main co-routine.
            let mut message_tx = self.message_tx.clone();
            tokio::spawn(async move {
                message_tx
                    .send(pending.message)
                    .await
                    .expect("Failed to re-send ready message - channel closed.");
            });
        }
    }
}
