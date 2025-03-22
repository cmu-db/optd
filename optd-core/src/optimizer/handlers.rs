use super::{
    ingest::{LogicalIngest, PhysicalIngest},
    jobs::JobKind,
    memo::{Memoize, MergeResult},
    tasks::{CostExpressionTask, ImplementExpressionTask, TaskKind, TransformExpressionTask},
    JobId, OptimizeRequest, Optimizer, OptimizerMessage, PendingMessage, TaskId,
};
use crate::{
    cir::{
        expressions::{LogicalExpressionId, PhysicalExpressionId},
        goal::{Cost, Goal, GoalId},
        group::GroupId,
        plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan, PhysicalPlan},
        properties::{LogicalProperties, PhysicalProperties},
    },
    engine::{CostedPhysicalPlanContinuation, LogicalPlanContinuation},
    error::Error,
};
use futures::{
    channel::{mpsc::Sender, oneshot},
    SinkExt,
};
use JobKind::*;
use LogicalIngest::*;
use OptimizerMessage::*;
use TaskKind::*;

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
                        self.schedule_job(task_id, DeriveLogicalProperties(logical_expr_id))
                    })
                    .collect();

                let pending_message = PendingMessage {
                    message: OptimizeRequestWrapper(OptimizeRequest { plan, response_tx }, task_id),
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
            Found(new_group_id) if new_group_id != group_id => {
                // Atomically perform the merge in the memo and process all results.
                let merge_results = self.memo.merge_groups(group_id, new_group_id).await?;
                self.handle_merge_result(merge_results).await;
            }
            Found(_) => {
                // Group already exists, nothing to merge or do.
            }
            Missing(logical_exprs) => {
                // Store the request as a pending message that will be processed
                // once all create task dependencies are resolved.
                let related_task_id = self.running_jobs[&job_id].0;
                let pending_dependencies = logical_exprs
                    .iter()
                    .cloned()
                    .map(|logical_expr_id| {
                        self.schedule_job(related_task_id, DeriveLogicalProperties(logical_expr_id))
                    })
                    .collect();

                let pending_message = PendingMessage {
                    message: NewLogicalPartial(plan, group_id, job_id),
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
        let PhysicalIngest {
            goal_id: ingested_goal_id,
            new_expression_id,
        } = self.ingest_physical_plan(&plan).await?;

        if ingested_goal_id != goal_id {
            // Atomically perform the merge in the memo and process all results.
            let merge_results = self.memo.merge_goals(ingested_goal_id, goal_id).await?;
            self.handle_merge_result(merge_results).await;

            // If a physical expression was just created, its status is *always* dirty
            // and will need to be costed.
            if let Some(expression_id) = new_expression_id {
                let related_task_id = self.running_jobs[&job_id].0;
                self.launch_cost_expression_task(expression_id, related_task_id)
                    .await?;
            }
        }

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
        let (new_best, goal_id) = self
            .memo
            .update_physical_expr_cost(expression_id, cost)
            .await?;

        // If this is the new best expression found so far for this goal,
        // schedule continuation jobs for all subscribers and send to clients.
        if new_best {
            self.schedule_optimized_continuations(goal_id, &[(expression_id, cost)]);
            self.egest_to_subscribers(goal_id, expression_id).await?;
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
        continuation: LogicalPlanContinuation,
        job_id: JobId,
    ) -> Result<(), Error> {
        let related_task_id = self.running_jobs[&job_id].0;

        // Register the continuation and notify the memo about the dependency to ensure the
        // operation corresponding to the task gets invalidated when the group has new expressions.
        match &mut self.tasks.get_mut(&related_task_id).unwrap().kind {
            TransformExpression(TransformExpressionTask {
                rule,
                expression_id,
                continuations,
                ..
            }) => {
                continuations
                    .entry(group_id)
                    .or_default()
                    .push(continuation.clone());

                self.memo
                    .add_transformation_dependency(*expression_id, rule, group_id)
                    .await?;
            }
            ImplementExpression(ImplementExpressionTask {
                rule,
                expression_id,
                goal_id,
                continuations,
                ..
            }) => {
                continuations
                    .entry(group_id)
                    .or_default()
                    .push(continuation.clone());

                self.memo
                    .add_implementation_dependency(*expression_id, *goal_id, rule, group_id)
                    .await?;
            }
            _ => panic!("Task type cannot produce group subscription."),
        }

        // Subscribe to future expressions and bootstrap with existing ones.
        let expressions = self
            .subscribe_task_to_group(group_id, related_task_id)
            .await?;

        for expression_id in expressions {
            self.schedule_job(
                related_task_id,
                ContinueWithLogical(expression_id, continuation.clone()),
            );
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
        continuation: CostedPhysicalPlanContinuation,
        job_id: JobId,
    ) -> Result<(), Error> {
        let related_task_id = self.running_jobs[&job_id].0;
        let goal_id = self.memo.get_goal_id(goal).await?;

        // Register the continuation and notify the memo about the dependency to ensure the
        // operation corresponding to the task gets invalidated when the goal has a new optimum.
        match &mut self.tasks.get_mut(&related_task_id).unwrap().kind {
            CostExpression(CostExpressionTask {
                expression_id,
                continuations,
                ..
            }) => {
                continuations
                    .entry(goal_id)
                    .or_default()
                    .push(continuation.clone());

                self.memo
                    .add_cost_dependency(*expression_id, goal_id)
                    .await?;
            }
            _ => panic!("Only cost tasks can subscribe to goals."),
        }

        // Subscribe to future optimized expressions and bootstrap with current best.
        if let Some((best_expr_id, cost)) = self
            .subscribe_task_to_goal(goal_id, related_task_id)
            .await?
        {
            self.schedule_job(
                related_task_id,
                ContinueWithCostedPhysical(best_expr_id, cost, continuation),
            );
        }

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

    /// Helper method to handle different types of merge results.
    ///
    /// This method processes the results of group and goal merges, updating
    /// subscribers and tasks appropriately.
    ///
    /// # Parameters
    /// * `result` - The merge result to handle.
    async fn handle_merge_result(&mut self, result: MergeResult) {
        for group_merge in result.group_merges {
            let all_exprs_by_group = group_merge.merged_groups;

            // For each group, schedule expressions from all OTHER groups,
            // ignoring any potential duplicates due to merges for now.
            for (i, (current_group_id, _)) in all_exprs_by_group.iter().enumerate() {
                let other_groups_exprs: Vec<_> = all_exprs_by_group
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i) // Filter out the current group.
                    .flat_map(|(_, (_, exprs))| exprs)
                    .copied()
                    .collect();

                self.schedule_logical_continuations(*current_group_id, &other_groups_exprs);
            }

            // 2. Task updates using indexes.
        }

        for goal_merge in result.goal_merges {
            let all_exprs_by_goal = goal_merge.merged_goals;

            // For each goal, schedule expressions from all OTHER goals,
            // ignoring any potential duplicates due to merges for now.
            for (i, (current_goal_id, _)) in all_exprs_by_goal.iter().enumerate() {
                let other_goals_exprs: Vec<_> = all_exprs_by_goal
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i) // Filter out the current goal.
                    .flat_map(|(_, (_, exprs))| exprs)
                    .copied()
                    .collect();

                self.schedule_optimized_continuations(*current_goal_id, &other_goals_exprs);
            }

            // 2. Task updates using indexes.
        }

        // 3. Dirty stuff.
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
