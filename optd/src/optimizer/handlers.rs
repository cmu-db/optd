use super::{JobId, Optimizer, TaskId, jobs::LogicalContinuation};
use crate::{
    cir::*,
    memo::Memo,
    optimizer::{
        EngineProduct, OptimizeRequest, OptimizerMessage, PendingMessage, jobs::JobKind,
        memo_io::LogicalIngest,
    },
};
use tokio::sync::mpsc::Sender;

impl<M: Memo> Optimizer<M> {
    /// This method initiates the optimization process for a logical plan by launching
    /// an optimization task. It may need dependencies.
    ///
    /// # Parameters
    /// * `plan` - The logical plan to optimize.
    /// * `physical_tx` - Channel to send the resulting physical plan.
    /// * `optimize_plan_task_id` - ID of the task that initiated this request.
    ///
    /// # Returns
    /// * `Result<(), Error>` - Success or error during processing.
    pub(super) async fn process_optimize_request(
        &mut self,
        plan: LogicalPlan,
        physical_tx: Sender<PhysicalPlan>,
        optimize_plan_task_id: TaskId,
    ) -> Result<(), M::MemoError> {
        use JobKind::*;
        use LogicalIngest::*;
        use OptimizerMessage::*;

        match self.probe_ingest_logical_plan(&plan.clone().into()).await? {
            Found(group_id) => {
                // The goal represents what we want to achieve: optimize the root group
                // with no specific physical properties required.
                let goal = Goal(group_id, PhysicalProperties(None));
                let goal_id = self.memo.get_goal_id(&goal).await?;

                // Launch the corresponding task now that we know the goal_id.
                self.launch_optimize_plan_task(optimize_plan_task_id, goal_id)
                    .await?;
            }
            Missing(logical_exprs) => {
                // Store the request as a pending message that will be processed
                // once all create task dependencies are resolved.
                let pending_dependencies = logical_exprs
                    .iter()
                    .cloned()
                    .map(|logical_expr_id| {
                        self.schedule_job(optimize_plan_task_id, Derive(logical_expr_id))
                    })
                    .collect();

                self.pending_messages.push(PendingMessage::new(
                    Request(OptimizeRequest { plan, physical_tx }, optimize_plan_task_id),
                    pending_dependencies,
                ));
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
    ) -> Result<(), M::MemoError> {
        use EngineProduct::*;
        use JobKind::*;
        use LogicalIngest::*;
        use OptimizerMessage::*;

        let group_id = self.memo.find_repr_group_id(group_id).await?;

        match self.probe_ingest_logical_plan(&plan).await? {
            Found(new_group_id) if new_group_id != group_id => {
                // Atomically perform the merge in the memo and process all results.
                let merge_results = self.memo.merge_groups(group_id, new_group_id).await?;

                self.handle_merge_result(merge_results).await?;
            }
            Found(_) => {
                // Group already exists, nothing to merge or do.
            }
            Missing(logical_exprs) => {
                // Store the request as a pending message that will be processed
                // once all create task dependencies are resolved.
                let related_task_id = self.get_related_task_id(job_id);
                let pending_dependencies = logical_exprs
                    .iter()
                    .cloned()
                    .map(|logical_expr_id| {
                        self.schedule_job(related_task_id, Derive(logical_expr_id))
                    })
                    .collect();

                self.pending_messages.push(PendingMessage::new(
                    Product(NewLogicalPartial(plan, group_id), job_id),
                    pending_dependencies,
                ));
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
    ) -> Result<(), M::MemoError> {
        use GoalMemberId::*;

        let goal_id = self.memo.find_repr_goal_id(goal_id).await?;
        let related_task_id = self.get_related_task_id(job_id);

        let member_id = self.probe_ingest_physical_plan(&plan).await?;
        let added = self.memo.add_goal_member(goal_id, member_id).await?;

        match member_id {
            PhysicalExpressionId(_) => {
                // TODO: Here we would launch costing tasks based on the design.
            }
            GoalId(goal_id) => {
                if added {
                    // Optimize the new sub-goal and add to task graph.
                    let sub_optimize_task_id = self.ensure_optimize_goal_task(goal_id).await?;

                    self.get_optimize_goal_task_mut(sub_optimize_task_id)
                        .unwrap()
                        .optimize_goal_out
                        .insert(related_task_id);
                    self.get_optimize_goal_task_mut(related_task_id)
                        .unwrap()
                        .optimize_goal_in
                        .insert(sub_optimize_task_id);
                }
            }
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
    ) -> Result<(), M::MemoError> {
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
        continuation: LogicalContinuation,
        job_id: JobId,
    ) -> Result<(), M::MemoError> {
        let parent_task_id = self.get_related_task_id(job_id);
        self.launch_fork_logical_task(group_id, continuation, parent_task_id)
            .await
    }

    /// Retrieves the logical properties for the given group from the memo
    /// and sends them back to the requestor through the provided channel.
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
        sender: Sender<LogicalProperties>,
    ) -> Result<(), M::MemoError> {
        let props = self.memo.get_logical_properties(group_id).await?;

        // We don't want to make a job out of this, as it is merely a way to unblock
        // an existing pending job. We send it to the channel without blocking the
        // main co-routine.
        tokio::spawn(async move {
            sender
                .send(props)
                .await
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
            let message_tx = self.message_tx.clone();
            tokio::spawn(async move {
                message_tx
                    .send(pending.message)
                    .await
                    .expect("Failed to re-send ready message - channel closed.");
            });
        }
    }
}
