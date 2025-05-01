use super::{
    EngineMessageKind, Optimizer, Task, TaskId, client::ClientMessage, tasks::SourceTaskId,
};
use crate::{
    core::cir::{
        Cost, Goal, GoalId, GoalMemberId, GroupId, LogicalProperties, PartialLogicalPlan,
        PartialPhysicalPlan, PhysicalExpressionId,
    },
    core::error::Error,
    core::memo::Memoize,
};

use crate::dsl::{
    analyzer::hir::Value,
    engine::{Continuation, EngineResponse},
};
use futures::{SinkExt, channel::mpsc::Sender};

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
    pub(super) async fn process_client_request(
        &mut self,
        message: ClientMessage,
    ) -> Result<bool, Error> {
        match message {
            ClientMessage::Init {
                logical_plan,
                physical_plan_tx,
                id_tx,
            } => {
                // Creates an OptimizePlanTask and register as a new query instance.
                let task_id = self
                    .create_optimize_plan_task(logical_plan, physical_plan_tx)
                    .await?;
                let query_instance_id = self.next_query_instance_id();
                self.query_instances.insert(query_instance_id, task_id);
                let _ = id_tx.send(query_instance_id);
                Ok(false)
            }
            ClientMessage::Complete { query_instance_id } => {
                // Remove the OptimizePlanTask from the tasks and clean up subscriptions.
                let task_id = self.query_instances.remove(&query_instance_id).unwrap();
                let task = self.tasks.remove(&task_id).unwrap().into_optimize_plan();
                let optimize_goal_task = self
                    .tasks
                    .get_mut(&task.optimize_goal_in)
                    .unwrap()
                    .as_optimize_goal_mut();

                if let Some(index) = optimize_goal_task
                    .optimize_plan_out
                    .iter()
                    .position(|id| id == &task_id)
                {
                    optimize_goal_task.optimize_plan_out.swap_remove(index);
                }

                // TODO(yuchen): if the goal has no other subscribers, we should remove it.
                Ok(false)
            }
            ClientMessage::Shutdown => {
                // TODO(yuchen): Clean up state.
                Ok(true)
            }
        }
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
    ) -> Result<(), Error> {
        let new_group_id = self.ingest_logical_plan(&plan).await?;
        // Atomically perform the merge in the memo and process all results.
        if let Some(merge_results) = self.memo.merge_groups(group_id, new_group_id).await? {
            self.handle_merge_result(merge_results).await?;
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
        _task_id: TaskId,
    ) -> Result<(), Error> {
        let goal_id = self.memo.find_repr_goal(goal_id).await?;

        let member = self.ingest_physical_plan(&plan).await?;

        let forward_result = self.memo.add_goal_member(goal_id, member).await?;

        match member {
            GoalMemberId::PhysicalExpressionId(physical_expr_id) => {
                self.ensure_cost_expression_task(physical_expr_id, Cost(f64::MAX), _task_id)
                    .await?;
            }
            GoalMemberId::GoalId(ref_goal_id) => {
                self.ensure_optimize_goal_task(ref_goal_id, SourceTaskId::OptimizeGoal(_task_id))
                    .await?;
            }
        }

        if let Some(forward_result) = forward_result {
            self.handle_forward_result(forward_result).await?;
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
        physical_expr_id: PhysicalExpressionId,
        cost: Cost,
    ) -> Result<(), Error> {
        let result = self
            .memo
            .update_physical_expr_cost(physical_expr_id, cost)
            .await?;

        if let Some(result) = result {
            self.handle_forward_result(result).await?;
        }
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
                    .add_transformation_dependency(task.logical_expr_id, &task.rule, group_id)
                    .await?;
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
    ) -> Result<(), Error> {
        // Update the logical properties in the memo.
        self.memo
            .set_logical_properties(group_id, properties.clone())
            .await?;

        let (_, senders) = self.pending_derives.remove(&group_id).unwrap();

        tokio::spawn(async move {
            for mut sender in senders {
                let _ = sender.send(properties.clone()).await;
            }
        });
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
        mut sender: Sender<LogicalProperties>,
    ) -> Result<(), Error> {
        if let Some(props) = self.memo.get_logical_properties(group_id).await? {
            tokio::spawn(async move {
                sender
                    .send(props)
                    .await
                    .expect("Failed to send properties - channel closed.");
            });
        } else {
            // Schedule a job to retrieve the properties.
            self.schedule_derive_job(group_id, sender);
        }

        Ok(())
    }
}
