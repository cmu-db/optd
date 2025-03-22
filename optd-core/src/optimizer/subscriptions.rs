use super::{
    jobs::JobKind,
    memo::Memoize,
    tasks::{TaskId, TaskKind},
    Optimizer,
};
use crate::{
    cir::{
        expressions::{LogicalExpressionId, PhysicalExpressionId},
        goal::{Cost, GoalId},
        group::GroupId,
    },
    error::Error,
};
use futures::SinkExt;
use JobKind::*;
use TaskKind::*;

impl<M: Memoize> Optimizer<M> {
    /// Subscribe a task to logical expressions in a specific group.
    ///
    /// This method adds a task as a subscriber to a group, ensures there's an exploration
    /// task for the group, and returns all existing expressions for bootstrapping.
    ///
    /// # Parameters
    /// * `group_id` - The ID of the group to subscribe to.
    /// * `subscriber_task_id` - The ID of the task that wants to receive notifications.
    ///
    /// # Returns
    /// A vector of existing logical expressions in the group that the subscriber
    /// can use for initialization.
    pub(super) async fn subscribe_task_to_group(
        &mut self,
        group_id: GroupId,
        subscriber_task_id: TaskId,
    ) -> Result<Vec<LogicalExpressionId>, Error> {
        let subscribers = self.group_subscribers.entry(group_id).or_default();
        if !subscribers.contains(&subscriber_task_id) {
            subscribers.push(subscriber_task_id);
        }

        self.ensure_group_exploration_task(group_id, subscriber_task_id)
            .await?;

        self.memo.get_all_logical_exprs(group_id).await
    }

    /// Subscribe a task to optimized expressions for a specific goal.
    ///
    /// This method adds a task as a subscriber to a goal, ensures there's an exploration
    /// task for the goal, and returns the best existing optimized expression for bootstrapping.
    ///
    /// # Parameters
    /// * `goal_id` - The ID of the goal to subscribe to.
    /// * `subscriber_task_id` - The ID of the task that wants to receive notifications.
    ///
    /// # Returns
    /// The best optimized expression for the goal if one exists, or None if no
    /// optimized expression is available yet.
    pub(super) async fn subscribe_task_to_goal(
        &mut self,
        goal_id: GoalId,
        subscriber_task_id: TaskId,
    ) -> Result<Option<(PhysicalExpressionId, Cost)>, Error> {
        let subscribers = self.goal_subscribers.entry(goal_id).or_default();
        if !subscribers.contains(&subscriber_task_id) {
            subscribers.push(subscriber_task_id);
        }

        self.ensure_goal_exploration_task(goal_id, subscriber_task_id)
            .await?;

        self.memo.get_best_optimized_physical_expr(goal_id).await
    }

    /// Notifies all registered continuations about a new logical expression in a group.
    ///
    /// When a new logical expression is added to a group, we need to inform all continuations
    /// that have been registered for this group.
    ///
    /// # Parameters
    /// * `group_id` - The group containing the new expression
    /// * `expression_id` - The new logical expression
    pub(super) fn notify_all_logical_continuations(
        &mut self,
        group_id: GroupId,
        expression_id: LogicalExpressionId,
    ) {
        // Get all subscribers and schedule continuation jobs for each.
        self.group_subscribers
            .get(&group_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .for_each(|task_id| {
                self.notify_task_logical_continuations(task_id, group_id, expression_id);
            });
    }

    /// Notifies continuations in a specific task about a new logical expression.
    ///
    /// # Parameters
    /// * `task_id` - The task containing the continuations to notify
    /// * `group_id` - The group containing the new expression
    /// * `expression_id` - The new logical expression
    pub(super) fn notify_task_logical_continuations(
        &mut self,
        task_id: TaskId,
        group_id: GroupId,
        expression_id: LogicalExpressionId,
    ) {
        // Collect all jobs to schedule and register their launch.
        let task = self.tasks.get_mut(&task_id).unwrap();

        let jobs = match &mut task.kind {
            TransformExpression(transform_task) => transform_task
                .group_continuations
                .get(&group_id)
                .map(|cont_ids| {
                    cont_ids
                        .iter()
                        .filter_map(|&cont_id| {
                            transform_task
                                .launched_continuations
                                .get_mut(&cont_id)
                                .unwrap()
                                .push(expression_id);

                            transform_task
                                .continuations
                                .get(&cont_id)
                                .map(|cont| ContinueWithLogical(expression_id, cont.clone()))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            ImplementExpression(implement_task) => implement_task
                .group_continuations
                .get(&group_id)
                .map(|cont_ids| {
                    cont_ids
                        .iter()
                        .filter_map(|&cont_id| {
                            implement_task
                                .launched_continuations
                                .get_mut(&cont_id)
                                .unwrap()
                                .push(expression_id);

                            implement_task
                                .continuations
                                .get(&cont_id)
                                .map(|cont| ContinueWithLogical(expression_id, cont.clone()))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            _ => Vec::default(),
        };

        // Schedule all jobs.
        jobs.into_iter().for_each(|job| {
            self.schedule_job(task_id, job);
        });
    }

    /// Notifies all registered continuations about a new best physical expression for a goal.
    ///
    /// When a better physical implementation is found for a goal, we need to inform
    /// all registered continuations to ensure the optimizer converges to the best overall plan.
    ///
    /// # Parameters
    /// * `goal_id` - The goal with the improved implementation
    /// * `expression_id` - The new physical expression
    /// * `cost` - The associated cost
    pub(super) fn notify_all_physical_continuations(
        &mut self,
        goal_id: GoalId,
        expression_id: PhysicalExpressionId,
        cost: Cost,
    ) {
        // Get all subscribers and schedule continuation jobs for each.
        self.goal_subscribers
            .get(&goal_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .for_each(|task_id| {
                self.notify_task_physical_continuations(task_id, goal_id, expression_id, cost);
            });
    }

    /// Notifies continuations in a specific task about a new best physical expression.
    ///
    /// # Parameters
    /// * `task_id` - The task containing the continuations to notify
    /// * `goal_id` - The goal with the improved implementation
    /// * `expression_id` - The new physical expression
    /// * `cost` - The associated cost
    pub(super) fn notify_task_physical_continuations(
        &mut self,
        task_id: TaskId,
        goal_id: GoalId,
        expression_id: PhysicalExpressionId,
        cost: Cost,
    ) {
        // Collect all jobs to schedule and register their launch.
        let task = self.tasks.get_mut(&task_id).unwrap();
        let jobs = if let CostExpression(cost_task) = &mut task.kind {
            cost_task
                .goal_continuations
                .get(&goal_id)
                .map(|cont_ids| {
                    cont_ids
                        .iter()
                        .filter_map(|&cont_id| {
                            cost_task
                                .launched_continuations
                                .get_mut(&cont_id)
                                .unwrap()
                                .push(expression_id);
                            cost_task.continuations.get(&cont_id).map(|cont| {
                                ContinueWithCostedPhysical(expression_id, cost, cont.clone())
                            })
                        })
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::default()
        };

        // Schedule all jobs.
        jobs.into_iter().for_each(|job| {
            self.schedule_job(task_id, job);
        });
    }

    /// Egests and sends optimized plans to optimize plan task subscribers.
    ///
    /// This method converts the optimized expression to a physical plan and
    /// sends it to any optimize plan tasks that are waiting for results.
    ///
    /// # Parameters
    /// * `goal_id` - The ID of the goal that has a new best expression.
    /// * `expression_id` - The ID of the optimized expression to egest as a physical plan.
    pub(super) async fn egest_to_subscribers(
        &mut self,
        goal_id: GoalId,
        expression_id: PhysicalExpressionId,
    ) -> Result<(), Error> {
        // Find all optimize plan tasks that are subscribed to this root goal.
        let send_channels: Vec<_> = self
            .goal_subscribers
            .get(&goal_id)
            .into_iter()
            .flatten()
            .filter_map(|task_id| {
                self.tasks.get(task_id).and_then(|task| {
                    if let OptimizePlan(plan_task) = &task.kind {
                        Some(plan_task.response_tx.clone())
                    } else {
                        None
                    }
                })
            })
            .collect();

        // If we have any optimize plan tasks, egest the plan and send it
        // without blocking the optimizer.
        if !send_channels.is_empty() {
            let physical_plan = self.egest_best_plan(expression_id).await?.unwrap();

            for mut response_tx in send_channels {
                let plan_clone = physical_plan.clone();
                tokio::spawn(async move {
                    response_tx
                        .send(plan_clone)
                        .await
                        .expect("Failed to send plan - channel closed.");
                });
            }
        }

        Ok(())
    }
}
