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

    /// Schedules logical expression continuation jobs for group subscribers.
    ///
    /// This method finds all tasks that have subscribed to the specified group,
    /// collects their logical expression continuations, and schedules continuation
    /// jobs to process the new expression.
    ///
    /// # Parameters
    /// * `group_id` - The ID of the group that has a new expression.
    /// * `expression_id` - The ID of the new logical expression to continue with.
    pub(super) fn schedule_logical_continuations(
        &mut self,
        group_id: GroupId,
        expression_id: LogicalExpressionId,
    ) {
        // Return early if no subscribers.
        let subscribers = match self.group_subscribers.get(&group_id) {
            Some(subs) => subs,
            None => return,
        };

        // Collect all continuation jobs to schedule.
        let continuation_jobs: Vec<_> = subscribers
            .iter()
            .filter_map(|&task_id| {
                self.tasks.get(&task_id).map(|task| {
                    let continuations = match &task.kind {
                        TransformExpression(task) => task.continuations.get(&group_id),
                        ImplementExpression(task) => task.continuations.get(&group_id),
                        _ => None,
                    };

                    (task_id, continuations)
                })
            })
            .flat_map(|(task_id, continuations)| {
                continuations
                    .map(|conts| {
                        conts.iter().map(move |cont| {
                            (task_id, ContinueWithLogical(expression_id, cont.clone()))
                        })
                    })
                    .into_iter()
                    .flatten()
            })
            .collect();

        // Schedule all collected jobs.
        for (task_id, job) in continuation_jobs {
            self.schedule_job(task_id, job);
        }
    }

    /// Schedules optimized expression continuation jobs for goal subscribers.
    ///
    /// This method finds all tasks that have subscribed to the specified goal,
    /// collects their optimized expression continuations, and schedules continuation
    /// jobs to process the new optimized expression.
    ///
    /// # Parameters
    /// * `goal_id` - The ID of the goal that has a new best expression.
    /// * `expression_id` - The ID of the new physical expression to continue with.
    /// * `cost` - The corresponding cost.
    pub(super) fn schedule_optimized_continuations(
        &mut self,
        goal_id: GoalId,
        expression_id: PhysicalExpressionId,
        cost: Cost,
    ) {
        // Return early if no subscribers.
        let subscribers = match self.goal_subscribers.get(&goal_id) {
            Some(subs) => subs,
            None => return,
        };

        // Collect all continuation jobs to schedule.
        let continuation_jobs: Vec<_> = subscribers
            .iter()
            .filter_map(|&task_id| {
                self.tasks.get(&task_id).and_then(|task| match &task.kind {
                    CostExpression(cost_task) => cost_task
                        .continuations
                        .get(&goal_id)
                        .map(|conts| (task_id, conts)),
                    _ => None,
                })
            })
            .flat_map(|(task_id, conts)| {
                conts.iter().map(move |cont| {
                    (
                        task_id,
                        ContinueWithCostedPhysical(expression_id, cost, cont.clone()),
                    )
                })
            })
            .collect();

        // Schedule all collected jobs.
        for (task_id, job) in continuation_jobs {
            self.schedule_job(task_id, job);
        }
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
