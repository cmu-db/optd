use super::{Optimizer, jobs::JobKind, tasks_old::TaskId};
use crate::{
    cir::{Cost, GoalId, GroupId, LogicalExpressionId, PhysicalExpressionId},
    error::Error,
    memo::Memoize,
};
use JobKind::*;
use futures::SinkExt;

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

        self.ensure_goal_optimize_task(goal_id, subscriber_task_id)
            .await?;

        self.memo.get_best_optimized_physical_expr(goal_id).await
    }

    /// Schedules logical expression continuation jobs for group subscribers.
    ///
    /// # Parameters
    /// * `group_id` - The ID of the group that has new expressions.
    /// * `expression_ids` - A slice of logical expression IDs to continue with.
    pub(super) fn schedule_logical_continuations(
        &mut self,
        group_id: GroupId,
        expression_ids: &[LogicalExpressionId],
    ) {
        // Skip processing if there are no expressions or subscribers.
        let Some(subscribers) = self.group_subscribers.get(&group_id) else {
            return;
        };

        let all_continuation_jobs: Vec<_> = subscribers
            .iter()
            .filter_map(|&task_id| {
                self.tasks.get(&task_id).and_then(|task| {
                    let continuations = match &task.kind {
                        TransformExpression(task) => task.continuations.get(&group_id),
                        ImplementExpression(task) => task.continuations.get(&group_id),
                        _ => None,
                    };
                    continuations.map(|conts| (task_id, conts))
                })
            })
            .flat_map(|(task_id, conts)| {
                expression_ids.iter().flat_map(move |&expr_id| {
                    conts.iter().map(move |cont| {
                        (task_id, JobKind::ContinueWithLogical(expr_id, cont.clone()))
                    })
                })
            })
            .collect();

        for (task_id, job) in all_continuation_jobs {
            self.schedule_job(task_id, job);
        }
    }

    /// Schedules optimized expression continuation jobs for goal subscribers.
    ///
    /// # Parameters
    /// * `goal_id` - The ID of the goal that has new best expressions.
    /// * `expression_id` - The ID of the optimized expression to continue with.
    /// * `cost` - The cost of the optimized expression.
    pub(super) fn schedule_optimized_continuations(
        &mut self,
        goal_id: GoalId,
        expression_id: PhysicalExpressionId,
        cost: Cost,
    ) {
        // Skip processing if there are no expressions or subscribers.
        let Some(subscribers) = self.goal_subscribers.get(&goal_id) else {
            return;
        };

        let all_continuation_jobs: Vec<_> = subscribers
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

        // Schedule all collected jobs in batch.
        for (task_id, job) in all_continuation_jobs {
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
