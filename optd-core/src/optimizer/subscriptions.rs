use super::{memo::Memoize, tasks::TaskId, Optimizer};
use crate::cir::{
    expressions::{LogicalExpression, OptimizedExpression},
    goal::Goal,
    group::GroupId,
};

impl<M: Memoize> Optimizer<M> {
    /// Subscribe a task to logical expressions in a specific group
    ///
    /// This method adds a task as a subscriber to a group, ensures there's an exploration
    /// task for the group, and returns all existing expressions for bootstrapping.
    ///
    /// # Parameters
    /// * `group_id` - The ID of the group to subscribe to
    /// * `subscriber_task_id` - The ID of the task that wants to receive notifications
    ///
    /// # Returns
    /// A vector of existing logical expressions in the group that the subscriber
    /// can use for initialization
    pub(super) async fn subscribe_task_to_group(
        &mut self,
        group_id: GroupId,
        subscriber_task_id: TaskId,
    ) -> Vec<LogicalExpression> {
        // Add the task to group subscribers list if not already there
        if !self
            .group_subscribers
            .entry(group_id)
            .or_default()
            .contains(&subscriber_task_id)
        {
            self.group_subscribers
                .entry(group_id)
                .or_default()
                .push(subscriber_task_id);
        }

        // Ensure there's a group exploration task
        self.ensure_group_exploration_task(group_id, subscriber_task_id)
            .await;

        // Return existing expressions for bootstrapping
        self.memo
            .get_all_logical_exprs(group_id)
            .await
            .expect("Failed to get logical expressions for group")
    }

    /// Subscribe a task to optimized expressions for a specific goal
    ///
    /// This method adds a task as a subscriber to a goal, ensures there's an exploration
    /// task for the goal, and returns the best existing optimized expression for bootstrapping.
    ///
    /// # Parameters
    /// * `goal` - The goal to subscribe to
    /// * `subscriber_task_id` - The ID of the task that wants to receive notifications
    ///
    /// # Returns
    /// The best optimized expression for the goal if one exists, or None if no
    /// optimized expression is available yet
    pub(super) async fn subscribe_task_to_goal(
        &mut self,
        goal: Goal,
        subscriber_task_id: TaskId,
    ) -> Option<OptimizedExpression> {
        // Add the task to goal subscribers list if not already there
        if !self
            .goal_subscribers
            .entry(goal.clone())
            .or_default()
            .contains(&subscriber_task_id)
        {
            self.goal_subscribers
                .entry(goal.clone())
                .or_default()
                .push(subscriber_task_id);
        }

        // Ensure there's a goal exploration task
        self.ensure_goal_exploration_task(&goal, subscriber_task_id)
            .await;

        // Return best expression for bootstrapping
        self.memo
            .get_best_optimized_physical_expr(&goal)
            .await
            .expect("Failed to get best optimized physical expression")
    }
}
