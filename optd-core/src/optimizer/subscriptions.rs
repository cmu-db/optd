use super::{memo::Memoize, tasks::TaskId, Optimizer};
use crate::cir::{goal::Goal, group::GroupId};

impl<M: Memoize> Optimizer<M> {
    /// Subscribe a task to logical expressions in a specific group
    ///
    /// This method adds a task as a subscriber to a group and establishes
    /// the appropriate exploration task to discover new expressions in that group.
    /// The subscriber task will be notified of all expressions in the group.
    ///
    /// # Parameters
    /// * `group_id` - The ID of the group to subscribe to
    /// * `subscriber_task_id` - The ID of the task that wants to receive notifications
    pub(super) async fn subscribe_task_to_group(
        &mut self,
        group_id: GroupId,
        subscriber_task_id: TaskId,
    ) {
        let group_id = self.group_repr.find(&group_id);

        // Add the task to group subscribers list
        self.group_subscribers
            .entry(group_id)
            .or_default()
            .push(subscriber_task_id);

        // Find or create an exploration task and establish the relationship
        self.ensure_group_exploration_task(group_id, subscriber_task_id)
            .await;
    }

    /// Subscribe a task to optimized expressions for a specific goal
    ///
    /// This method adds a task as a subscriber to a goal and establishes
    /// the appropriate exploration task to discover optimal implementations for that goal.
    /// The subscriber task will be notified of the best optimized expressions for the goal.
    ///
    /// # Parameters
    /// * `goal` - The goal to subscribe to
    /// * `subscriber_task_id` - The ID of the task that wants to receive notifications
    pub(super) async fn subscribe_task_to_goal(&mut self, goal: Goal, subscriber_task_id: TaskId) {
        let goal = self.goal_repr.find(&goal);

        // Add the task to goal subscribers list
        self.goal_subscribers
            .entry(goal.clone())
            .or_default()
            .push(subscriber_task_id);

        // Find or create an exploration task and establish the relationship
        self.ensure_goal_exploration_task(goal.clone(), subscriber_task_id)
            .await;
    }
}
