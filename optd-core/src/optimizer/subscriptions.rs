use super::{
    jobs::{JobId, JobKind},
    memo::Memoize,
    tasks::{
        CostExpressionTask, ImplementExpressionTask, OptimizePlanTask, Task, TaskId, TaskKind,
        TransformExpressionTask,
    },
    Optimizer,
};
use crate::cir::{
    expressions::{LogicalExpression, OptimizedExpression},
    goal::Goal,
    group::GroupId,
};
use JobKind::*;
use TaskKind::*;

impl<M: Memoize> Optimizer<M> {
    /// Subscribe a task to logical expressions in a specific group
    ///
    /// This method adds a task as a subscriber to a group, ensures there's an exploration
    /// task for the group, and notifies the task about all existing expressions.
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

        // Bootstrap the subscriber with existing expressions
        let expressions = self
            .memo
            .get_all_logical_exprs(group_id)
            .await
            .expect("Failed to get logical expressions for group");

        for expr in expressions {
            // Clone the task first to avoid borrow issues
            let task = self.tasks.get(&subscriber_task_id)
                .expect("Task must exist when bootstrapping with expressions");
                
            // Handle based on task type
            match &task.kind {
                // Handle expression for ExploreGoal tasks
                ExploreGoal(goal) if goal.0 == group_id => {
                    // Create implementation tasks for all rules
                    let rules = self.rule_book.get_implementations().to_vec();
                    for rule in rules {
                        self.launch_implement_expression_task(rule, expr.clone());
                    }
                }

                // Handle expression for ExploreGroup tasks
                ExploreGroup(task_group_id) if *task_group_id == group_id => {
                    // Create transformation tasks for all rules
                    let rules = self.rule_book.get_transformations().to_vec();
                    for rule in rules {
                        self.launch_transform_expression_task(rule, expr.clone());
                    }
                }

                // Handle expression for ImplementExpression tasks
                ImplementExpression(implement_task) => {
                    // Check if this group has continuations
                    if let Some(continuations) = implement_task.conts.get(&group_id) {
                        // Create jobs for all continuations
                        for continuation in continuations {
                            self.schedule_job(
                                subscriber_task_id,
                                ContinueWithLogical(expr.clone(), continuation.clone()),
                            );
                        }
                    }
                }

                // Handle expression for TransformExpression tasks
                TransformExpression(transform_task) => {
                    // Check if this group has continuations
                    if let Some(continuations) = transform_task.conts.get(&group_id) {
                        // Create jobs for all continuations
                        for continuation in continuations {
                            self.schedule_job(
                                subscriber_task_id,
                                ContinueWithLogical(expr.clone(), continuation.clone()),
                            );
                        }
                    }
                }

                // For other task types, ignore the expression
                _ => {}
            }
        }
    }

    /// Subscribe a task to optimized expressions for a specific goal
    ///
    /// This method adds a task as a subscriber to a goal, ensures there's an exploration
    /// task for the goal, and notifies the task about the best existing optimized expression.
    ///
    /// # Parameters
    /// * `goal` - The goal to subscribe to
    /// * `subscriber_task_id` - The ID of the task that wants to receive notifications
    pub(super) async fn subscribe_task_to_goal(&mut self, goal: Goal, subscriber_task_id: TaskId) {
        let goal = self.goal_repr.find(&goal);
        
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
        self.ensure_goal_exploration_task(goal.clone(), subscriber_task_id)
            .await;

        // Bootstrap the subscriber with the best expression
        if let Some(expr) = self
            .memo
            .get_best_optimized_physical_expr(&goal)
            .await
            .expect("Failed to get best optimized physical expression")
        {
            // Clone the task first to avoid borrow issues
            let task = self.tasks.get(&subscriber_task_id)
                .expect("Task must exist when bootstrapping with expressions");
                
            // Handle based on task type
            match &task.kind {
                // Handle expression for OptimizePlan tasks
                OptimizePlan(optimize_task) => {
                    // This would egest the plan and send to response channel
                    // TODO: Implement egest
                }

                // Handle expression for CostExpression tasks
                CostExpression(cost_task) => {
                    // Check if this goal has continuations
                    if let Some(continuations) = &cost_task.conts.get(&goal) {
                        // Create jobs for all continuations
                        for continuation in continuations {
                            self.schedule_job(
                                subscriber_task_id,
                                ContinueWithOptimized(expr.0.clone(), continuation.clone()),
                            );
                        }
                    }
                }

                // For other task types, ignore the expression
                _ => {}
            }
        }
    }

    /// Notify subscribers about a new logical expression in a group.
    ///
    /// This method notifies all subscriber tasks about a new logical expression.
    ///
    /// # Parameters
    /// * `group_id` - The ID of the group that has a new expression
    /// * `expr` - The new logical expression
    pub(super) fn notify_group_subscribers(&mut self, group_id: GroupId, expr: &LogicalExpression) {
        let group_id = self.group_repr.find(&group_id);

        // Get all subscriber tasks for this group
        let subscriber_tasks = match self.group_subscribers.get(&group_id) {
            Some(tasks) => tasks.clone(),
            None => return, // No subscribers
        };

        for task_id in subscriber_tasks {
            // Clone the task first to avoid borrow issues
            let task = match self.tasks.get(&task_id) {
                Some(task) => task,
                None => continue, // Task may have been removed
            };
            
            // Handle based on task type
            match &task.kind {
                // Handle expression for ExploreGoal tasks
                ExploreGoal(goal) if goal.0 == group_id => {
                    // Create implementation tasks for all rules
                    let rules = self.rule_book.get_implementations().to_vec();
                    for rule in rules {
                        self.launch_implement_expression_task(rule, expr.clone());
                    }
                }

                // Handle expression for ExploreGroup tasks
                ExploreGroup(task_group_id) if *task_group_id == group_id => {
                    // Create transformation tasks for all rules
                    let rules = self.rule_book.get_transformations().to_vec();
                    for rule in rules {
                        self.launch_transform_expression_task(rule, expr.clone());
                    }
                }

                // Handle expression for ImplementExpression tasks
                ImplementExpression(implement_task) => {
                    // Check if this group has continuations
                    if let Some(continuations) = implement_task.conts.get(&group_id) {
                        // Create jobs for all continuations
                        for continuation in continuations {
                            self.schedule_job(
                                task_id,
                                ContinueWithLogical(expr.clone(), continuation.clone()),
                            );
                        }
                    }
                }

                // Handle expression for TransformExpression tasks
                TransformExpression(transform_task) => {
                    // Check if this group has continuations
                    if let Some(continuations) = transform_task.conts.get(&group_id) {
                        // Create jobs for all continuations
                        for continuation in continuations {
                            self.schedule_job(
                                task_id,
                                ContinueWithLogical(expr.clone(), continuation.clone()),
                            );
                        }
                    }
                }

                // For other task types, ignore the expression
                _ => {}
            }
        }
    }

    /// Notify subscribers about a new optimized expression for a goal.
    ///
    /// This method notifies all subscriber tasks about a new optimized expression.
    ///
    /// # Parameters
    /// * `goal` - The goal that has a new optimized expression
    /// * `expr` - The new optimized expression
    pub(super) fn notify_goal_subscribers(&mut self, goal: Goal, expr: &OptimizedExpression) {
        let goal = self.goal_repr.find(&goal);

        // Get all subscriber tasks for this goal
        let subscriber_tasks = match self.goal_subscribers.get(&goal) {
            Some(tasks) => tasks.clone(),
            None => return, // No subscribers
        };

        for task_id in subscriber_tasks {
            // Clone the task first to avoid borrow issues
            let task = match self.tasks.get(&task_id) {
                Some(task) => task,
                None => continue, // Task may have been removed
            };
            
            // Handle based on task type
            match &task.kind {
                // Handle expression for OptimizePlan tasks
                OptimizePlan(optimize_task) => {
                    // This would egest the plan and send to response channel
                    // TODO: Implement egest
                }

                // Handle expression for CostExpression tasks
                CostExpression(cost_task) => {
                    // Check if this goal has continuations
                    if let Some(continuations) = cost_task.conts.get(&goal) {
                        // Create jobs for all continuations
                        for continuation in continuations {
                            self.schedule_job(
                                task_id,
                                ContinueWithOptimized(expr.0.clone(), continuation.clone()),
                            );
                        }
                    }
                }

                // For other task types, ignore the expression
                _ => {}
            }
        }
    }

    /// Process a group merge
    ///
    /// Updates subscriptions and notifies subscribers about new expressions when groups are merged.
    ///
    /// # Parameters
    /// * `prev_group_id` - The old group ID that was merged
    /// * `new_group_id` - The new representative group ID
    /// * `expressions` - New expressions added to the group during merge
    pub(super) fn process_group_merge(
        &mut self,
        prev_group_id: GroupId,
        new_group_id: GroupId,
        expressions: &[LogicalExpression],
    ) {
        // Get subscribers of the old group
        let old_subscribers = self.group_subscribers.remove(&prev_group_id).unwrap_or_default();
        
        if !old_subscribers.is_empty() {
            // Add them to the new group's subscribers, avoiding duplicates
            let current_subscribers = self.group_subscribers.entry(new_group_id).or_default();
            
            // Keep track of new subscribers
            let mut new_subscribers = Vec::new();

            for subscriber in old_subscribers {
                if !current_subscribers.contains(&subscriber) {
                    current_subscribers.push(subscriber);
                    new_subscribers.push(subscriber);
                }
            }
            
            // Notify the new subscribers about the expressions
            for expr in expressions {
                for subscriber_id in &new_subscribers {
                    // Clone the task first to avoid borrow issues
                    let task = match self.tasks.get(&subscriber_id) {
                        Some(task) => task,
                        None => continue, // Task may have been removed
                    };
                    
                    // Handle based on task type
                    match &task.kind {
                        // Handle expression for ExploreGoal tasks
                        ExploreGoal(goal) if goal.0 == new_group_id => {
                            // Create implementation tasks for all rules
                            let rules = self.rule_book.get_implementations().to_vec();
                            for rule in rules {
                                self.launch_implement_expression_task(rule, expr.clone());
                            }
                        }

                        // Handle expression for ExploreGroup tasks
                        ExploreGroup(task_group_id) if *task_group_id == new_group_id => {
                            // Create transformation tasks for all rules
                            let rules = self.rule_book.get_transformations().to_vec();
                            for rule in rules {
                                self.launch_transform_expression_task(rule, expr.clone());
                            }
                        }

                        // Handle expression for ImplementExpression tasks
                        ImplementExpression(implement_task) => {
                            // Check if this group has continuations
                            if let Some(continuations) = implement_task.conts.get(&new_group_id) {
                                // Create jobs for all continuations
                                for continuation in continuations {
                                    self.schedule_job(
                                        *subscriber_id,
                                        ContinueWithLogical(expr.clone(), continuation.clone()),
                                    );
                                }
                            }
                        }

                        // Handle expression for TransformExpression tasks
                        TransformExpression(transform_task) => {
                            // Check if this group has continuations
                            if let Some(continuations) = transform_task.conts.get(&new_group_id) {
                                // Create jobs for all continuations
                                for continuation in continuations {
                                    self.schedule_job(
                                        *subscriber_id,
                                        ContinueWithLogical(expr.clone(), continuation.clone()),
                                    );
                                }
                            }
                        }

                        // For other task types, ignore the expression
                        _ => {}
                    }
                }
            }
        }
    }

    /// Process a goal merge
    ///
    /// Updates subscriptions and notifies subscribers about new expressions when goals are merged.
    ///
    /// # Parameters
    /// * `prev_goal` - The old goal that was merged
    /// * `new_goal` - The new representative goal
    /// * `expression` - New optimized expression added during merge, if any
    pub(super) fn process_goal_merge(
        &mut self,
        prev_goal: Goal,
        new_goal: Goal,
        expression: Option<&OptimizedExpression>,
    ) {
        // Get subscribers of the old goal
        let old_subscribers = self.goal_subscribers.remove(&prev_goal).unwrap_or_default();
        
        if !old_subscribers.is_empty() && expression.is_some() {
            let expr = expression.unwrap();
            
            // Add them to the new goal's subscribers, avoiding duplicates
            let current_subscribers = self.goal_subscribers.entry(new_goal.clone()).or_default();
            
            // Keep track of new subscribers
            let mut new_subscribers = Vec::new();

            for subscriber in old_subscribers {
                if !current_subscribers.contains(&subscriber) {
                    current_subscribers.push(subscriber);
                    new_subscribers.push(subscriber);
                }
            }
            
            // Notify the new subscribers about the expression
            for subscriber_id in &new_subscribers {
                // Clone the task first to avoid borrow issues
                let task = match self.tasks.get(&subscriber_id) {
                    Some(task) => task,
                    None => continue, // Task may have been removed
                };
                
                // Handle based on task type
                match &task.kind {
                    // Handle expression for OptimizePlan tasks
                    OptimizePlan(optimize_task) => {
                        // This would egest the plan and send to response channel
                        // TODO: Implement egest
                    }

                    // Handle expression for CostExpression tasks
                    CostExpression(cost_task) => {
                        // Check if this goal has continuations
                        if let Some(continuations) = cost_task.conts.get(&new_goal) {
                            // Create jobs for all continuations
                            for continuation in continuations {
                                self.schedule_job(
                                    *subscriber_id,
                                    ContinueWithOptimized(expr.0.clone(), continuation.clone()),
                                );
                            }
                        }
                    }

                    // For other task types, ignore the expression
                    _ => {}
                }
            }
        } else if !old_subscribers.is_empty() {
            // Just merge the subscribers without notification
            let current_subscribers = self.goal_subscribers.entry(new_goal).or_default();
            
            for subscriber in old_subscribers {
                if !current_subscribers.contains(&subscriber) {
                    current_subscribers.push(subscriber);
                }
            }
        }
    }

    /// Remove a task from all group subscribers lists
    ///
    /// This method is called when a task is completed or canceled,
    /// to clean up its subscriptions.
    ///
    /// # Parameters
    /// * `task_id` - The ID of the task to unsubscribe
    pub(super) fn unsubscribe_task_from_all_groups(&mut self, task_id: TaskId) {
        for subscribers in self.group_subscribers.values_mut() {
            subscribers.retain(|&id| id != task_id);
        }

        // Clean up empty subscriber lists
        self.group_subscribers
            .retain(|_, subscribers| !subscribers.is_empty());
    }

    /// Remove a task from all goal subscribers lists
    ///
    /// This method is called when a task is completed or canceled,
    /// to clean up its subscriptions.
    ///
    /// # Parameters
    /// * `task_id` - The ID of the task to unsubscribe
    pub(super) fn unsubscribe_task_from_all_goals(&mut self, task_id: TaskId) {
        for subscribers in self.goal_subscribers.values_mut() {
            subscribers.retain(|&id| id != task_id);
        }

        // Clean up empty subscriber lists
        self.goal_subscribers
            .retain(|_, subscribers| !subscribers.is_empty());
    }
}