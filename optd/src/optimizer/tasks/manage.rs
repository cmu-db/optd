use super::{
    ContinueWithLogicalTask, ExploreGroupTask, ForkLogicalTask, ImplementExpressionTask,
    OptimizeGoalTask, OptimizePlanTask, Task, TaskId, TransformExpressionTask,
};
use crate::{memo::Memo, optimizer::Optimizer};

impl<M: Memo> Optimizer<M> {
    /// The next task ID to be assigned.
    pub(crate) fn next_task_id(&mut self) -> TaskId {
        let task_id = self.next_task_id;
        self.next_task_id.0 += 1;
        task_id
    }

    /// Get a reference to a task by its ID.
    pub(crate) fn get_task(&self, task_id: TaskId) -> Option<&Task> {
        self.tasks.get(&task_id)
    }

    /// Get a mutable reference to a task by its ID.
    pub(crate) fn get_task_mut(&mut self, task_id: TaskId) -> Option<&mut Task> {
        self.tasks.get_mut(&task_id)
    }

    /// Add a new task.
    pub(crate) fn add_task(&mut self, task_id: TaskId, task: Task) {
        self.tasks.insert(task_id, task);
    }

    /// Remove a task.
    pub(crate) fn remove_task(&mut self, task_id: TaskId) -> Option<Task> {
        self.tasks.remove(&task_id)
    }

    // Direct task type access by ID - immutable versions

    /// Get a task as an OptimizePlanTask by its ID.
    pub(crate) fn get_optimize_plan_task(&self, task_id: TaskId) -> Option<&OptimizePlanTask> {
        self.get_task(task_id).and_then(|task| match &task {
            Task::OptimizePlan(task) => Some(task),
            _ => None,
        })
    }

    /// Get a task as an OptimizeGoalTask by its ID.
    pub(crate) fn get_optimize_goal_task(&self, task_id: TaskId) -> Option<&OptimizeGoalTask> {
        self.get_task(task_id).and_then(|task| match &task {
            Task::OptimizeGoal(task) => Some(task),
            _ => None,
        })
    }

    /// Get a task as an ExploreGroupTask by its ID.
    pub(crate) fn get_explore_group_task(&self, task_id: TaskId) -> Option<&ExploreGroupTask> {
        self.get_task(task_id).and_then(|task| match &task {
            Task::ExploreGroup(task) => Some(task),
            _ => None,
        })
    }

    /// Get a task as an ImplementExpressionTask by its ID.
    pub(crate) fn get_implement_expression_task(
        &self,
        task_id: TaskId,
    ) -> Option<&ImplementExpressionTask> {
        self.get_task(task_id).and_then(|task| match &task {
            Task::ImplementExpression(task) => Some(task),
            _ => None,
        })
    }

    /// Get a task as a TransformExpressionTask by its ID.
    pub(crate) fn get_transform_expression_task(
        &self,
        task_id: TaskId,
    ) -> Option<&TransformExpressionTask> {
        self.get_task(task_id).and_then(|task| match &task {
            Task::TransformExpression(task) => Some(task),
            _ => None,
        })
    }

    /// Get a task as a ForkLogicalTask by its ID.
    pub(crate) fn get_fork_logical_task(&self, task_id: TaskId) -> Option<&ForkLogicalTask> {
        self.get_task(task_id).and_then(|task| match &task {
            Task::ForkLogical(task) => Some(task),
            _ => None,
        })
    }

    /// Get a task as a ContinueWithLogicalTask by its ID.
    pub(crate) fn get_continue_with_logical_task(
        &self,
        task_id: TaskId,
    ) -> Option<&ContinueWithLogicalTask> {
        self.get_task(task_id).and_then(|task| match &task {
            Task::ContinueWithLogical(task) => Some(task),
            _ => None,
        })
    }

    // Direct task type access by ID - mutable versions

    /// Get a mutable task as an OptimizePlanTask by its ID.
    pub(crate) fn get_optimize_plan_task_mut(
        &mut self,
        task_id: TaskId,
    ) -> Option<&mut OptimizePlanTask> {
        self.get_task_mut(task_id).and_then(|task| match task {
            Task::OptimizePlan(task) => Some(task),
            _ => None,
        })
    }

    /// Get a mutable task as an OptimizeGoalTask by its ID.
    pub(crate) fn get_optimize_goal_task_mut(
        &mut self,
        task_id: TaskId,
    ) -> Option<&mut OptimizeGoalTask> {
        self.get_task_mut(task_id).and_then(|task| match task {
            Task::OptimizeGoal(task) => Some(task),
            _ => None,
        })
    }

    /// Get a mutable task as an ExploreGroupTask by its ID.
    pub(crate) fn get_explore_group_task_mut(
        &mut self,
        task_id: TaskId,
    ) -> Option<&mut ExploreGroupTask> {
        self.get_task_mut(task_id).and_then(|task| match task {
            Task::ExploreGroup(task) => Some(task),
            _ => None,
        })
    }

    /// Get a mutable task as an ImplementExpressionTask by its ID.
    pub(crate) fn get_implement_expression_task_mut(
        &mut self,
        task_id: TaskId,
    ) -> Option<&mut ImplementExpressionTask> {
        self.get_task_mut(task_id).and_then(|task| match task {
            Task::ImplementExpression(task) => Some(task),
            _ => None,
        })
    }

    /// Get a mutable task as a TransformExpressionTask by its ID.
    pub(crate) fn get_transform_expression_task_mut(
        &mut self,
        task_id: TaskId,
    ) -> Option<&mut TransformExpressionTask> {
        self.get_task_mut(task_id).and_then(|task| match task {
            Task::TransformExpression(task) => Some(task),
            _ => None,
        })
    }

    /// Get a mutable task as a ForkLogicalTask by its ID.
    pub(crate) fn get_fork_logical_task_mut(
        &mut self,
        task_id: TaskId,
    ) -> Option<&mut ForkLogicalTask> {
        self.get_task_mut(task_id).and_then(|task| match task {
            Task::ForkLogical(task) => Some(task),
            _ => None,
        })
    }

    /// Get a mutable task as a ContinueWithLogicalTask by its ID.
    pub(crate) fn get_continue_with_logical_task_mut(
        &mut self,
        task_id: TaskId,
    ) -> Option<&mut ContinueWithLogicalTask> {
        self.get_task_mut(task_id).and_then(|task| match task {
            Task::ContinueWithLogical(task) => Some(task),
            _ => None,
        })
    }
}
