use futures::{SinkExt, channel::mpsc};

use crate::{
    cir::{Goal, LogicalPlan, PhysicalPlan, PhysicalProperties},
    error::Error,
    memo::Memoize,
    optimizer::{Optimizer, tasks::SourceTaskId},
};

use super::{Task, TaskId};

/// Task data for optimizing a logical plan.
#[derive(Debug)]
pub struct OptimizePlanTask {
    /// The logical plan to be optimized.
    pub logical_plan: LogicalPlan,

    /// Channel to send the optimized physical plan back to the caller.
    pub physical_plan_tx: mpsc::Sender<PhysicalPlan>,

    /// The only dependency to get the best plans from.
    pub optimize_goal_in: TaskId,
}

impl OptimizePlanTask {
    pub fn new(
        logical_plan: LogicalPlan,
        physical_plan_tx: mpsc::Sender<PhysicalPlan>,
        optimize_goal_in: TaskId,
    ) -> Self {
        Self {
            logical_plan,
            physical_plan_tx,
            optimize_goal_in,
        }
    }
}

impl<M: Memoize> Optimizer<M> {
    pub async fn create_optimize_plan_task(
        &mut self,
        logical_plan: LogicalPlan,
        physical_plan_tx: mpsc::Sender<PhysicalPlan>,
    ) -> Result<TaskId, Error> {
        let task_id = self.next_task_id();

        let group_id = self
            .ingest_logical_plan(&logical_plan.clone().into())
            .await?;
        let goal_id = self
            .memo
            .get_goal_id(&Goal(group_id, PhysicalProperties(None)))
            .await?;

        let (optimize_goal_in, best_costed) = self
            .ensure_optimize_goal_task(goal_id, SourceTaskId::OptimizePlan(task_id))
            .await?;

        let mut task = OptimizePlanTask::new(logical_plan, physical_plan_tx, optimize_goal_in);

        if let Some((physical_expr_id, _)) = best_costed {
            // TODO: return value of egest_best_plan is always `Some(plan)` because we already have it costed.
            let physical_plan = self.egest_best_plan(physical_expr_id).await?.unwrap();
            let _ = task.physical_plan_tx.send(physical_plan).await;
        }

        self.tasks.insert(task_id, Task::OptimizePlan(task));
        Ok(task_id)
    }
}
