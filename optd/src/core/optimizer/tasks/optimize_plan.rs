use futures::{SinkExt, channel::mpsc};

use crate::{
    core::cir::{Goal, LogicalPlan, PhysicalExpressionId, PhysicalPlan, PhysicalProperties},
    core::error::Error,
    core::memo::Memoize,
    core::optimizer::{Optimizer, tasks::SourceTaskId},
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
    pub async fn emit_best_physical_plan(
        &mut self,
        mut physical_plan_tx: mpsc::Sender<PhysicalPlan>,
        physical_expr_id: PhysicalExpressionId,
    ) -> Result<(), Error> {
        let physical_plan = self.egest_best_plan(physical_expr_id).await?.unwrap();
        tokio::spawn(async move {
            physical_plan_tx.send(physical_plan).await.unwrap();
        });
        Ok(())
    }

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

        let task = OptimizePlanTask::new(logical_plan, physical_plan_tx, optimize_goal_in);

        if let Some((physical_expr_id, _)) = best_costed {
            self.emit_best_physical_plan(task.physical_plan_tx.clone(), physical_expr_id)
                .await?;
        }

        self.tasks.insert(task_id, Task::OptimizePlan(task));
        Ok(task_id)
    }
}
