use futures::{SinkExt, channel::mpsc};

use crate::{
    core::{cir::{Cost, Goal, LogicalPlan, PhysicalExpressionId, PhysicalPlan, PhysicalProperties}, error::Error, optimizer::{tasks::SourceTaskId, Optimizer}},
    memo::Memoize,
};

use super::{Task, TaskId};

/// Task data for optimizing a logical plan.
#[derive(Debug)]
pub struct OptimizePlanTask {
    /// The logical plan to be optimized.
    pub logical_plan: LogicalPlan,

    /// Channel to send the optimized physical plan back to the caller.
    pub physical_plan_tx: mpsc::Sender<(PhysicalPlan, Cost)>,

    /// The only dependency to get the best plans from.
    pub optimize_goal_in: TaskId,
}

impl OptimizePlanTask {
    pub fn new(
        logical_plan: LogicalPlan,
        physical_plan_tx: mpsc::Sender<(PhysicalPlan, Cost)>,
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
        mut physical_plan_tx: mpsc::Sender<(PhysicalPlan, Cost)>,
        cost: Cost,
        physical_expr_id: PhysicalExpressionId,
    ) -> Result<(), Error> {
        let physical_plan = self.egest_best_plan(physical_expr_id).await?.unwrap();
        tokio::spawn(async move {
            physical_plan_tx.send((physical_plan, cost)).await.unwrap();
        });
        Ok(())
    }

    pub async fn create_optimize_plan_task(
        &mut self,
        logical_plan: LogicalPlan,
        physical_plan_tx: mpsc::Sender<(PhysicalPlan, Cost)>,
    ) -> Result<TaskId, Error> {
        let task_id = self.next_task_id();
        println!("Creating optimize plan task");
        let group_id = self
            .ingest_logical_plan(&logical_plan.clone().into())
            .await?;
        println!("Ingested logical plan {:?}", group_id);
        let goal_id = self
            .memo
            .get_goal_id(&Goal(group_id, PhysicalProperties(None)))
            .await?;
        println!("Got goal id {:?}", goal_id);
        let (optimize_goal_in, best_costed) = self
            .ensure_optimize_goal_task(goal_id, SourceTaskId::OptimizePlan(task_id))
            .await?;
        println!(
            "Ensured optimize goal task {:?}, {:?}",
            optimize_goal_in, best_costed
        );
        let task = OptimizePlanTask::new(logical_plan, physical_plan_tx, optimize_goal_in);
        println!("Created optimize plan task");
        if let Some((physical_expr_id, cost)) = best_costed {
            println!("Emitting best physical plan for {:?}, cost {:?}", physical_expr_id, cost);
            self.emit_best_physical_plan(task.physical_plan_tx.clone(), cost, physical_expr_id)
                .await?;
        }

        self.tasks.insert(task_id, Task::OptimizePlan(task));
        println!("Inserted optimize plan task {:?}", task_id);
        Ok(task_id)
    }
}
