use std::sync::Arc;

use optd_dsl::engine::Engine;

use crate::{
    bridge::{from_cir::partial_physical_to_value, into_cir::value_to_cost},
    cir::{Cost, PhysicalExpressionId},
    error::Error,
    memo::{Memoize, Status},
    optimizer::{EngineMessageKind, Job, JobId, Optimizer, Task},
};

use super::TaskId;

/// Task data for costing a physical expression.
#[derive(Debug)]
pub(super) struct CostExpressionTask {
    /// The physical expression to cost.
    pub physical_expr_id: PhysicalExpressionId,

    /// Whether the task has started the cost estimation.
    pub is_processing: bool,

    pub budget: Cost,

    pub optimize_goal_out: Vec<TaskId>,

    pub fork_in: Option<TaskId>,
}

impl CostExpressionTask {
    /// Creates a new `CostExpressionTask`.
    pub fn new(
        physical_expr_id: PhysicalExpressionId,
        is_processing: bool,
        budget: Cost,
        out: TaskId,
    ) -> Self {
        let mut task = Self {
            physical_expr_id,
            is_processing,
            budget,
            optimize_goal_out: Vec::new(),
            fork_in: None,
        };
        task.add_subscriber(out);
        task
    }

    pub fn add_subscriber(&mut self, out: TaskId) {
        self.optimize_goal_out.push(out);
    }

    /// Adds a `ForkLogical` task as a dependency.
    pub fn add_fork_in(&mut self, task_id: TaskId) {
        self.fork_in = Some(task_id);
    }
}

impl<M: Memoize> Optimizer<M> {
    /// Ensures a cost expression task exists and sets up a parent-child relationship.
    ///
    /// This is used when a task needs to cost a physical expression as part of its work.
    /// If a costing task already exists, we reuse it.
    pub async fn ensure_cost_expression_task(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        budget: Cost,
        optimize_goal_out: TaskId,
    ) -> Result<TaskId, Error> {
        if let Some(task_id) = self.cost_expression_task_index.get(&physical_expr_id) {
            // If cost expression task already exists, we register the subscriber and reuse.
            let cost_task = self
                .tasks
                .get_mut(task_id)
                .unwrap()
                .as_cost_expression_mut();
            cost_task.add_subscriber(optimize_goal_out);
            Ok(*task_id)
        } else {
            let task_id = self.next_task_id();
            let is_dirty = self.memo.get_cost_status(physical_expr_id).await? == Status::Dirty;
            let task =
                CostExpressionTask::new(physical_expr_id, is_dirty, budget, optimize_goal_out);

            self.tasks.insert(task_id, Task::CostExpression(task));
            self.cost_expression_task_index
                .insert(physical_expr_id, task_id);

            if is_dirty {
                self.schedule_job(Job::Task(task_id));
            }
            Ok(task_id)
        }
    }

    /// Executes a job to compute the cost of a physical expression.
    ///
    /// This creates an engine instance and launches the cost calculation process
    /// for the specified physical expression.
    pub async fn execute_cost_expression(
        &self,
        task: &CostExpressionTask,
        job_id: JobId,
    ) -> Result<(), Error> {
        let physical_expr_id = task.physical_expr_id;
        let plan = self.egest_partial_plan(physical_expr_id).await?;
        let engine = Engine::new(self.hir_context.clone());
        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            let response = engine
                .launch_rule(
                    "cost",
                    vec![partial_physical_to_value(&plan)],
                    Arc::new(move |value| {
                        let cost = value_to_cost(&value);
                        Box::pin(async move {
                            EngineMessageKind::NewCostedPhysical(physical_expr_id, cost)
                        })
                    }),
                )
                .await;

            Self::send_engine_response(job_id, message_tx, response).await;
        });

        Ok(())
    }
}
