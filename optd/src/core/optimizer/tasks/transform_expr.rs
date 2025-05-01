use std::sync::Arc;

use crate::dsl::engine::Engine;

use crate::{
    core::bridge::{from_cir::partial_logical_to_value, into_cir::value_to_partial_logical},
    core::cir::{LogicalExpressionId, PartialLogicalPlan, TransformationRule},
    core::error::Error,
    core::memo::{Memoize, Status},
    core::optimizer::{EngineMessageKind, JobId, Optimizer, Task},
};

use super::TaskId;

/// Task data for transforming a logical expression using a specific rule.
#[derive(Debug)]
pub struct TransformExpressionTask {
    /// The transformation rule to apply.
    pub rule: TransformationRule,

    /// Whether the task has started the transformation rule.
    pub is_processing: bool,

    /// The logical expression to transform.
    pub logical_expr_id: LogicalExpressionId,

    pub explore_group_out: TaskId,

    pub fork_in: Option<TaskId>,
}

impl TransformExpressionTask {
    /// Creates a new `TransformExpressionTask`.
    pub fn new(
        rule: TransformationRule,
        is_processing: bool,
        logical_expr_id: LogicalExpressionId,
        explore_group_out: TaskId,
    ) -> Self {
        Self {
            rule,
            is_processing,
            logical_expr_id,
            explore_group_out,
            fork_in: None,
        }
    }

    /// Adds a `ForkLogical` task as a dependency.
    pub fn add_fork_in(&mut self, task_id: TaskId) {
        self.fork_in = Some(task_id);
    }
}

impl<M: Memoize> Optimizer<M> {
    /// Creates a task to start applying a transformation rule to a logical expression.
    ///
    /// This task generates alternative logical expressions that are
    /// semantically equivalent to the original. It maintains a set of continuations
    /// that will be notified of the transformation results.
    ///
    /// Only schedules the starting job if the transformation is marked as dirty in the memo.
    pub async fn create_transform_expression_task(
        &mut self,
        rule: TransformationRule,
        logical_expr_id: LogicalExpressionId,
        out: TaskId,
    ) -> Result<TaskId, Error> {
        let task_id = self.next_task_id();
        let is_dirty = self
            .memo
            .get_transformation_status(logical_expr_id, &rule)
            .await?
            == Status::Dirty;
        let task = TransformExpressionTask::new(rule, false, logical_expr_id, out);

        self.tasks.insert(task_id, Task::TransformExpression(task));
        if is_dirty {
            self.schedule_task_job(task_id);
        }
        Ok(task_id)
    }

    /// Executes a job to apply a transformation rule to a logical expression.
    ///
    /// This creates an engine instance and launches the transformation rule
    /// application process for the specified logical expression.
    pub async fn execute_transformation_rule(
        &self,
        task: &TransformExpressionTask,
        job_id: JobId,
    ) -> Result<(), Error> {
        let explore_group_task = self
            .tasks
            .get(&task.explore_group_out)
            .unwrap()
            .as_explore_group();
        let group_id = explore_group_task.group_id;
        let plan: PartialLogicalPlan = self
            .memo
            .materialize_logical_expr(task.logical_expr_id)
            .await?
            .into();

        let engine = Engine::new(self.hir_context.clone(), self.catalog.clone());
        let engine_tx = self.engine_tx.clone();
        let rule_name = task.rule.clone();

        tokio::spawn(async move {
            let response = engine
                .launch_rule(
                    &rule_name.0,
                    vec![partial_logical_to_value(&plan)],
                    Arc::new(move |value| {
                        let plan = value_to_partial_logical(&value);

                        Box::pin(
                            async move { EngineMessageKind::NewLogicalPartial(plan, group_id) },
                        )
                    }),
                )
                .await;

            Self::send_engine_response(job_id, engine_tx, response).await;
        });

        Ok(())
    }
}
