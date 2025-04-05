use std::sync::Arc;

use optd_dsl::engine::Engine;

use crate::{
    bridge::{
        from_cir::{partial_logical_to_value, physical_properties_to_value},
        into_cir::value_to_partial_physical,
    },
    cir::{Goal, GoalId, ImplementationRule, LogicalExpressionId},
    error::Error,
    memo::{Memoize, Status},
    optimizer::{EngineMessageKind, JobId, Optimizer, Task},
};

use super::TaskId;

/// Task data for implementing a logical expression using a specific rule.
#[derive(Debug)]
pub struct ImplementExpressionTask {
    /// The implementation rule to apply.
    pub rule: ImplementationRule,

    /// Whether the task has started the implementation rule.
    pub is_processing: bool,

    /// The logical expression to implement.
    pub logical_expr_id: LogicalExpressionId,

    pub goal_id: GoalId,

    pub optimize_goal_out: TaskId,

    pub fork_in: Option<TaskId>,
}

impl ImplementExpressionTask {
    /// Creates a new `ImplementExpressionTask`.
    pub fn new(
        rule: ImplementationRule,
        is_processing: bool,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        optimize_goal_out: TaskId,
    ) -> Self {
        Self {
            rule,
            is_processing,
            logical_expr_id,
            goal_id,
            optimize_goal_out,
            fork_in: None,
        }
    }

    /// Adds a `ForkLogical` task as a dependency.
    pub fn add_fork_in(&mut self, task_id: TaskId) {
        self.fork_in = Some(task_id);
    }
}

impl<M: Memoize> Optimizer<M> {
    pub(super) async fn create_implement_expression_task(
        &mut self,
        rule: ImplementationRule,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        out: TaskId,
    ) -> Result<TaskId, Error> {
        let task_id = self.next_task_id();
        let is_dirty = self
            .memo
            .get_implementation_status(logical_expr_id, goal_id, &rule)
            .await?
            == Status::Dirty;

        let task = ImplementExpressionTask::new(rule, is_dirty, logical_expr_id, goal_id, out);
        self.tasks.insert(task_id, Task::ImplementExpression(task));

        if is_dirty {
            self.schedule_task_job(task_id);
        }
        Ok(task_id)
    }

    /// Executes a job to apply an implementation rule to a logical expression.
    ///
    /// This creates an engine instance and launches the implementation rule
    /// application process for the specified logical expression and goal.
    pub async fn execute_implementation_rule(
        &self,
        task: &ImplementExpressionTask,
        job_id: JobId,
    ) -> Result<(), Error> {
        let optimize_goal_task = self
            .tasks
            .get(&task.optimize_goal_out)
            .unwrap()
            .as_optimize_goal();
        let Goal(_, physical_props) = self
            .memo
            .materialize_goal(optimize_goal_task.goal_id)
            .await?;

        let goal_id = optimize_goal_task.goal_id;
        let plan = self
            .memo
            .materialize_logical_expr(task.logical_expr_id)
            .await?
            .into();
        let engine = Engine::new(self.hir_context.clone());
        let engine_tx = self.engine_tx.clone();
        let rule_name = task.rule.clone();

        tokio::spawn(async move {
            let response = engine
                .launch_rule(
                    &rule_name.0,
                    vec![
                        partial_logical_to_value(&plan),
                        physical_properties_to_value(&physical_props),
                    ],
                    Arc::new(move |value| {
                        let plan = value_to_partial_physical(&value);
                        Box::pin(
                            async move { EngineMessageKind::NewPhysicalPartial(plan, goal_id) },
                        )
                    }),
                )
                .await;
            Self::send_engine_response(job_id, engine_tx, response).await;
        });

        Ok(())
    }
}
