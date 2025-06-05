use super::{JobId, LogicalContinuation};
use crate::{
    cir::*,
    dsl::{
        analyzer::hir::{CoreData, Value},
        engine::{Engine, EngineResponse},
    },
    memo::Memo,
    optimizer::{
        EngineProduct, Optimizer, OptimizerMessage,
        hir_cir::{
            from_cir::{partial_logical_to_value, physical_properties_to_value},
            into_cir::{
                hir_group_id_to_cir, value_to_logical_properties, value_to_partial_logical,
                value_to_partial_physical,
            },
        },
    },
};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::Instrument;

impl<M: Memo> Optimizer<M> {
    /// Executes a job to derive logical properties for a logical expression.
    ///
    /// This creates an engine instance and launches the property derivation process
    /// for the specified logical expression.
    ///
    /// # Parameters
    /// * `expression_id`: The ID of the logical expression to derive properties for.
    /// * `job_id`: The ID of the job to be executed.
    #[tracing::instrument(skip(self), fields(job_id, expr_id = ?expression_id), target="optd::optimizer::jobs")]
    pub(super) async fn derive_logical_properties(
        &self,
        expression_id: LogicalExpressionId,
        job_id: JobId,
    ) -> Result<(), M::MemoError> {
        use EngineProduct::*;

        let engine = Engine::new(
            self.hir_context.clone(),
            self.catalog.clone(),
            self.retriever.clone(),
        );
        let plan = partial_logical_to_value(
            &self
                .memo
                .materialize_logical_expr(expression_id)
                .await?
                .into(),
            None, // FIXME: This is correct, however in the DSL right now the parameters
                  // of the derive function is a *, to allow its children to be * too. However,
                  // since the group is not yet created, it isn't really stored yet it.
                  // Hence, the input should be a different type, and ideally be made consistent
                  // with how costing is handled (i.e. with $ and *).
        );

        tracing::debug!(target: "optd::optimizer::jobs", "Launching DSL engine for 'derive'");
        let message_tx = self.message_tx.clone();
        tokio::spawn(
            async move {
                let response = engine
                    .launch(
                        "derive",
                        vec![plan],
                        Arc::new(move |logical_props| {
                            Box::pin(async move {
                                CreateGroup(
                                    expression_id,
                                    value_to_logical_properties(&logical_props),
                                )
                            })
                        }),
                    )
                    .await;

                Self::process_engine_response(job_id, message_tx, response).await;
            }
            .instrument(tracing::debug_span!(target: "optd::optimizer::jobs", "derive_job_execution", job_id = ?job_id, expression_id = ?expression_id)));

        Ok(())
    }

    /// Executes a job to apply a transformation rule to a logical expression.
    ///
    /// This creates an engine instance and launches the transformation rule
    /// application process for the specified logical expression.
    ///
    /// # Parameters
    /// * `rule_name`: The name of the transformation rule to apply.
    /// * `expression_id`: The ID of the logical expression to transform.
    /// * `group_id`: The ID of the group to which the transformed expression belongs.
    /// * `job_id`: The ID of the job to be executed.
    #[tracing::instrument(skip(self), fields(job_id, rule = %rule_name.0, expr_id = ?expression_id, group_id = ?group_id), target="optd::optimizer::jobs")]
    pub(super) async fn execute_transformation_rule(
        &self,
        rule_name: TransformationRule,
        expression_id: LogicalExpressionId,
        group_id: GroupId,
        job_id: JobId,
    ) -> Result<(), M::MemoError> {
        use CoreData::*;
        use EngineProduct::*;
        use EngineResponse::*;

        let engine = self.init_engine();
        let plan = partial_logical_to_value(
            &self
                .memo
                .materialize_logical_expr(expression_id)
                .await?
                .into(),
            Some(group_id),
        );

        tracing::debug!(target: "optd::optimizer::jobs", "Launching DSL engine for transformation rule '{}'", rule_name.0);
        let message_tx = self.message_tx.clone();
        let rule_name_clone = rule_name.0.clone();
        tokio::spawn(
            async move {
                let response = engine
                    .launch(
                        &rule_name.0,
                        vec![plan],
                        Arc::new(move |output| {
                            Box::pin(async move {
                                NewLogicalPartial(value_to_partial_logical(&output), group_id)
                            })
                        }),
                    )
                    .await;

                // A none result means the rule was not applicable.
                if !matches!(response, Return(Value { data: None, .. }, _)) {
                    Self::process_engine_response(job_id, message_tx, response).await;
                } else {
                    tracing::debug!(target: "optd::optimizer::rules", rule_name=%rule_name.0, "Rule not applicable or returned None");
                }
            }
            .instrument(tracing::debug_span!(target: "optd::optimizer::jobs", "transform_job_execution", job_id = ?job_id, rule_name = %rule_name_clone)));

        Ok(())
    }

    /// Executes a job to apply an implementation rule to a logical expression.
    ///
    /// This creates an engine instance and launches the implementation rule
    /// application process for the specified logical expression and goal.
    ///
    /// # Parameters
    /// * `rule_name`: The name of the implementation rule to apply.
    /// * `expression_id`: The ID of the logical expression to implement.
    /// * `goal_id`: The ID of the goal to which the implementation belongs.
    /// * `job_id`: The ID of the job to be executed.
    #[tracing::instrument(skip(self), fields(job_id, rule = %rule_name.0, expr_id = ?expression_id, goal_id = ?goal_id), target="optd::optimizer::jobs")]
    pub(super) async fn execute_implementation_rule(
        &self,
        rule_name: ImplementationRule,
        expression_id: LogicalExpressionId,
        goal_id: GoalId,
        job_id: JobId,
    ) -> Result<(), M::MemoError> {
        use EngineProduct::*;

        let Goal(group_id, physical_props) = self.memo.materialize_goal(goal_id).await?;
        let properties = physical_properties_to_value(&physical_props);

        let engine = self.init_engine();
        let plan = partial_logical_to_value(
            &self
                .memo
                .materialize_logical_expr(expression_id)
                .await?
                .into(),
            Some(group_id),
        );

        tracing::debug!(target: "optd::optimizer::jobs", "Launching DSL engine for implementation rule '{}'", rule_name.0);
        let message_tx = self.message_tx.clone();
        let rule_name_clone = rule_name.0.clone();
        tokio::spawn(
            async move {
                let response = engine
                    .launch(
                        &rule_name.0,
                        vec![plan, properties],
                        Arc::new(move |plan| {
                            Box::pin(async move {
                                NewPhysicalPartial(value_to_partial_physical(&plan), goal_id)
                            })
                        }),
                    )
                    .await;

                Self::process_engine_response(job_id, message_tx, response).await;
            }
            .instrument(tracing::debug_span!(target: "optd::optimizer::jobs", "implement_job_execution", job_id = ?job_id, rule_name = %rule_name_clone)));

        Ok(())
    }

    /// Executes a job to continue processing with a logical expression result.
    ///
    /// This materializes the logical expression and passes it to the continuation.
    ///
    /// # Parameters
    /// * `expression_id`: The ID of the logical expression to continue with.
    /// * `group_id`: The ID of the group to which the expression belongs.
    /// * `k`: The continuation function to be called with the materialized plan.
    /// * `job_id`: The ID of the job to be executed.
    #[tracing::instrument(skip(self, k), fields(job_id, expr_id = ?expression_id, group_id = ?group_id), target="optd::optimizer::jobs")]
    pub(super) async fn execute_continue_with_logical(
        &self,
        expression_id: LogicalExpressionId,
        group_id: GroupId,
        k: LogicalContinuation,
        job_id: JobId,
    ) -> Result<(), M::MemoError> {
        use CoreData::*;
        use EngineResponse::*;

        let plan = partial_logical_to_value(
            &self
                .memo
                .materialize_logical_expr(expression_id)
                .await?
                .into(),
            Some(group_id),
        );

        tracing::debug!(target: "optd::optimizer::jobs", "Executing logical continuation");
        let message_tx = self.message_tx.clone();
        tokio::spawn(
            async move {
                let response = k.0(plan).await;

                // A none result means the rule was not applicable.
                if !matches!(response, Return(Value { data: None, .. }, _)) {
                    Self::process_engine_response(job_id, message_tx, response).await;
                } else {
                    tracing::debug!(target: "optd::optimizer::jobs", "Logical continuation returned None or was not applicable");
                }
            }
            .instrument(tracing::debug_span!(target: "optd::optimizer::jobs", "continue_logical_job_execution", job_id = ?job_id)));

        Ok(())
    }

    /// Helper function to process the engine response and send it to the optimizer.
    ///
    /// Handles `YieldGroup` and `YieldGoal` responses by sending subscription messages
    /// for the respective group or goal.
    ///
    /// # Parameters
    /// * `job_id`: The ID of the job being processed.
    /// * `engine_tx`: The sender channel for optimizer messages.
    /// * `response`: The engine response to process.
    async fn process_engine_response(
        job_id: JobId,
        engine_tx: Sender<OptimizerMessage>,
        response: EngineResponse<EngineProduct>,
    ) {
        tracing::trace!(target: "optd::optimizer::jobs", job_id = ?job_id, "Processing engine response");
        use EngineProduct::*;
        use EngineResponse::*;

        let msg = match response {
            Return(value, k) => OptimizerMessage::product(k(value).await, job_id),
            YieldGroup(group_id, k) => OptimizerMessage::product(
                SubscribeGroup(hir_group_id_to_cir(&group_id), LogicalContinuation(k)),
                job_id,
            ),
            YieldGoal(_, _) => todo!("Decide what to do here depending on the cost model"),
        };

        if let Err(e) = engine_tx.send(msg).await {
            tracing::error!(target: "optd::optimizer::jobs", job_id = ?job_id, "Failed to send optimizer message from job: {}", e);
        }
    }

    /// Helper function to create a new engine instance.
    fn init_engine(&self) -> Engine<EngineProduct> {
        Engine::new(
            self.hir_context.clone(),
            self.catalog.clone(),
            self.retriever.clone(),
        )
    }
}
