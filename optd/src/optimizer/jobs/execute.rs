use super::{CostedContinuation, JobId, LogicalContinuation};
use crate::{
    cir::{
        Goal, GoalId, GroupId, ImplementationRule, LogicalExpressionId, PhysicalExpressionId,
        TransformationRule,
    },
    dsl::engine::{Engine, EngineResponse},
    memo::Memo,
    optimizer::{
        EngineProduct, Optimizer, OptimizerMessage,
        errors::OptimizeError,
        hir_cir::{
            from_cir::{
                partial_logical_to_value, partial_physical_to_value, physical_properties_to_value,
            },
            into_cir::{
                hir_goal_to_cir, hir_group_id_to_cir, value_to_cost, value_to_logical_properties,
                value_to_partial_logical, value_to_partial_physical,
            },
        },
    },
};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

impl<M: Memo> Optimizer<M> {
    /// Executes a job to derive logical properties for a logical expression.
    ///
    /// This creates an engine instance and launches the property derivation process
    /// for the specified logical expression.
    ///
    /// # Parameters
    /// * `expression_id`: The ID of the logical expression to derive properties for.
    /// * `job_id`: The ID of the job to be executed.
    pub(super) async fn derive_logical_properties(
        &self,
        expression_id: LogicalExpressionId,
        job_id: JobId,
    ) -> Result<(), OptimizeError> {
        use EngineProduct::*;

        let engine = Engine::new(self.hir_context.clone(), self.catalog.clone());
        let plan = partial_logical_to_value(
            &self
                .memo
                .materialize_logical_expr(expression_id)
                .await
                .map_err(OptimizeError::MemoError)?
                .into(),
        );

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            let response = engine
                .launch(
                    "derive",
                    vec![plan],
                    Arc::new(move |logical_props| {
                        Box::pin(async move {
                            CreateGroup(expression_id, value_to_logical_properties(&logical_props))
                        })
                    }),
                )
                .await;

            Self::process_engine_response(job_id, message_tx, response)
        });

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
    pub(super) async fn execute_transformation_rule(
        &self,
        rule_name: TransformationRule,
        expression_id: LogicalExpressionId,
        group_id: GroupId,
        job_id: JobId,
    ) -> Result<(), OptimizeError> {
        use EngineProduct::*;

        let engine = Engine::new(self.hir_context.clone(), self.catalog.clone());
        let plan = partial_logical_to_value(
            &self
                .memo
                .materialize_logical_expr(expression_id)
                .await
                .map_err(OptimizeError::MemoError)?
                .into(),
        );

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
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

            Self::process_engine_response(job_id, message_tx, response)
        });

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
    pub(super) async fn execute_implementation_rule(
        &self,
        rule_name: ImplementationRule,
        expression_id: LogicalExpressionId,
        goal_id: GoalId,
        job_id: JobId,
    ) -> Result<(), OptimizeError> {
        use EngineProduct::*;

        let engine = Engine::new(self.hir_context.clone(), self.catalog.clone());
        let plan = partial_logical_to_value(
            &self
                .memo
                .materialize_logical_expr(expression_id)
                .await
                .map_err(OptimizeError::MemoError)?
                .into(),
        );

        let Goal(_, physical_props) = self
            .memo
            .materialize_goal(goal_id)
            .await
            .map_err(OptimizeError::MemoError)?;
        let properties = physical_properties_to_value(&physical_props);

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
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

            Self::process_engine_response(job_id, message_tx, response)
        });

        Ok(())
    }

    /// Executes a job to compute the cost of a physical expression.
    ///
    /// This creates an engine instance and launches the cost calculation process
    /// for the specified physical expression.
    ///
    /// # Parameters
    /// * `expression_id`: The ID of the physical expression to cost.
    /// * `job_id`: The ID of the job to be executed.
    pub(super) async fn execute_cost_expression(
        &self,
        expression_id: PhysicalExpressionId,
        job_id: JobId,
    ) -> Result<(), OptimizeError> {
        use EngineProduct::*;

        let engine = Engine::new(self.hir_context.clone(), self.catalog.clone());
        let plan = partial_physical_to_value(&self.egest_partial_plan(expression_id).await?);

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            let response = engine
                .launch(
                    "cost",
                    vec![plan],
                    Arc::new(move |cost| {
                        Box::pin(
                            async move { NewCostedPhysical(expression_id, value_to_cost(&cost)) },
                        )
                    }),
                )
                .await;

            Self::process_engine_response(job_id, message_tx, response)
        });

        Ok(())
    }

    /// Executes a job to continue processing with a logical expression result.
    ///
    /// This materializes the logical expression and passes it to the continuation.
    ///
    /// # Parameters
    /// * `expression_id`: The ID of the logical expression to continue with.
    /// * `k`: The continuation function to be called with the materialized plan.
    /// * `job_id`: The ID of the job to be executed.
    pub(super) async fn execute_continue_with_logical(
        &self,
        expression_id: LogicalExpressionId,
        k: LogicalContinuation,
        job_id: JobId,
    ) -> Result<(), OptimizeError> {
        let plan = partial_logical_to_value(
            &self
                .memo
                .materialize_logical_expr(expression_id)
                .await
                .map_err(OptimizeError::MemoError)?
                .into(),
        );

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            let response = k.0(plan).await;
            Self::process_engine_response(job_id, message_tx, response)
        });

        Ok(())
    }

    /// Executes a job to continue processing with an optimized physical expression result.
    ///
    /// This materializes the physical expression and passes it along with its cost
    /// to the continuation.
    ///
    /// # Parameters
    /// * `expression_id`: The ID of the physical expression to continue with.
    /// * `k`: The continuation function to be called with the materialized plan.
    /// * `job_id`: The ID of the job to be executed.
    pub(super) async fn execute_continue_with_costed(
        &self,
        expression_id: PhysicalExpressionId,
        k: CostedContinuation,
        job_id: JobId,
    ) -> Result<(), OptimizeError> {
        let plan = partial_physical_to_value(&self.egest_partial_plan(expression_id).await?);

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            let response = k.0(plan).await;
            Self::process_engine_response(job_id, message_tx, response)
        });

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
        use EngineProduct::*;
        use EngineResponse::*;

        let msg = match response {
            Return(value, k) => OptimizerMessage::product(k(value).await, job_id),
            YieldGroup(group_id, k) => OptimizerMessage::product(
                SubscribeGroup(hir_group_id_to_cir(&group_id), LogicalContinuation(k)),
                job_id,
            ),
            YieldGoal(goal, k) => OptimizerMessage::product(
                SubscribeGoal(hir_goal_to_cir(&goal), CostedContinuation(k)),
                job_id,
            ),
        };

        engine_tx
            .send(msg)
            .await
            .expect("Failed to send message - channel closed");
    }
}
