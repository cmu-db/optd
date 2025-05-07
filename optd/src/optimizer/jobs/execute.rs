use super::{CostedContinuation, JobId, LogicalContinuation};
use crate::{
    cir::{
        Cost, GoalId, GroupId, ImplementationRule, LogicalExpressionId, PhysicalExpressionId,
        TransformationRule,
    },
    memo::Memo,
    optimizer::{
        Optimizer, OptimizerMessage,
        errors::OptimizeError,
        hir_cir::{from_cir::partial_logical_to_value, into_cir::value_to_partial_logical},
    },
};
use std::sync::Arc;

impl<M: Memo> Optimizer<M> {
    /// Executes a job to derive logical properties for a logical expression.
    ///
    /// This creates an engine instance and launches the property derivation process
    /// for the specified logical expression.
    async fn derive_logical_properties(
        &self,
        expression_id: LogicalExpressionId,
        job_id: JobId,
    ) -> Result<(), OptimizeError> {
        /*let engine = Engine::new(self.hir_context.clone(), self.catalog.clone());

        let plan = self
            .memo
            .materialize_logical_expr(expression_id)
            .await?
            .into();

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            engine
                .launch_derive_properties(
                    plan,
                    Arc::new(move |logical_props| {
                        let mut message_tx = message_tx.clone();
                        Box::pin(async move {
                            message_tx
                                .send(CreateGroup(expression_id, logical_props, job_id))
                                .await
                                .expect("Failed to send create group - channel closed");
                        })
                    }),
                )
                .await;
        });*/
        Ok(())
    }

    /// Executes a job to apply a transformation rule to a logical expression.
    ///
    /// This creates an engine instance and launches the transformation rule
    /// application process for the specified logical expression.
    async fn execute_transformation_rule(
        &self,
        rule_name: TransformationRule,
        expression_id: LogicalExpressionId,
        group_id: GroupId,
        job_id: JobId,
    ) -> Result<(), OptimizeError> {
        /*let engine = Engine::new(self.hir_context.clone(), self.catalog.clone());

        let plan = &self
            .memo
            .materialize_logical_expr(expression_id)
            .await
            .map_err(OptimizeError::MemoError)?
            .into();

        let input = vec![partial_logical_to_value(plan)];

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            engine
                .launch_rule(
                    &rule_name.0,
                    input,
                    Arc::new(move |output| {
                        let message_tx = message_tx.clone();
                        Box::pin(async move {
                            let plan = value_to_partial_logical(&output);
                            message_tx
                                .send(OptimizerMessage::NewLogicalPartial(plan, group_id, job_id))
                                .await
                                .expect("Failed to send new logical partial - channel closed");
                        })
                    }),
                )
                .await;
        });*/

        Ok(())
    }

    /// Executes a job to apply an implementation rule to a logical expression.
    ///
    /// This creates an engine instance and launches the implementation rule
    /// application process for the specified logical expression and goal.
    async fn execute_implementation_rule(
        &self,
        rule_name: ImplementationRule,
        expression_id: LogicalExpressionId,
        goal_id: GoalId,
        job_id: JobId,
    ) -> Result<(), OptimizeError> {
        //let engine = Engine::new(self.hir_context.clone(), self.catalog.clone());

        /*let Goal(_, physical_props) = self.memo.materialize_goal(goal_id).await?;
        let plan = self
            .memo
            .materialize_logical_expr(expression_id)
            .await?
            .into();

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            engine
                .launch_implementation_rule(
                    rule_name.0,
                    plan,
                    physical_props,
                    Arc::new(move |plan| {
                        let mut message_tx = message_tx.clone();
                        Box::pin(async move {
                            message_tx
                                .send(NewPhysicalPartial(plan, goal_id, job_id))
                                .await
                                .expect("Failed to send new physical partial - channel closed");
                        })
                    }),
                )
                .await;
        });*/

        Ok(())
    }

    /// Executes a job to compute the cost of a physical expression.
    ///
    /// This creates an engine instance and launches the cost calculation process
    /// for the specified physical expression.
    async fn execute_cost_expression(
        &self,
        expression_id: PhysicalExpressionId,
        job_id: JobId,
    ) -> Result<(), OptimizeError> {
        //let engine = Engine::new(self.hir_context.clone(), self.catalog.clone());

        /*let plan = self.egest_partial_plan(expression_id).await?;

        let message_tx = self.message_tx.clone();
        tokio::spawn(async move {
            engine
                .launch_cost_plan(
                    plan,
                    Arc::new(move |cost| {
                        let mut message_tx = message_tx.clone();
                        Box::pin(async move {
                            message_tx
                                .send(NewCostedPhysical(expression_id, cost, job_id))
                                .await
                                .expect("Failed to send new costed physical - channel closed");
                        })
                    }),
                )
                .await;
        });*/

        Ok(())
    }

    /// Executes a job to continue processing with a logical expression result.
    ///
    /// This materializes the logical expression and passes it to the continuation.
    async fn execute_continue_with_logical(
        &self,
        expression_id: LogicalExpressionId,
        k: LogicalContinuation,
    ) -> Result<(), OptimizeError> {
        /*let plan = self
            .memo
            .materialize_logical_expr(expression_id)
            .await?
            .into();

        tokio::spawn(async move {
            k(plan).await;
        });*/

        Ok(())
    }

    /// Executes a job to continue processing with an optimized physical expression result.
    ///
    /// This materializes the physical expression and passes it along with its cost
    /// to the continuation.
    async fn execute_continue_with_optimized(
        &self,
        physical_expr_id: PhysicalExpressionId,
        cost: Cost,
        k: CostedContinuation,
    ) -> Result<(), OptimizeError> {
        /*let plan = self.egest_partial_plan(physical_expr_id).await?;

        tokio::spawn(async move {
            k((plan, cost)).await;
        });*/

        Ok(())
    }
}
