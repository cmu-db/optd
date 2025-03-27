use super::{Optimizer, OptimizerMessage, memo::Memoize};
use crate::{
    cir::{
        Cost, Goal, GroupId, LogicalExpression, OptimizedExpression, PartialLogicalPlan,
        PartialPhysicalPlan,
    },
    engine::Continuation,
};
use OptimizerMessage::*;
use async_recursion::async_recursion;
use futures::{
    SinkExt, StreamExt,
    channel::mpsc::{self, Sender},
};
use std::sync::Arc;

impl<M: Memoize> Optimizer<M> {
    /// Subscribe to logical expressions in a specific group.
    ///
    /// This method:
    /// - Adds the sender to the group subscribers list to receive future expressions
    /// - Launches exploration of the group if it hasn't already started
    /// - Fetches existing expressions from the memo and sends them to the subscriber
    #[async_recursion]
    pub(super) async fn subscribe_to_group(
        &mut self,
        group_id: GroupId,
        sender: Sender<LogicalExpression>,
    ) {
        // Add the sender to group subscribers list
        self.group_subscribers
            .entry(group_id)
            .or_default()
            .push(sender.clone());

        // Launch exploration if this group isn't being explored yet
        if !self.exploring_groups.contains(&group_id) {
            self.explore_group(group_id).await;
        }

        // Get and send existing expressions from the memo
        let expressions = self
            .memo
            .get_all_logical_exprs(group_id)
            .await
            .expect("Failed to get logical expressions");

        // Send expressions asynchronously
        tokio::spawn(async move {
            for expr in expressions {
                sender
                    .clone()
                    .send(expr)
                    .await
                    .expect("Failed to send existing expression");
            }
        });
    }

    /// Subscribe to optimized expressions for a specific goal.
    ///
    /// This method:
    /// - Adds the sender to the goal subscribers list to receive future optimized expressions
    /// - Launches exploration of the goal if it hasn't already started
    /// - Fetches the best existing optimized expression and sends it to the subscriber
    pub(super) async fn subscribe_to_goal(
        &mut self,
        goal: Goal,
        sender: Sender<OptimizedExpression>,
    ) {
        // Add the sender to goal subscribers list
        self.goal_subscribers
            .entry(goal.clone())
            .or_default()
            .push(sender.clone());

        // Launch exploration if this goal isn't being explored yet
        if !self.exploring_goals.contains(&goal) {
            self.explore_goal(goal.clone()).await;
        }

        // Get and send the best existing optimized expression if any
        if let Some(best_expr) = self
            .memo
            .get_best_optimized_physical_expr(&goal)
            .await
            .expect("Failed to get best expression")
        {
            // Send the expression asynchronously
            tokio::spawn(async move {
                sender
                    .clone()
                    .send(best_expr)
                    .await
                    .expect("Failed to send existing optimized expression");
            });
        }
    }

    /// Explores a logical group by applying transformation rules
    ///
    /// This method triggers the application of transformation rules to
    /// the logical expressions in a group, generating new equivalent
    /// logical expressions.
    async fn explore_group(&mut self, group_id: GroupId) {
        // Mark the group as exploring
        self.exploring_groups.insert(group_id);

        // Subscribe to the group using the sender
        let (expr_tx, mut expr_rx) = mpsc::channel(0);
        self.subscribe_to_group(group_id, expr_tx).await;

        let transformations = self.rule_book.get_transformations().to_vec();
        let engine = self.engine.clone();
        let message_tx = self.message_tx.clone();

        // Spawn a task to explore the group
        tokio::spawn(async move {
            while let Some(expr) = expr_rx.next().await {
                let plan: PartialLogicalPlan = expr.into();

                for rule in &transformations {
                    let rule_name = rule.0.clone();
                    let engine_clone = engine.clone();
                    let message_tx_clone = message_tx.clone();
                    let plan_clone = plan.clone();

                    tokio::spawn(async move {
                        // Create a continuation that processes transformed logical plans
                        let logical_continuation: Continuation<PartialLogicalPlan> =
                            Arc::new(move |transformed_plan| {
                                let mut result_tx = message_tx_clone.clone();
                                Box::pin(async move {
                                    result_tx
                                        .send(NewLogicalPartial(transformed_plan, group_id))
                                        .await
                                        .expect("Failed to send transformation result");
                                })
                            });

                        // Launch the logical rule application with the continuation
                        engine_clone
                            .launch_logical_rule(rule_name, &plan_clone, logical_continuation)
                            .await;
                    });
                }
            }
        });
    }

    /// Explores a goal by applying implementation rules
    ///
    /// This method triggers the application of implementation rules to
    /// logical expressions in the goal's group, generating physical
    /// implementations for the goal.
    async fn explore_goal(&mut self, goal: Goal) {
        self.exploring_goals.insert(goal.clone());

        // Create a channel for receiving expressions
        let (expr_tx, mut expr_rx) = mpsc::channel(0);
        self.subscribe_to_group(goal.0, expr_tx).await;

        let implementations = self.rule_book.get_implementations().to_vec();
        let props = goal.1.clone();
        let engine = self.engine.clone();
        let message_tx = self.message_tx.clone();

        let physical_exprs = self
            .memo
            .get_all_physical_exprs(&goal)
            .await
            .expect("Failed to get physical expressions");

        // Spawn a task to cost all existing physical expressions using CPS
        if !physical_exprs.is_empty() {
            for expr in physical_exprs {
                let plan: PartialPhysicalPlan = expr.clone().into();
                let engine_clone = engine.clone();
                let message_tx_clone = message_tx.clone();
                let goal_clone = goal.clone();
                let expr_clone = expr.clone();

                tokio::spawn(async move {
                    // Create a continuation that processes cost values
                    let cost_continuation: Continuation<Cost> = Arc::new(move |cost| {
                        let mut result_tx = message_tx_clone.clone();
                        let goal = goal_clone.clone();
                        let expr = expr_clone.clone();

                        Box::pin(async move {
                            result_tx
                                .send(NewOptimizedExpression(
                                    OptimizedExpression(expr, cost),
                                    goal,
                                ))
                                .await
                                .expect("Failed to send costed plan");
                        })
                    });

                    // Launch the cost plan operation with the continuation
                    engine_clone
                        .launch_cost_plan(&plan, cost_continuation)
                        .await;
                });
            }
        }

        // Spawn a task to implement the logical expressions
        tokio::spawn(async move {
            while let Some(expr) = expr_rx.next().await {
                let plan: PartialLogicalPlan = expr.into();

                for rule in &implementations {
                    let rule_name = rule.0.clone();
                    let engine_clone = engine.clone();
                    let message_tx_clone = message_tx.clone();
                    let goal_clone = goal.clone();
                    let plan_clone = plan.clone();
                    let props_clone = props.clone();

                    tokio::spawn(async move {
                        // Create a continuation that processes physical plans
                        let physical_continuation: Continuation<PartialPhysicalPlan> =
                            Arc::new(move |physical_plan| {
                                let mut result_tx = message_tx_clone.clone();
                                let goal = goal_clone.clone();

                                Box::pin(async move {
                                    result_tx
                                        .send(NewPhysicalPartial(physical_plan, goal))
                                        .await
                                        .expect("Failed to send implementation result");
                                })
                            });

                        // Launch the implementation rule application with the continuation
                        engine_clone
                            .launch_implementation_rule(
                                rule_name,
                                &plan_clone,
                                &props_clone,
                                physical_continuation,
                            )
                            .await;
                    });
                }
            }
        });
    }
}
