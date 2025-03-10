use super::{memo::Memoize, Optimizer, OptimizerMessage};
use crate::{
    capture,
    cir::{
        expressions::{LogicalExpression, OptimizedExpression},
        goal::Goal,
        group::GroupId,
        plans::PartialLogicalPlan,
    },
};
use futures::{channel::mpsc, SinkExt, Stream, StreamExt};
use OptimizerMessage::*;

impl<M: Memoize> Optimizer<M> {
    /// Subscribe to optimized expressions for a specific goal.
    /// If group hasn't started to explore, launch exploration.
    pub(super) fn subscribe_to_goal(
        &mut self,
        goal: Goal,
    ) -> impl Stream<Item = OptimizedExpression> {
        let (expr_tx, expr_rx) = mpsc::channel(0);

        // Add the sender to goal subscribers list
        self.goal_subscribers
            .entry(goal.clone())
            .or_default()
            .push(expr_tx.clone());

        // Launch exploration if this goal isn't being explored yet
        if !self.exploring_goals.contains(&goal) {
            self.explore_goal(goal.clone());
        }

        // Send the subscription request to the engine
        let mut message_tx = self.message_tx.clone();
        let goal_clone = goal.clone();
        let expr_tx_clone = expr_tx.clone();

        tokio::spawn(async move {
            message_tx
                .send(SubscribeGoal(goal_clone, expr_tx_clone))
                .await
                .expect("Failed to send goal subscription - channel closed");
        });

        expr_rx
    }

    /// Subscribe to logical expressions in a specific group.
    /// If group hasn't started to explore, launch exploration.
    pub(super) fn subscribe_to_group(
        &mut self,
        group_id: GroupId,
    ) -> impl Stream<Item = LogicalExpression> {
        let (expr_tx, expr_rx) = mpsc::channel(0);

        // Launch exploration if this group isn't being explored yet
        if !self.exploring_groups.contains(&group_id) {
            self.explore_group(group_id);
        }

        // Add the sender to group subscribers list
        self.group_subscribers
            .entry(group_id)
            .or_default()
            .push(expr_tx.clone());

        // Send the subscription request to the engine
        let mut message_tx = self.message_tx.clone();
        let expr_tx_clone = expr_tx.clone();

        tokio::spawn(async move {
            message_tx
                .send(SubscribeGroup(group_id, expr_tx_clone))
                .await
                .expect("Failed to send group subscription - channel closed");
        });

        expr_rx
    }

    /// Explores a logical group by applying transformation rules
    ///
    /// This method triggers the application of transformation rules to
    /// the logical expressions in a group, generating new equivalent
    /// logical expressions.
    fn explore_group(&mut self, group_id: GroupId) {
        // Mark the group as exploring
        self.exploring_groups.insert(group_id);

        // Reuse subscribe_to_group to get expressions from the group
        let mut expr_rx = self.subscribe_to_group(group_id);

        let transformations = self.rule_book.get_transformations().to_vec();
        let engine = self.engine.clone();
        let message_tx = self.message_tx.clone();

        // Spawn a task to explore the group
        tokio::spawn(async move {
            while let Some(expr) = expr_rx.next().await {
                let plan: PartialLogicalPlan = expr.into();

                for rule in &transformations {
                    let rule_name = rule.0.clone();

                    tokio::spawn(capture!([engine, plan, message_tx], async move {
                        engine
                            .match_and_apply_logical_rule(&rule_name, &plan)
                            .inspect(|result| {
                                if let Err(err) = result {
                                    eprintln!("Error applying rule {}: {:?}", rule_name, err);
                                }
                            })
                            .filter_map(|result| async move { result.ok() })
                            .for_each(|transformed_plan| {
                                let mut result_tx = message_tx.clone();
                                async move {
                                    result_tx
                                        .send(NewLogicalPartial(transformed_plan, group_id))
                                        .await
                                        .expect("Failed to send transformation result");
                                }
                            })
                            .await;
                    }));
                }
            }
        });
    }

    /// Explores a goal by applying implementation rules
    ///
    /// This method triggers the application of implementation rules to
    /// logical expressions in the goal's group, generating physical
    /// implementations for the goal.
    fn explore_goal(&mut self, goal: Goal) {
        self.exploring_goals.insert(goal.clone());

        // Reuse subscribe_to_group to get expressions from the group's ID
        let mut expr_rx = self.subscribe_to_group(goal.0);

        let implementations = self.rule_book.get_implementations().to_vec();
        let props = goal.1.clone();
        let engine = self.engine.clone();
        let message_tx = self.message_tx.clone();

        // Spawn a task to implement the goal
        tokio::spawn(async move {
            while let Some(expr) = expr_rx.next().await {
                let plan: PartialLogicalPlan = expr.into();

                for rule in &implementations {
                    let rule_name = rule.0.clone();

                    tokio::spawn(capture!(
                        [engine, plan, message_tx, goal, props],
                        async move {
                            engine
                                .match_and_apply_implementation_rule(&rule_name, &plan, &props)
                                .inspect(|result| {
                                    if let Err(err) = result {
                                        eprintln!(
                                            "Error applying implementation rule {}: {:?}",
                                            rule_name, err
                                        );
                                    }
                                })
                                .filter_map(|result| async move { result.ok() })
                                .for_each(|physical_plan| {
                                    let mut result_tx = message_tx.clone();
                                    let goal = goal.clone();
                                    async move {
                                        result_tx
                                            .send(NewPhysicalPartial(physical_plan, goal))
                                            .await
                                            .expect("Failed to send implementation result");
                                    }
                                })
                                .await;
                        }
                    ));
                }
            }
        });
    }
}
