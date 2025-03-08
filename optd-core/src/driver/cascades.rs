use super::{ingest::ingest_logical_plan, memo::Memoize};
use crate::{
    capture,
    engine::Engine,
    error::Error,
    cir::{
        expressions::{LogicalExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan},
        properties::PhysicalProperties,
        rules::RuleBook,
    },
};
use futures::StreamExt;
use futures::{
    channel::mpsc::{self, UnboundedReceiver, UnboundedSender},
    lock::Mutex,
};
use optd_dsl::analyzer::hir::HIR;
use std::{collections::HashMap, sync::Arc};

/// Represents the result of an optimization task
#[derive(Debug)]
pub enum TaskResult {
    /// Result of exploring a logical expression within a group
    Exploration(PartialLogicalPlan, GroupId),
    /// Result of implementing a physical expression for a goal
    Implementation(PartialPhysicalPlan, Goal),
}

#[derive(Debug)]
pub struct Driver<M: Memoize> {
    pub(crate) memo: M,
    rule_book: RuleBook,
    engine: Engine<Arc<Self>>,
    task_sender: UnboundedSender<Result<TaskResult, Error>>,
    task_receiver: UnboundedReceiver<Result<TaskResult, Error>>,
    pub(crate) group_subscribers: Mutex<HashMap<GroupId, Vec<UnboundedSender<LogicalExpression>>>>,
}

impl<M: Memoize> Driver<M> {
    pub(crate) fn new(memo: M, hir: HIR) -> Arc<Self> {
        // TODO: Use the HIR to initialize the rule book
        let rule_book = RuleBook::default();
        let (task_sender, task_receiver) = mpsc::unbounded();

        // TODO: Launch a task that polls task receiver, and dispatches the results to subscribers

        Arc::new_cyclic(|this| Self {
            memo,
            rule_book,
            engine: Engine::new(hir.context, this.upgrade().unwrap()),
            task_sender,
            task_receiver,
            group_subscribers: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn subscribe_to_group(
        &self,
        group_id: GroupId,
    ) -> UnboundedReceiver<LogicalExpression> {
        // TODO: Add the sender to the list of subscribers for the group
        todo!()
    }

    pub(crate) fn subscribe_to_goal(&self, goal: Goal) -> UnboundedReceiver<PhysicalExpression> {
        // TODO: Add the sender to the list of subscribers for the goal
        todo!()
    }

    pub(crate) async fn optimize(self: Arc<Self>, logical_plan: LogicalPlan) -> Result<(), Error> {
        let partial_plan = logical_plan.into();
        let group_id = ingest_logical_plan(&self.memo, &self.engine, &partial_plan).await?;
        let goal = Goal(group_id, PhysicalProperties(None));
        let sender = self.task_sender.clone();

        for rule in self.rule_book.get_implementations() {
            self.engine
                .clone()
                .match_and_apply_implementation_rule(&rule.0, &partial_plan, &goal.1)
                .for_each(capture!([sender, goal], move |result| capture!(
                    [sender, goal],
                    async move {
                        // Map success case to TaskResult::Implementation
                        let mapped_result =
                            result.map(|plan| TaskResult::Implementation(plan, goal.clone()));

                        // Send the result (success or error) through the channel
                        if let Err(e) = sender.unbounded_send(mapped_result) {
                            eprintln!("Failed to send result: {:?}", e);
                        }
                    }
                )))
                .await
        }

        Ok(())
    }
}
