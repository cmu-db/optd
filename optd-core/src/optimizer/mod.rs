use crate::{
    capture,
    cir::{
        expressions::{LogicalExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan, PhysicalPlan},
        properties::PhysicalProperties,
        rules::RuleBook,
    },
    engine::Engine,
    error::Error,
};
use expander::{Expander, OptimizerExpander};
use futures::channel::oneshot;
use futures::StreamExt;
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    SinkExt,
};
use ingest::ingest_logical_plan;
use memo::Memoize;
use optd_dsl::analyzer::{context::Context, hir::HIR};
use std::collections::{HashMap, HashSet};

pub(crate) mod expander;
mod ingest;
mod memo;

/// The central access point to the OPTD optimizer.
///
/// This struct provides the main interface to the query optimization system,
/// allowing clients to submit logical plans for optimization and receive
/// optimized physical plans in return.
pub struct Optimizer<M: Memoize> {
    memo: M,
    rule_book: RuleBook,
    context: Context,
}

/// Represents the result of an optimization task
#[derive(Debug)]
pub enum TaskResult {
    /// Result of exploring a logical expression within a group
    Exploration(PartialLogicalPlan, GroupId),
    /// Result of implementing a physical expression for a goal
    Implementation(PartialPhysicalPlan, Goal),
}

/// Represents external requests sent to the optimizer
#[derive(Debug)]
pub enum OptimizerRequest {
    /// Request to optimize a logical plan into a physical plan
    Optimize(LogicalPlan, oneshot::Sender<Result<PhysicalPlan, Error>>),
}

// Subscription requests that can be sent to the optimizer
#[derive(Debug)]
pub enum SubscriptionRequest {
    Group(GroupId, Sender<LogicalExpression>),
    Goal(Goal, Sender<PhysicalExpression>),
}

/// Internal volatile state of the optimizer
#[derive(Default)]
struct OptimizerState {
    /// Subscribers to logical expressions in groups
    group_subscribers: HashMap<GroupId, Vec<Sender<LogicalExpression>>>,
    /// Subscribers to physical expressions for goals
    goal_subscribers: HashMap<Goal, Vec<Sender<PhysicalExpression>>>,
    /// Track which groups we've already launched tasks for
    explored_groups: HashSet<GroupId>,
    /// Track which goals we've already launched tasks for
    launched_goals: HashSet<Goal>,
}

impl<M: Memoize> Optimizer<M> {
    /// Create a new optimizer with the given memo and HIR context
    pub fn new(memo: M, hir: HIR) -> Self {
        Self {
            memo,
            rule_book: RuleBook::default(),
            context: hir.context,
        }
    }

    /// Launch the optimizer's processing loop and return a channel for sending requests
    ///
    /// This method consumes the optimizer and starts its processing loop in a
    /// separate task. Clients can communicate with the optimizer through the
    /// returned request channel.
    pub fn launch(self) -> Sender<OptimizerRequest> {
        let (request_sender, request_receiver) = mpsc::channel(0);
        let (task_sender, task_receiver) = mpsc::channel(0);
        let (subscription_sender, subscription_receiver) = mpsc::channel(0);

        tokio::spawn(async move {
            let mut state = OptimizerState::default();

            let engine = Engine::new(
                self.context.clone(),
                OptimizerExpander {
                    subscription_sender: subscription_sender.clone(),
                },
            );

            self.run(
                request_receiver,
                task_receiver,
                subscription_receiver,
                task_sender,
                subscription_sender,
                engine,
                &mut state,
            )
            .await;
        });

        request_sender
    }

    /// Main processing loop that handles requests, tasks, and subscriptions
    async fn run<E: Expander>(
        self,
        mut request_receiver: Receiver<OptimizerRequest>,
        mut task_receiver: Receiver<TaskResult>,
        mut subscription_receiver: Receiver<SubscriptionRequest>,
        task_sender: Sender<TaskResult>,
        subscription_sender: Sender<SubscriptionRequest>,
        engine: Engine<E>,
        state: &mut OptimizerState,
    ) {
        loop {
            tokio::select! {
                Some(request) = request_receiver.next() => {
                    match request {
                        OptimizerRequest::Optimize(logical_plan, response_sender) => {
                            handle_optimize_request(
                                logical_plan,
                                response_sender,
                                &self.memo,
                                &engine,
                                subscription_sender.clone(),
                            ).await;
                        }
                    }
                },

                Some(task_result) = task_receiver.next() => {
                    handle_task_result(
                        task_result,
                        &self.memo,
                        state
                    ).await;
                },

                Some(subscription) = subscription_receiver.next() => {
                    handle_subscription_request(
                        subscription,
                        &self.memo,
                        &self.rule_book,
                        &engine,
                        &task_sender,
                        subscription_sender.clone(),
                        state
                    ).await;
                },

                else => break,
            }
        }
    }
}

async fn handle_optimize_request<M: Memoize, E: Expander>(
    logical_plan: LogicalPlan,
    _response_sender: oneshot::Sender<Result<PhysicalPlan, Error>>,
    memo: &M,
    engine: &Engine<E>,
    mut subscription_sender: Sender<SubscriptionRequest>,
) {
    let (expr_sender, mut expr_receiver) = mpsc::channel(0);

    let group_id = ingest_logical_plan(memo, engine, &logical_plan.into())
        .await
        .expect("Failed to ingest logical plan");
    let goal = Goal(group_id, PhysicalProperties(None));
    let request = SubscriptionRequest::Goal(goal.clone(), expr_sender);

    tokio::spawn(async move {
        subscription_sender
            .send(request)
            .await
            .expect("Failed to send goal subscription request");

        // Just print all expressions for now, as they come, and never stop.
        while let Some(expr) = expr_receiver.next().await {
            println!("Received expression: {:?}", expr);
        }

        todo!("Transform expr into physical plan and send to _response_sender");
    });
}

async fn handle_subscription_request<M: Memoize, E: Expander>(
    subscription: SubscriptionRequest,
    memo: &M,
    rule_book: &RuleBook,
    engine: &Engine<E>,
    task_sender: &Sender<TaskResult>,
    subscription_sender: Sender<SubscriptionRequest>,
    state: &mut OptimizerState,
) {
    match subscription {
        SubscriptionRequest::Group(group_id, mut sender) => {
            if let Some(senders) = state.group_subscribers.get_mut(&group_id) {
                senders.push(sender.clone());
            } else {
                state
                    .group_subscribers
                    .insert(group_id, vec![sender.clone()]);
            }

            if !state.explored_groups.contains(&group_id) {
                explore_group(
                    group_id,
                    task_sender.clone(),
                    subscription_sender.clone(),
                    rule_book,
                    engine.clone(),
                    state,
                );
            }

            let exprs = memo
                .get_all_logical_exprs(group_id)
                .await
                .expect("Failed to get logical exprs");

            for expr in exprs {
                sender
                    .send(expr)
                    .await
                    .expect("Failed to send existing expression");
            }
        }
        SubscriptionRequest::Goal(goal, mut sender) => {
            if let Some(senders) = state.goal_subscribers.get_mut(&goal) {
                senders.push(sender.clone());
            } else {
                state
                    .goal_subscribers
                    .insert(goal.clone(), vec![sender.clone()]);
            }

            if !state.launched_goals.contains(&goal) {
                explore_goal(
                    goal.clone(),
                    task_sender.clone(),
                    subscription_sender.clone(),
                    rule_book,
                    engine.clone(),
                    state,
                );
            }

            let result = memo
                .get_winning_physical_expr(&goal)
                .await
                .expect("Failed to get winning physical expr");

            if let Some((expr, _cost)) = result {
                sender
                    .send(expr)
                    .await
                    .expect("Failed to send existing expression");
            }
        }
    }
}

fn explore_group<E: Expander>(
    group_id: GroupId,
    task_sender: Sender<TaskResult>,
    mut subscription_sender: Sender<SubscriptionRequest>,
    rule_book: &RuleBook,
    engine: Engine<E>,
    state: &mut OptimizerState,
) {
    state.explored_groups.insert(group_id);

    let (expr_sender, mut expr_receiver) = mpsc::channel(0);
    let transformations = rule_book
        .get_transformations()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let request = SubscriptionRequest::Group(group_id, expr_sender);

    tokio::spawn(async move {
        subscription_sender
            .send(request)
            .await
            .expect("Failed to send group subscription request");

        while let Some(expr) = expr_receiver.next().await {
            let plan: PartialLogicalPlan = expr.into();

            for rule in &transformations {
                let rule_name = rule.0.clone();
                tokio::spawn(capture!([engine, plan, task_sender], async move {
                    engine
                        .match_and_apply_logical_rule(&rule_name, &plan)
                        .inspect(|result| {
                            if let Err(err) = result {
                                eprintln!("Error applying rule {}: {:?}", rule_name, err);
                            }
                        })
                        .filter_map(|result| async move { result.ok() })
                        .for_each(|transformed_plan| {
                            let mut task_sender = task_sender.clone();
                            async move {
                                task_sender
                                    .send(TaskResult::Exploration(transformed_plan, group_id))
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

fn explore_goal<E: Expander>(
    goal: Goal,
    task_sender: Sender<TaskResult>,
    mut subscription_sender: Sender<SubscriptionRequest>,
    rule_book: &RuleBook,
    engine: Engine<E>,
    state: &mut OptimizerState,
) {
    state.launched_goals.insert(goal.clone());

    let (expr_sender, mut expr_receiver) = mpsc::channel(0);
    let implementations = rule_book
        .get_implementations()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let props = goal.1.clone();
    let request = SubscriptionRequest::Group(goal.0, expr_sender);

    tokio::spawn(async move {
        subscription_sender
            .send(request)
            .await
            .expect("Failed to send group subscription request for goal");

        while let Some(expr) = expr_receiver.next().await {
            let plan: PartialLogicalPlan = expr.into();

            for rule in &implementations {
                let rule_name = rule.0.clone();
                tokio::spawn(capture!(
                    [engine, plan, task_sender, goal, props],
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
                                let mut task_sender = task_sender.clone();
                                let goal = goal.clone();
                                async move {
                                    task_sender
                                        .send(TaskResult::Implementation(physical_plan, goal))
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

/// Handle a task result from an exploration or implementation task
async fn handle_task_result<M: Memoize>(
    task_result: TaskResult,
    memo: &M,
    state: &mut OptimizerState,
) {
    /*
    If logical addition:
      -> insert into memo
      -> if already exists: ignore
      -> if merge needs to happened: 
      ---> ask what would get merged to the memo, let the memo return a tuple of what groups would get merged
      ---> send to all subscribers of the merged groups (crossed)
      ---> act the merge in the memo
      -> if new expr: send to all subscribers
    If physical addition:
        -> insert into memo
        -> if already exists: ignore
        -> if merge needs to happened:
        ---> ask what would get merged to the memo, let the memo return a tuple of what goals would get merged
        ---> send to all subscribers of the merged goals (crossed) ONLY if cost is better
        ---> act the merge in the memo
        -> if new expr: send to all subscribers
        ===> NOT TOTALLY CLEAR HOW TO HANDLE IF ALREADY MATER OR NOT... REOPTIMIZATAION?
     */

    todo!()
}
