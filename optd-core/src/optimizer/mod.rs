use crate::{
    capture,
    cir::{
        expressions::{LogicalExpression, OptimizedExpression},
        goal::Goal,
        group::GroupId,
        plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan, PhysicalPlan},
        properties::PhysicalProperties,
        rules::RuleBook,
    },
    engine::Engine,
    error::Error,
};
use expander::OptimizerExpander;
use futures::channel::oneshot;
use futures::StreamExt;
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    SinkExt,
};
use ingest::ingest_logical_plan;
use memo::Memoize;
use optd_dsl::analyzer::hir::HIR;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

pub(crate) mod expander;
mod ingest;
mod memo;

/// Represents external requests sent to the optimizer
#[derive(Debug)]
pub enum OptimizationRequest {
    /// Request to start optimizing a logical plan into a physical plan
    StartOptimize(LogicalPlan, oneshot::Sender<Result<PhysicalPlan, Error>>),
    /// Request to end the optimization process and return the optimized physical plan
    EndOptimize(
        OptimizedExpression,
        oneshot::Sender<Result<PhysicalPlan, Error>>,
    ),
}

/// Represents the result of an optimization task
#[derive(Debug)]
enum TaskResult {
    /// Result of exploring a logical expression within a group
    Exploration(PartialLogicalPlan, GroupId),
    /// Result of implementing a physical expression for a goal
    Implementation(PartialPhysicalPlan, Goal),
}

// Subscription requests that can be sent to the optimizer
#[derive(Debug)]
enum SubscriptionRequest {
    Group(GroupId, Sender<LogicalExpression>),
    Goal(Goal, Sender<OptimizedExpression>),
}

/// The central access point to the OPTD optimizer.
///
/// This struct provides the main interface to the query optimization system,
/// allowing clients to submit logical plans for optimization and receive
/// optimized physical plans in return. It contains all state necessary for
/// optimization, including channels for communication between components.
struct Optimizer<M: Memoize> {
    /// The memo instance for storing optimization data
    memo: M,
    /// The rule book containing transformation and implementation rules
    rule_book: RuleBook,
    /// The engine for evaluating rules and functions
    engine: Engine<OptimizerExpander>,
    /// Subscribers to logical expressions in groups
    group_subscribers: HashMap<GroupId, Vec<Sender<LogicalExpression>>>,
    /// Subscribers to optimized physical expressions for goals
    goal_subscribers: HashMap<Goal, Vec<Sender<OptimizedExpression>>>,
    /// Track which groups we've already launched tasks for
    explored_groups: HashSet<GroupId>,
    /// Track which goals we've already launched tasks for
    launched_goals: HashSet<Goal>,
    /// Sender for task results
    task_sender: Sender<TaskResult>,
    /// Receiver for task results
    task_receiver: Receiver<TaskResult>,
    /// Sender for subscription requests
    subscription_sender: Sender<SubscriptionRequest>,
    /// Receiver for subscription requests
    subscription_receiver: Receiver<SubscriptionRequest>,
    /// Sender for optimization requests
    request_sender: Sender<OptimizationRequest>,
    /// Receiver for optimization requests
    request_receiver: Receiver<OptimizationRequest>,
}

impl<M: Memoize> Optimizer<M> {
    /// Launch a new optimizer with the given memo and HIR context
    ///
    /// This method creates, initializes, and launches the optimizer in one step.
    /// It returns a sender that can be used to communicate with the optimizer.
    pub fn launch(memo: M, hir: HIR) -> Sender<OptimizationRequest> {
        // Initialize all channels
        let (task_sender, task_receiver) = mpsc::channel(0);
        let (subscription_sender, subscription_receiver) = mpsc::channel(0);
        let (request_sender, request_receiver) = mpsc::channel(0);

        // Create the engine with the optimizer expander
        let engine = Engine::new(
            hir.context,
            OptimizerExpander {
                subscription_sender: subscription_sender.clone(),
            },
        );

        // Create the optimizer with initialized channels
        let optimizer = Self {
            memo,
            rule_book: RuleBook::default(),
            engine,
            group_subscribers: HashMap::new(),
            goal_subscribers: HashMap::new(),
            explored_groups: HashSet::new(),
            launched_goals: HashSet::new(),
            task_sender: task_sender.clone(),
            task_receiver,
            subscription_sender: subscription_sender.clone(),
            subscription_receiver,
            request_sender: request_sender.clone(),
            request_receiver,
        };

        // Start the background processing loop
        tokio::spawn(async move {
            optimizer.run().await;
        });

        // Return the request sender for client communication
        request_sender
    }

    /// Run the optimizer's main processing loop
    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(request) = self.request_receiver.next() => {
                    match request {
                        OptimizationRequest::StartOptimize(logical_plan, response_sender) => {
                            self.start_optimize(logical_plan, response_sender).await;
                        },
                        OptimizationRequest::EndOptimize(optimized_expr, response_sender) => {
                            self.end_optimize(optimized_expr, response_sender).await;
                        }
                    }
                },

                Some(task_result) = self.task_receiver.next() => {
                    match task_result {
                        TaskResult::Exploration(plan, group_id) => {
                            // Handle exploration result
                            println!("Received exploration result for group: {:?}", group_id);
                        },
                        TaskResult::Implementation(plan, goal) => {
                            // Handle implementation result
                            println!("Received implementation result for goal: {:?}", goal);
                        }
                    }
                },

                Some(subscription) = self.subscription_receiver.next() => {
                    match subscription {
                        SubscriptionRequest::Group(group_id, sender) => {
                            self.handle_group_subscription(group_id, sender).await;
                        },
                        SubscriptionRequest::Goal(goal, sender) => {
                            self.handle_goal_subscription(goal, sender).await;
                        }
                    }
                },

                else => break,
            }
        }
    }

    /// Handles a start optimize request
    async fn start_optimize(
        &self,
        logical_plan: LogicalPlan,
        response_sender: oneshot::Sender<Result<PhysicalPlan, Error>>,
    ) {
        let (expr_sender, mut expr_receiver) = mpsc::channel(0);

        let group_id = ingest_logical_plan(&self.memo, &logical_plan.into())
            .await
            .expect("Failed to ingest logical plan");
        let goal = Goal(group_id, PhysicalProperties(None));
        let request = SubscriptionRequest::Goal(goal.clone(), expr_sender);

        let mut subscription_sender = self.subscription_sender.clone();
        let mut request_sender = self.request_sender.clone();

        tokio::spawn(async move {
            subscription_sender
                .send(request)
                .await
                .expect("Failed to send goal subscription request");

            // Wait for the first expression (which always comes)
            let first_expr = expr_receiver
                .next()
                .await
                .expect("Should always get at least one expression");
            println!("Received first expression: {:?}", first_expr);

            // Create a timeout for 5 seconds of additional optimization
            let optimization_timeout = tokio::time::sleep(Duration::from_secs(5));

            // Continue collecting expressions for the timeout period
            let mut best_expr = first_expr;

            tokio::select! {
                _ = optimization_timeout => {
                    println!("Optimization timeout reached after 5 seconds");
                }

                () = async {
                    // Process additional expressions as they arrive
                    while let Some(expr) = expr_receiver.next().await {
                        println!("Received additional expression: {:?}", expr);
                        // Keep track of the latest expression as our "best" so far
                        best_expr = expr;
                    }
                } => {}
            }

            // After timeout, send the EndOptimize request with the best expression we found
            request_sender
                .send(OptimizationRequest::EndOptimize(best_expr, response_sender))
                .await
                .expect("Failed to send EndOptimize request");
        });
    }

    /// Handles an end optimize request
    async fn end_optimize(
        &self,
        _optimized_expr: OptimizedExpression,
        _response_sender: oneshot::Sender<Result<PhysicalPlan, Error>>,
    ) {
        // Empty implementation for now as requested
    }

    /// Handles a group subscription request
    async fn handle_group_subscription(
        &mut self,
        group_id: GroupId,
        mut sender: Sender<LogicalExpression>,
    ) {
        // Get existing logical expressions for the group
        let exprs = self
            .memo
            .get_all_logical_exprs(group_id)
            .await
            .expect("Failed to get logical exprs");

        // Send existing expressions to the subscriber
        for expr in exprs {
            sender
                .send(expr)
                .await
                .expect("Failed to send existing expression");
        }

        // Set up exploration of the group if not already explored
        if !self.explored_groups.contains(&group_id) {
            self.explore_group(group_id).await;
        }
    }

    /// Handles a goal subscription request
    async fn handle_goal_subscription(
        &mut self,
        goal: Goal,
        mut sender: Sender<OptimizedExpression>,
    ) {
        // Get the best existing physical expression for the goal
        let result = self
            .memo
            .get_best_optimized_physical_expr(&goal)
            .await
            .expect("Failed to get winning physical expr");

        // Send existing expression to the subscriber if available
        if let Some(expr) = result {
            sender
                .send(expr)
                .await
                .expect("Failed to send existing expression");
        }

        // Set up implementation of the goal if not already launched
        if !self.launched_goals.contains(&goal) {
            self.explore_goal(goal).await;
        }
    }

    /// Explores a logical group by applying transformation rules
    async fn explore_group(&mut self, group_id: GroupId) {
        // Mark the group as explored
        self.explored_groups.insert(group_id);

        // Set up exploration of the group
        let (expr_sender, mut expr_receiver) = mpsc::channel(0);
        let transformations = self
            .rule_book
            .get_transformations()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let request = SubscriptionRequest::Group(group_id, expr_sender);

        let mut subscription_sender = self.subscription_sender.clone();
        let task_sender = self.task_sender.clone();
        let engine = self.engine.clone();

        // Spawn a task to explore the group
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

    /// Explores a goal by applying implementation rules
    async fn explore_goal(&mut self, goal: Goal) {
        self.launched_goals.insert(goal.clone());

        // Set up implementation of the goal
        let (expr_sender, mut expr_receiver) = mpsc::channel(0);
        let implementations = self
            .rule_book
            .get_implementations()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let props = goal.1.clone();
        let request = SubscriptionRequest::Group(goal.0, expr_sender);

        let mut subscription_sender = self.subscription_sender.clone();
        let task_sender = self.task_sender.clone();
        let engine = self.engine.clone();

        // Spawn a task to implement the goal
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
}
