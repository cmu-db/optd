use crate::{
    capture,
    cir::{
        expressions::{LogicalExpression, OptimizedExpression},
        goal::Goal,
        group::GroupId,
        plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan, PhysicalPlan},
        properties::{LogicalProperties, PhysicalProperties},
        rules::RuleBook,
    },
    engine::Engine,
    error::Error,
};
use expander::OptimizerExpander;
use futures::StreamExt;
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    SinkExt,
};
use futures::{channel::oneshot, Stream};
use ingest::{ingest_logical_plan, LogicalIngestion};
use memo::Memoize;
use optd_dsl::analyzer::hir::HIR;
use std::collections::{HashMap, HashSet};

mod expander;
mod ingest;
mod memo;

/// External client requests to the query optimizer.
///
/// Defines the public API for submitting queries and receiving execution plans.
#[derive(Debug)]
pub enum OptimizationRequest {
    /// Optimize a logical query plan into a physical execution plan.
    ///
    /// Streams results back as they become available, allowing clients to:
    /// * Receive progressively better plans during optimization
    /// * Terminate early when a "good enough" plan is found
    /// * Control optimization duration by consuming or dropping the receiver
    ///
    /// * `LogicalPlan` - Input logical plan to optimize
    /// * `Sender<Result<PhysicalPlan, Error>>` - Channel for receiving optimized plans
    Optimize(LogicalPlan, Sender<Result<PhysicalPlan, Error>>),
}

/// Results produced by the optimization engine for the central optimizer.
///
/// Represents outcomes from various stages of query optimization.
#[derive(Debug)]
enum EngineResult {
    /// New logical plan alternative for a group from applying transformation rules.
    NewLogicalPartial(PartialLogicalPlan, GroupId),

    /// New physical implementation for a goal, awaiting recursive optimization.
    ///
    /// Intermediate result without full costing or optimized child operators.
    NewPhysicalPartial(PartialPhysicalPlan, Goal),

    /// Fully optimized physical expression with complete costing.
    ///
    /// May not be the globally optimal plan for the goal.
    NewOptimizedExpression(OptimizedExpression, Goal),

    /// The derived logical properties of a logical expression
    ///
    /// Includes the job ID of the property derivation task that produced these properties,
    /// allowing correlation with pending ingestion tasks.
    DeriveProperties(LogicalProperties, i64),
}

/// Requests from the optimization engine to the central optimizer.
///
/// Used to access shared state or subscribe to updates in the memo structure.
#[derive(Debug)]
pub(crate) enum EngineRequest {
    /// Subscribe to all logical expressions in a specific group.
    ///
    /// Provides notifications for existing and new expressions in the equivalence class.
    /// * `GroupId` - Group to subscribe to
    /// * `Sender<LogicalExpression>` - Channel for expressions
    SubscribeGroup(GroupId, Sender<LogicalExpression>),

    /// Subscribe to optimized physical implementations for a goal.
    ///
    /// Only sends notifications for plans better than previously discovered ones,
    /// implementing pruning to focus on promising candidates.
    /// * `Goal` - Optimization goal (group ID and physical properties)
    /// * `Sender<OptimizedExpression>` - Channel for improved expressions
    SubscribeGoal(Goal, Sender<OptimizedExpression>),

    /// Retrieve logical properties for a specific group.
    ///
    /// Gets properties (schema, cardinality, statistics) needed for rules and costing.
    /// * `GroupId` - Group whose properties are requested
    /// * `oneshot::Sender<LogicalProperties>` - Channel for properties response
    DeriveProperties(GroupId, oneshot::Sender<LogicalProperties>),
}

/// Represents a logical plan waiting for property derivation to complete before ingestion.
///
/// This structure tracks the state of a logical plan that requires derived properties
/// before it can be fully ingested into the memo. It maintains a set of property
/// derivation job IDs that must complete before the plan can be processed.
#[derive(Debug)]
struct PendingIngestion {
    /// The logical plan awaiting ingestion
    plan: PartialLogicalPlan,

    /// Set of property derivation job IDs that must complete before this plan can be ingested
    ///
    /// As property derivation tasks complete, their job IDs are removed from this set.
    /// When the set becomes empty, the plan is ready for ingestion.
    pending_derivations: HashSet<i64>,
}

/// The central access point to the OPTD optimizer.
///
/// This struct provides the main interface to the query optimization system,
/// allowing clients to submit logical plans for optimization and receive
/// optimized physical plans in return. It contains all state necessary for
/// optimization, including channels for communication between components.
struct Optimizer<M: Memoize> {
    //
    // Core optimization components
    //
    /// The memo instance for storing optimization data
    memo: M,

    /// The rule book containing transformation and implementation rules
    rule_book: RuleBook,

    /// The engine for evaluating rules and functions
    engine: Engine<OptimizerExpander>,

    //
    // Ingestion state tracking
    //
    /// Logical plans waiting for property derivation to complete
    ///
    /// Each entry represents a plan that needs derived properties before
    /// it can be fully ingested. The associated set tracks which property
    /// derivation tasks must complete before the plan can be processed.
    pending_ingestions: Vec<PendingIngestion>,

    /// Counter for generating unique property derivation job IDs
    ///
    /// Each property derivation task gets a unique ID to correlate results
    /// with pending ingestion tasks that depend on those properties.
    next_derive_job_id: i64,

    //
    // Exploration tracking
    //
    /// Track which groups we've started exploring with transformation rules
    exploring_groups: HashSet<GroupId>,

    /// Track which goals we've started implementing with implementation rules
    exploring_goals: HashSet<Goal>,

    //
    // Subscription management
    //
    /// Subscribers to logical expressions in groups
    group_subscribers: HashMap<GroupId, Vec<Sender<LogicalExpression>>>,

    /// Subscribers to *optimized* physical expressions for goals
    goal_subscribers: HashMap<Goal, Vec<Sender<OptimizedExpression>>>,

    //
    // Communication channels
    //
    /// Engine results communication channel
    engine_result_tx: Sender<EngineResult>,
    engine_result_rx: Receiver<EngineResult>,

    /// Engine requests communication channel
    engine_request_tx: Sender<EngineRequest>,
    engine_request_rx: Receiver<EngineRequest>,

    /// Optimization requests from external clients
    optimize_rx: Receiver<OptimizationRequest>,
}

impl<M: Memoize> Optimizer<M> {
    /// Launch a new optimizer with the given memo and HIR context
    ///
    /// This method creates, initializes, and launches the optimizer in one step.
    /// It returns a sender that can be used to communicate with the optimizer.
    pub fn launch(memo: M, hir: HIR) -> Sender<OptimizationRequest> {
        // Initialize all channels
        let (engine_result_tx, engine_result_rx) = mpsc::channel(0);
        let (engine_request_tx, engine_request_rx) = mpsc::channel(0);
        let (optimize_tx, optimize_rx) = mpsc::channel(0);

        // Create the engine with the optimizer expander
        let engine = Engine::new(
            hir.context,
            OptimizerExpander {
                engine_request_tx: engine_request_tx.clone(),
            },
        );

        // Create the optimizer with initialized channels and state
        let optimizer = Self {
            // Core optimization components
            memo,
            rule_book: RuleBook::default(),
            engine,

            // Ingestion state tracking
            pending_ingestions: Vec::new(),
            next_derive_job_id: 0,

            // Exploration tracking
            exploring_groups: HashSet::new(),
            exploring_goals: HashSet::new(),

            // Subscription management
            group_subscribers: HashMap::new(),
            goal_subscribers: HashMap::new(),

            // Communication channels
            engine_result_tx,
            engine_result_rx,
            engine_request_tx,
            engine_request_rx,
            optimize_rx,
        };

        // Start the background processing loop
        tokio::spawn(async move {
            optimizer.run().await;
        });

        // Return the request sender for client communication
        optimize_tx
    }

    /// Run the optimizer's main processing loop
    ///
    /// This method continuously processes incoming requests and task results,
    /// handling optimization requests, engine results, and subscription requests.
    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(request) = self.optimize_rx.next() => {
                    match request {
                        OptimizationRequest::Optimize(logical_plan, response_sender) => {
                            self.optimize(logical_plan, response_sender).await;
                        }
                    }
                },
                Some(result) = self.engine_result_rx.next() => {
                    match result {
                        EngineResult::NewLogicalPartial(plan, group_id) => {
                            todo!("Received exploration result for group: {:?}", group_id);
                        },
                        EngineResult::NewPhysicalPartial(plan, goal) => {
                            todo!("Received implementation result for goal: {:?}", goal);
                        }
                        EngineResult::NewOptimizedExpression(expr, goal) => {
                            todo!("Received optimized expression for goal: {:?}", goal);
                        }
                        EngineResult::DeriveProperties(group_id, props) => {
                            todo!("Received properties result for group: {:?}", group_id);
                        }
                    }
                },
                Some(request) = self.engine_request_rx.next() => {
                    match request {
                        EngineRequest::SubscribeGroup(group_id, sender) => {
                            self.handle_group_subscription(group_id, sender).await;
                        },
                        EngineRequest::SubscribeGoal(goal, sender) => {
                            self.handle_goal_subscription(goal, sender).await;
                        }
                        EngineRequest::DeriveProperties(group_id, sender) => {
                            self.handle_derive_properties(group_id, sender).await;
                        }
                    }
                },
                else => break,
            }
        }
    }

    // Very important helper
    async fn ingest_logical_plan(&self, logical_plan: PartialLogicalPlan) {
        /*let res = ingest_logical_plan(&self.memo, &logical_plan)
            .await
            .expect("Failed to ingest logical plan");

        match res {
            Ok(r) => println!("Ingested logical plan successfully"),
            Err(err) => eprintln!("Failed to ingest logical plan: {:?}", err),
        }*/
    }

    /// Subscribe to optimized expressions for a specific goal.
    /// If hasn't started to explore, launch exploration.
    async fn subscribe_to_goal(
        mut engine_request_tx: Sender<EngineRequest>,
        goal: Goal,
    ) -> impl Stream<Item = OptimizedExpression> {
        let (expr_tx, expr_rx) = mpsc::channel(0);

        engine_request_tx
            .send(EngineRequest::SubscribeGoal(goal, expr_tx))
            .await
            .expect("Failed to send goal subscription - channel closed");

        expr_rx
    }

    /// Subscribe to logical expressions in a specific group.
    /// If hasn't started to explore, launch exploration.
    async fn subscribe_to_group(
        mut engine_request_tx: Sender<EngineRequest>,
        group_id: GroupId,
    ) -> impl Stream<Item = LogicalExpression> {
        let (expr_tx, expr_rx) = mpsc::channel(0);

        engine_request_tx
            .send(EngineRequest::SubscribeGroup(group_id, expr_tx))
            .await
            .expect("Failed to send group subscription - channel closed");

        expr_rx
    }

    /// Handles a property retrieval request for a specific group
    ///
    /// Retrieves the logical properties for the given group from the memo
    /// and sends them back to the requestor through the provided oneshot channel.
    async fn handle_derive_properties(
        &self,
        group_id: GroupId,
        sender: oneshot::Sender<LogicalProperties>,
    ) {
        match self.memo.get_logical_properties(group_id).await {
            Ok(props) => {
                if let Err(_) = sender.send(props) {
                    println!(
                        "Failed to send properties for group {:?}: receiver dropped",
                        group_id
                    );
                }
            }
            Err(err) => {
                println!(
                    "Failed to get properties for group {:?}: {:?}",
                    group_id, err
                );
            }
        }
    }

    /// Handles an optimize request
    ///
    /// This method initiates the optimization process for a logical plan and streams
    /// results back to the client as they become available. The client can continue
    /// receiving optimized plans indefinitely until they decide to stop listening.
    ///
    /// Each new physical plan generated is sent through the response channel,
    /// allowing clients to see optimization progress in real-time and terminate
    /// the process whenever they're satisfied with the results.
    async fn optimize(
        &self,
        logical_plan: LogicalPlan,
        mut response_tx: Sender<Result<PhysicalPlan, Error>>,
    ) {
        /*let (expr_tx, mut expr_rx) = mpsc::channel(0);

        // Ingest the logical plan
        let ingestion_result = match ingest_logical_plan(&self.memo, &logical_plan.into()).await {
            Ok(result) => result,
            Err(err) => {
                if let Err(send_err) = response_tx.send(Err(err)).await {
                    eprintln!("Failed to send error response: {}", send_err);
                }
                return;
            }
        };

        // Extract the group ID from the ingestion result
        let group_id = match ingestion_result {
            LogicalIngestion::Found(group_id, _) => group_id,
            LogicalIngestion::NeedsProperties(_) => {
                let err =
                    Error::Internal("Plan requires property derivation before optimization".into());
                if let Err(send_err) = response_tx.send(Err(err)).await {
                    eprintln!("Failed to send error response: {}", send_err);
                }
                return;
            }
        };

        let goal = Goal(group_id, PhysicalProperties(None));
        let request = EngineRequest::SubscribeGoal(goal.clone(), expr_tx);

        let mut engine_request_tx = self.engine_request_tx.clone();

        // Spawn a task to handle the optimization process
        tokio::spawn(async move {
            // Subscribe to goal expressions
            if let Err(err) = engine_request_tx.send(request).await {
                let err_msg = format!("Failed to send goal subscription request: {}", err);
                if let Err(send_err) = response_tx.send(Err(Error::Internal(err_msg))).await {
                    eprintln!("Failed to send error response: {}", send_err);
                }
                return;
            }

            // Stream optimized expressions as they arrive
            while let Some(expr) = expr_rx.next().await {
                println!("Received optimized expression: {:?}", expr);

                // Convert OptimizedExpression to PhysicalPlan
                // In a real implementation, this would involve traversing the expression
                // and building a complete PhysicalPlan
                let physical_plan = PhysicalPlan {}; // Placeholder implementation

                // Send the plan to the client
                if let Err(err) = response_tx.send(Ok(physical_plan)).await {
                    eprintln!("Client disconnected, stopping optimization stream: {}", err);
                    break;
                }
            }
        });*/
    }

    /// Handles a group subscription request
    ///
    /// Sends existing logical expressions for the group to the subscriber
    /// and initiates exploration of the group if it hasn't been explored yet.
    async fn handle_group_subscription(
        &mut self,
        group_id: GroupId,
        mut sender: Sender<LogicalExpression>,
    ) {
        // Get existing logical expressions for the group
        let exprs = match self.memo.get_all_logical_exprs(group_id).await {
            Ok(exprs) => exprs,
            Err(err) => {
                eprintln!(
                    "Failed to get logical expressions for group {:?}: {:?}",
                    group_id, err
                );
                return;
            }
        };

        // Send existing expressions to the subscriber
        for expr in exprs {
            if let Err(err) = sender.send(expr).await {
                eprintln!("Failed to send existing expression: {}", err);
                return; // Exit early if the receiver has been dropped
            }
        }

        // Add the sender to the subscribers list
        self.group_subscribers
            .entry(group_id)
            .or_insert_with(Vec::new)
            .push(sender);

        // Set up exploration of the group if not already explored
        if !self.exploring_groups.contains(&group_id) {
            self.explore_group(group_id).await;
        }
    }

    /// Handles a goal subscription request
    ///
    /// Sends the best existing physical expression for the goal to the subscriber
    /// and initiates implementation of the goal if it hasn't been launched yet.
    async fn handle_goal_subscription(
        &mut self,
        goal: Goal,
        mut sender: Sender<OptimizedExpression>,
    ) {
        // Get the best existing physical expression for the goal
        let result = match self.memo.get_best_optimized_physical_expr(&goal).await {
            Ok(result) => result,
            Err(err) => {
                eprintln!(
                    "Failed to get winning physical expr for goal {:?}: {:?}",
                    goal, err
                );
                return;
            }
        };

        // Send existing expression to the subscriber if available
        if let Some(expr) = result {
            if let Err(err) = sender.send(expr).await {
                eprintln!("Failed to send existing expression: {}", err);
                return; // Exit early if the receiver has been dropped
            }
        }

        // Add the sender to the subscribers list
        self.goal_subscribers
            .entry(goal.clone())
            .or_insert_with(Vec::new)
            .push(sender);

        // Set up implementation of the goal if not already launched
        if !self.exploring_goals.contains(&goal) {
            self.explore_goal(goal).await;
        }
    }

    /// Explores a logical group by applying transformation rules
    ///
    /// This method triggers the application of transformation rules to
    /// the logical expressions in a group, generating new equivalent
    /// logical expressions.
    async fn explore_group(&mut self, group_id: GroupId) {
        // Mark the group as explored
        self.exploring_groups.insert(group_id);

        // Set up exploration of the group
        let (expr_tx, mut expr_rx) = mpsc::channel(0);

        let transformations = self
            .rule_book
            .get_transformations()
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let request = EngineRequest::SubscribeGroup(group_id, expr_tx);
        let mut engine_request_tx = self.engine_request_tx.clone();
        let engine_result_tx = self.engine_result_tx.clone();
        let engine = self.engine.clone();

        // Spawn a task to explore the group
        tokio::spawn(async move {
            if let Err(err) = engine_request_tx.send(request).await {
                eprintln!("Failed to send group subscription request: {}", err);
                return;
            }

            while let Some(expr) = expr_rx.next().await {
                let plan: PartialLogicalPlan = expr.into();

                for rule in &transformations {
                    let rule_name = rule.0.clone();

                    tokio::spawn(capture!([engine, plan, engine_result_tx], async move {
                        engine
                            .match_and_apply_logical_rule(&rule_name, &plan)
                            .inspect(|result| {
                                if let Err(err) = result {
                                    eprintln!("Error applying rule {}: {:?}", rule_name, err);
                                }
                            })
                            .filter_map(|result| async move { result.ok() })
                            .for_each(|transformed_plan| {
                                let mut result_tx = engine_result_tx.clone();
                                async move {
                                    if let Err(err) = result_tx
                                        .send(EngineResult::NewLogicalPartial(
                                            transformed_plan,
                                            group_id,
                                        ))
                                        .await
                                    {
                                        eprintln!("Failed to send transformation result: {}", err);
                                    }
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
    async fn explore_goal(&mut self, goal: Goal) {
        self.exploring_goals.insert(goal.clone());

        // Set up implementation of the goal
        let (expr_tx, mut expr_rx) = mpsc::channel(0);

        let implementations = self
            .rule_book
            .get_implementations()
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let props = goal.1.clone();
        let request = EngineRequest::SubscribeGroup(goal.0, expr_tx);
        let mut engine_request_tx = self.engine_request_tx.clone();
        let engine_result_tx = self.engine_result_tx.clone();
        let engine = self.engine.clone();

        // Spawn a task to implement the goal
        tokio::spawn(async move {
            if let Err(err) = engine_request_tx.send(request).await {
                eprintln!(
                    "Failed to send group subscription request for goal: {}",
                    err
                );
                return;
            }

            while let Some(expr) = expr_rx.next().await {
                let plan: PartialLogicalPlan = expr.into();

                for rule in &implementations {
                    let rule_name = rule.0.clone();

                    tokio::spawn(capture!(
                        [engine, plan, engine_result_tx, goal, props],
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
                                    let mut result_tx = engine_result_tx.clone();
                                    let goal = goal.clone();
                                    async move {
                                        if let Err(err) = result_tx
                                            .send(EngineResult::NewPhysicalPartial(
                                                physical_plan,
                                                goal,
                                            ))
                                            .await
                                        {
                                            eprintln!(
                                                "Failed to send implementation result: {}",
                                                err
                                            );
                                        }
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
