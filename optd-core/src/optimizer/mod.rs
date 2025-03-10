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
};
use egest::egest_best_plan;
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

mod egest;
mod expander;
mod ingest;
mod memo;

/// External client request to optimize a query in the optimizer.
///
/// Defines the public API for submitting a query and receiving execution plans.
#[derive(Debug)]
pub struct OptimizeRequest {
    /// The logical plan to optimize
    pub logical_plan: LogicalPlan,

    /// Channel for receiving optimized physical plans
    ///
    /// Streams results back as they become available, allowing clients to:
    /// * Receive progressively better plans during optimization
    /// * Terminate early when a "good enough" plan is found
    /// * Control optimization duration by consuming or dropping the receiver
    pub result_sender: Sender<PhysicalPlan>,
}

/// Messages passed within the optimization system.
///
/// This enum unifies all the message types that flow through the
/// optimizer, enabling centralized message handling.
#[derive(Debug)]
enum OptimizerMessage {
    /// Process an optimization request.
    ///
    /// Wraps the external client request as an internal message for consistent
    /// handling within the optimizer's message processing system.
    OptimizeRequest(OptimizeRequest),

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

    /// Create a new group with the provided logical properties.
    ///
    /// Used when the engine derives properties for a logical expression.
    /// Includes the job ID to track dependency resolution.
    CreateGroup(LogicalProperties, LogicalExpression, i64),

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
    RetrieveProperties(GroupId, oneshot::Sender<LogicalProperties>),
}

/// Result type for logical plan ingestion
enum IngestionResult {
    /// Plan was successfully ingested
    Success(GroupId, bool),

    /// Plan requires dependencies to be created
    NeedsDependencies(HashSet<i64>),
}

/// A message that is waiting for dependencies before it can be processed.
///
/// This structure represents a message that requires one or more groups to be
/// created before it can be fully processed. It tracks the set of group IDs
/// that must exist before the message can be handled.
struct PendingMessage {
    /// The message awaiting processing
    message: OptimizerMessage,

    /// Set of job IDs whose groups must be created before this message can be processed
    ///
    /// As groups are created, their corresponding job IDs are removed from this set.
    /// When the set becomes empty, the message is ready for processing.
    pending_dependencies: HashSet<i64>,
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
    // Dependency tracking
    //
    /// Messages waiting for dependencies to be resolved
    ///
    /// Each entry represents a message that can only be processed once
    /// certain groups have been created. The associated set tracks which
    /// group creation tasks must complete before the message can be processed.
    pending_messages: Vec<PendingMessage>,

    /// Counter for generating unique job IDs
    ///
    /// Each job (like property derivation) gets a unique ID to correlate
    /// results with pending messages that depend on it.
    next_dep_id: i64,

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
    /// Optimizer message communication channel
    message_tx: Sender<OptimizerMessage>,
    message_rx: Receiver<OptimizerMessage>,

    /// Optimization requests from external clients
    optimize_rx: Receiver<OptimizeRequest>,
}

impl<M: Memoize> Optimizer<M> {
    /// Launch a new optimizer with the given memo and HIR context
    ///
    /// This method creates, initializes, and launches the optimizer in one step.
    /// It returns a sender that can be used to communicate with the optimizer.
    pub fn launch(memo: M, hir: HIR) -> Sender<OptimizeRequest> {
        // Initialize all channels
        let (message_tx, message_rx) = mpsc::channel(0);
        let (optimize_tx, optimize_rx) = mpsc::channel(0);

        // Create the engine with the optimizer expander
        let engine = Engine::new(
            hir.context,
            OptimizerExpander {
                message_tx: message_tx.clone(),
            },
        );

        // Create the optimizer with initialized channels and state
        let optimizer = Self {
            // Core optimization components
            memo,
            rule_book: RuleBook::default(),
            engine,

            // Dependency tracking
            pending_messages: Vec::new(),
            next_dep_id: 0,

            // Exploration tracking
            exploring_groups: HashSet::new(),
            exploring_goals: HashSet::new(),

            // Subscription management
            group_subscribers: HashMap::new(),
            goal_subscribers: HashMap::new(),

            // Communication channels
            message_tx,
            message_rx,
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
                    let mut message_tx = self.message_tx.clone();
                    tokio::spawn(async move {
                        message_tx.send(OptimizerMessage::OptimizeRequest(request))
                            .await
                            .expect("Failed to forward optimize request");
                    });
                },
                Some(message) = self.message_rx.next() => {
                    match message {
                        OptimizerMessage::OptimizeRequest(request) => {
                            self.process_optimize_request(request.logical_plan, request.result_sender).await;
                        }
                        OptimizerMessage::NewLogicalPartial(plan, group_id) => {
                            self.process_new_logical_partial(plan, group_id).await;
                        },
                        OptimizerMessage::NewPhysicalPartial(plan, goal) => {
                            self.process_new_physical_partial(plan, goal).await;
                        },
                        OptimizerMessage::NewOptimizedExpression(expr, goal) => {
                            self.process_new_optimized_expression(expr, goal).await;
                        },
                        OptimizerMessage::CreateGroup(props, expr, job_id) => {
                            self.process_create_group(props, expr, job_id).await;
                        },
                        OptimizerMessage::SubscribeGroup(group_id, sender) => {
                            self.process_group_subscription(group_id, sender).await;
                        },
                        OptimizerMessage::SubscribeGoal(goal, sender) => {
                            self.process_goal_subscription(goal, sender).await;
                        },
                        OptimizerMessage::RetrieveProperties(group_id, sender) => {
                            self.process_retrieve_properties(group_id, sender).await;
                        },
                    }
                },
                else => break,
            }
        }
    }

    /// Handles an optimize request
    ///
    /// This method initiates the optimization process for a logical plan and streams
    /// results back to the client as they become available.
    async fn process_optimize_request(
        &mut self,
        logical_plan: LogicalPlan,
        response_tx: Sender<PhysicalPlan>,
    ) {
        // Convert the logical plan to a partial plan
        let partial_plan = PartialLogicalPlan::from(logical_plan);

        // Try to ingest the plan
        match self.process_ingest_logical_plan(partial_plan).await {
            IngestionResult::Success(group_id, _) => {
                // Plan was ingested successfully, subscribe to the goal
                let goal = Goal(group_id, PhysicalProperties(None));
                let mut expr_stream = self.subscribe_to_goal(goal);

                tokio::spawn(async move {
                    // Forward optimized expressions to the client
                    while let Some(expr) = expr_stream.next().await {
                        todo!("cannot access memo in coroutine: have a materilaize message!")
                        /*      // Convert OptimizedExpression to PhysicalPlan
                        let physical_plan = egest_best_plan(&self.memo, &expr)
                            .await
                            .expect("Failed to egest best plan")
                            .expect("Optimization not complete");

                        response_tx
                            .send(physical_plan)
                            .await
                            .expect("Failed to send physical plan");*/
                    }
                });
            }
            IngestionResult::NeedsDependencies(dependencies) => {
                // Store a message to handle optimization once dependencies are resolved
                todo!("do")
            }
        }
    }

    /// Process a logical plan for ingestion into the memo
    ///
    /// This method takes a logical plan and either ingests it directly or
    /// initiates property derivation if needed.
    ///
    /// Returns an IngestionResult indicating either success or the dependencies needed.
    async fn process_ingest_logical_plan(
        &mut self,
        logical_plan: PartialLogicalPlan,
    ) -> IngestionResult {
        let ingest_result = ingest_logical_plan(&self.memo, &logical_plan)
            .await
            .expect("Failed to ingest logical plan");

        match ingest_result {
            LogicalIngestion::Found(group_id, is_new_expr) => {
                IngestionResult::Success(group_id, is_new_expr)
            }
            LogicalIngestion::NeedsProperties(expressions) => {
                let pending_dependencies = expressions
                    .into_iter()
                    .map(|expr| {
                        let job_id = self.next_dep_id;
                        self.next_dep_id += 1;

                        let mut message_tx = self.message_tx.clone();
                        let engine = self.engine.clone();

                        tokio::spawn(async move {
                            let properties = engine
                                .derive_properties(&expr.clone().into())
                                .await
                                .expect("Failed to derive properties");

                            message_tx
                                .send(OptimizerMessage::CreateGroup(properties, expr, job_id))
                                .await
                                .expect("Failed to send CreateGroup message");
                        });

                        job_id
                    })
                    .collect();

                IngestionResult::NeedsDependencies(pending_dependencies)
            }
        }
    }

    /// Process a CreateGroup message to create a new group with derived properties
    ///
    /// This method handles group creation for expressions with derived properties
    /// and updates any pending messages that depend on this group.
    async fn process_create_group(
        &mut self,
        properties: LogicalProperties,
        expression: LogicalExpression,
        job_id: i64,
    ) {
        self.memo
            .create_group_with(&expression, &properties)
            .await
            .expect("Failed to create group");

        self.process_dependency_resolution(job_id).await;
    }

    /// Process resolution of a dependency
    ///
    /// This method is called when a group creation job completes. It updates all
    /// pending messages that were waiting for this job and processes any that
    /// are now ready (have no more pending dependencies).
    async fn process_dependency_resolution(&mut self, completed_job_id: i64) {
        // Find all pending messages that were waiting for this job
        let mut ready_indices = Vec::new();

        for (i, pending) in self.pending_messages.iter_mut().enumerate() {
            // Remove the completed job from the pending dependencies
            pending.pending_dependencies.remove(&completed_job_id);

            // If there are no more pending dependencies, this message is ready
            if pending.pending_dependencies.is_empty() {
                ready_indices.push(i);
            }
        }

        // Process all ready messages (in reverse order to avoid index issues when removing)
        for i in ready_indices.iter().rev() {
            // Take ownership of the message
            let pending = self.pending_messages.swap_remove(*i);

            // Re-send the message to be processed
            let mut message_tx = self.message_tx.clone();
            tokio::spawn(async move {
                if let Err(err) = message_tx.send(pending.message).await {
                    eprintln!("Failed to re-send ready message: {}", err);
                }
            });
        }
    }

    /// Process a new logical partial plan from transformation rules
    ///
    /// This method handles new logical plan alternatives discovered through
    /// transformation rule application.
    async fn process_new_logical_partial(&mut self, plan: PartialLogicalPlan, group_id: GroupId) {
        todo!()
    }

    /// Process a new physical partial plan from implementation rules
    ///
    /// This method handles new physical implementations discovered through
    /// implementation rule application.
    async fn process_new_physical_partial(&mut self, plan: PartialPhysicalPlan, goal: Goal) {
        todo!()
    }

    /// Process a new optimized expression
    ///
    /// This method handles fully optimized physical expressions with cost information.
    async fn process_new_optimized_expression(&mut self, expr: OptimizedExpression, goal: Goal) {
        todo!()
    }

    /// Subscribe to optimized expressions for a specific goal.
    /// If group hasn't started to explore, launch exploration.
    fn subscribe_to_goal(&mut self, goal: Goal) -> impl Stream<Item = OptimizedExpression> {
        let (expr_tx, expr_rx) = mpsc::channel(0);

        // Add the sender to goal subscribers list
        self.goal_subscribers
            .entry(goal.clone())
            .or_insert_with(Vec::new)
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
                .send(OptimizerMessage::SubscribeGoal(goal_clone, expr_tx_clone))
                .await
                .expect("Failed to send goal subscription - channel closed");
        });

        expr_rx
    }

    /// Subscribe to logical expressions in a specific group.
    /// If group hasn't started to explore, launch exploration.
    fn subscribe_to_group(&mut self, group_id: GroupId) -> impl Stream<Item = LogicalExpression> {
        let (expr_tx, expr_rx) = mpsc::channel(0);

        // Add the sender to group subscribers list
        self.group_subscribers
            .entry(group_id)
            .or_insert_with(Vec::new)
            .push(expr_tx.clone());

        // Launch exploration if this group isn't being explored yet
        if !self.exploring_groups.contains(&group_id) {
            self.explore_group(group_id);
        }

        // Send the subscription request to the engine
        let mut message_tx = self.message_tx.clone();
        let expr_tx_clone = expr_tx.clone();

        tokio::spawn(async move {
            message_tx
                .send(OptimizerMessage::SubscribeGroup(group_id, expr_tx_clone))
                .await
                .expect("Failed to send group subscription - channel closed");
        });

        expr_rx
    }

    /// Handles a property retrieval request for a specific group
    ///
    /// Retrieves the logical properties for the given group from the memo
    /// and sends them back to the requestor through the provided oneshot channel.
    async fn process_retrieve_properties(
        &self,
        group_id: GroupId,
        sender: oneshot::Sender<LogicalProperties>,
    ) {
        let props = self
            .memo
            .get_logical_properties(group_id)
            .await
            .expect("Failed to get logical properties");
        sender
            .send(props)
            .expect("Failed to send properties - channel closed");
    }

    /// Handles a group subscription request
    ///
    /// Sends existing logical expressions for the group to the subscriber
    /// and initiates exploration of the group if it hasn't been explored yet.
    async fn process_group_subscription(
        &mut self,
        group_id: GroupId,
        mut sender: Sender<LogicalExpression>,
    ) {
        todo!("review");
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
            self.explore_group(group_id);
        }
    }

    /// Handles a goal subscription request
    ///
    /// Sends the best existing physical expression for the goal to the subscriber
    /// and initiates implementation of the goal if it hasn't been launched yet.
    async fn process_goal_subscription(
        &mut self,
        goal: Goal,
        mut sender: Sender<OptimizedExpression>,
    ) {
        todo!("review");

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
            self.explore_goal(goal);
        }
    }

    /// Explores a logical group by applying transformation rules
    ///
    /// This method triggers the application of transformation rules to
    /// the logical expressions in a group, generating new equivalent
    /// logical expressions.
    fn explore_group(&mut self, group_id: GroupId) {
        // Mark the group as exploring
        self.exploring_groups.insert(group_id);

        // Set up exploration of the group
        let (expr_tx, mut expr_rx) = mpsc::channel(0);

        let transformations = self
            .rule_book
            .get_transformations()
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let mut message_tx = self.message_tx.clone();
        let engine = self.engine.clone();

        // Spawn a task to explore the group
        tokio::spawn(async move {
            // Subscribe to the group
            message_tx
                .send(OptimizerMessage::SubscribeGroup(group_id, expr_tx))
                .await
                .expect("Failed to send group subscription request");

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
                                    if let Err(err) = result_tx
                                        .send(OptimizerMessage::NewLogicalPartial(
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
    fn explore_goal(&mut self, goal: Goal) {
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
        let mut message_tx = self.message_tx.clone();
        let engine = self.engine.clone();

        // Spawn a task to implement the goal
        tokio::spawn(async move {
            // Subscribe to the group
            message_tx
                .send(OptimizerMessage::SubscribeGroup(goal.0, expr_tx))
                .await
                .expect("Failed to send group subscription request for goal");

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
                                        if let Err(err) = result_tx
                                            .send(OptimizerMessage::NewPhysicalPartial(
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
