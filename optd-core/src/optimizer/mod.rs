use crate::cir::{
    Goal, GroupId, LogicalExpression, LogicalPlan, LogicalProperties, OptimizedExpression,
    PartialLogicalPlan, PartialPhysicalPlan, PhysicalPlan, RuleBook,
};
use OptimizerMessage::*;
use futures::StreamExt;
use futures::channel::oneshot;
use futures::{
    SinkExt,
    channel::mpsc::{self, Receiver, Sender},
};
use memo::Memoize;
use merge_repr::Representative;
use optd_dsl::analyzer::hir::HIR;
use optd_dsl::engine::Engine;
use std::collections::{HashMap, HashSet};

mod egest;
mod handlers;
mod ingest;
mod memo;
mod merge_repr;
mod subscriptions;

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
    pub response_tx: Sender<PhysicalPlan>,
}

/// Messages passed within the optimization system.
///
/// This enum unifies all the message types that flow through the
/// optimizer, enabling centralized message handling.
#[derive(Debug)]
#[allow(unused)]
enum OptimizerMessage {
    /// Process an optimization request.
    ///
    /// Wraps the external client request as an internal message for consistent
    /// handling within the optimizer's message processing system.
    OptimizeRequest(OptimizeRequest),

    /// Convert an optimized expression to a physical plan and send it to the client.
    ///
    /// This message is triggered when a new optimized expression is available
    /// and needs to be materialized into a complete physical plan before
    /// being sent to the client through the result channel.
    EgestOptimized(OptimizedExpression, Sender<PhysicalPlan>),

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
    SubscribeGroup(GroupId, Sender<LogicalExpression>),

    /// Subscribe to optimized physical implementations for a goal.
    ///
    /// Only sends notifications for plans better than previously discovered ones,
    /// implementing pruning to focus on promising candidates.
    SubscribeGoal(Goal, Sender<OptimizedExpression>),

    /// Retrieve logical properties for a specific group.
    ///
    /// Gets properties (schema, cardinality, statistics) needed for rules and costing.
    #[allow(dead_code)]
    RetrieveProperties(GroupId, oneshot::Sender<LogicalProperties>),
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
pub struct Optimizer<M: Memoize> {
    //
    // Core optimization components
    //
    /// The memo instance for storing optimization data
    memo: M,

    /// The rule book containing transformation and implementation rules
    rule_book: RuleBook,

    /// The engine for evaluating rules and functions
    engine: Engine,

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
    // Representative tracking
    //
    /// Maps groups to their representatives for merging
    group_repr: Representative<GroupId>,

    /// Maps goals to their representatives for merging
    goal_repr: Representative<Goal>,

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

        // Create the engine with the HIR context
        let engine = Engine::new(hir.context);

        // Create the optimizer with initialized channels and state
        let optimizer = Self {
            // Core optimization components
            memo,
            rule_book: RuleBook::default(),
            engine,

            // Dependency tracking
            pending_messages: Vec::new(),
            next_dep_id: 0,

            // Representative tracking
            group_repr: Representative::new(),
            goal_repr: Representative::new(),

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
                        message_tx.send(OptimizeRequest(request))
                            .await
                            .expect("Failed to forward optimize request");
                    });
                },
                Some(message) = self.message_rx.next() => {
                    match message {
                        OptimizeRequest(request) => {
                            self.process_optimize_request(request.logical_plan, request.response_tx).await;
                        }
                        EgestOptimized(expr, sender) => {
                            self.process_egest_optimized(expr, sender).await;
                        },
                        NewLogicalPartial(plan, group_id) => {
                            self.process_new_logical_partial(plan, group_id).await;
                        },
                        NewPhysicalPartial(plan, goal) => {
                            self.process_new_physical_partial(plan, goal).await;
                        },
                        NewOptimizedExpression(expr, goal) => {
                            self.process_new_optimized_expr(expr, goal).await;
                        },
                        CreateGroup(props, expr, job_id) => {
                            self.process_create_group(props, expr, job_id).await;
                        },
                        SubscribeGroup(group_id, sender) => {
                            self.process_group_subscription(group_id, sender).await;
                        },
                        SubscribeGoal(goal, sender) => {
                            self.process_goal_subscription(goal, sender).await;
                        },
                        RetrieveProperties(group_id, sender) => {
                            self.process_retrieve_properties(group_id, sender).await;
                        },
                    }
                },
                else => break,
            }
        }
    }
}
