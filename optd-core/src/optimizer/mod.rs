use crate::{
    cir::{
        expressions::{LogicalExpression, OptimizedExpression},
        goal::Goal,
        group::GroupId,
        plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan, PhysicalPlan},
        properties::LogicalProperties,
        rules::RuleBook,
    },
    engine::{LogicalExprContinuation, OptimizedExprContinuation},
};
use futures::channel::oneshot;
use futures::StreamExt;
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    SinkExt,
};
use jobs::{Job, JobId};
use memo::Memoize;
use merge_repr::Representative;
use optd_dsl::analyzer::{context::Context, hir::HIR};
use std::collections::{HashMap, HashSet, VecDeque};
use tasks::{Task, TaskId};
use OptimizerMessage::*;

mod egest;
mod generator;
mod handlers;
mod ingest;
mod jobs;
pub mod memo;
mod merge_repr;
mod subscriptions;
mod tasks;

/// External client request to optimize a query in the optimizer.
///
/// Defines the public API for submitting a query and receiving execution plans.
#[derive(Debug)]
pub struct OptimizeRequest {
    /// The logical plan to optimize
    pub plan: LogicalPlan,

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
/// This enum unifies all the message types that flow through the optimizer,
/// enabling centralized message handling. Each message that includes a JobId
/// represents the result of a completed job, allowing the optimizer to track
/// which tasks are progressing and update their completion status.
enum OptimizerMessage {
    /// Process an optimization request.
    ///
    /// Wraps the external client request as an internal message for consistent
    /// handling within the optimizer's message processing system.
    OptimizeRequestWrapper(OptimizeRequest),

    /// New logical plan alternative for a group from applying transformation rules.
    ///
    /// The JobId represents the completed job that produced this result.
    /// It allows the optimizer to track which tasks launched this job and
    /// update their completion status.
    NewLogicalPartial(PartialLogicalPlan, GroupId, JobId),

    /// New physical implementation for a goal, awaiting recursive optimization.
    ///
    /// The JobId represents the completed job that produced this physical implementation.
    /// This allows the optimizer to update task status and track job completion.
    ///
    /// Intermediate result without full costing or optimized child operators.
    NewPhysicalPartial(PartialPhysicalPlan, Goal, JobId),

    /// Fully optimized physical expression with complete costing.
    ///
    /// The JobId represents the completed costing job that produced this optimized expression.
    /// This allows the optimizer to track which cost evaluation job has completed.
    ///
    /// May not be the globally optimal plan for the goal.
    NewOptimizedExpression(OptimizedExpression, Goal, JobId),

    /// Create a new group with the provided logical properties.
    ///
    /// The JobId represents the completed property derivation job.
    /// This allows pending messages waiting on this job to be processed.
    ///
    /// Used when the engine derives properties for a logical expression.
    CreateGroup(LogicalProperties, LogicalExpression, JobId),

    /// Subscribe to all logical expressions in a specific group.
    ///
    /// The JobId represents the job that's requesting the subscription.
    /// This allows the job to receive notifications about expressions in the group.
    ///
    /// Provides notifications for existing and new expressions in the equivalence class.
    SubscribeGroup(GroupId, LogicalExprContinuation, JobId),

    /// Subscribe to optimized physical implementations for a goal.
    ///
    /// The JobId represents the job that's requesting the subscription.
    /// This allows the job to receive notifications about optimized expressions for the goal.
    ///
    /// Only sends notifications for plans better than previously discovered ones,
    /// implementing pruning to focus on promising candidates.
    SubscribeGoal(Goal, OptimizedExprContinuation, JobId),

    /// Retrieve logical properties for a specific group.
    ///
    /// Gets properties (schema, cardinality, statistics) needed for rules and costing.
    RetrieveProperties(GroupId, oneshot::Sender<LogicalProperties>),
}

/// A message that is waiting for dependencies before it can be processed.
///
/// This structure represents a message that requires one or more groups to be
/// created before it can be fully processed. It tracks the set of job IDs
/// that must exist before the message can be handled.
struct PendingMessage {
    /// The message awaiting processing
    message: OptimizerMessage,

    /// Set of job IDs whose groups must be created before this message can be processed
    ///
    /// As groups are created, their corresponding job IDs are removed from this set.
    /// When the set becomes empty, the message is ready for processing.
    pending_dependencies: HashSet<JobId>,
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

    /// The HIR context for rule evaluation
    hir_context: Context,

    //
    // Dependency tracking
    //
    /// Messages waiting for dependencies to be resolved
    ///
    /// Each entry represents a message that can only be processed once
    /// certain groups have been created. The associated set tracks (right now) which
    /// group creation tasks must complete before the message can be processed.
    pending_messages: Vec<PendingMessage>,

    /// Mapping of task IDs to their corresponding tasks
    ///
    /// This tracks the dependency graph of tasks, including the jobs they have launched
    /// and their relationships to other tasks. Tasks represent higher-level objectives
    /// that may involve multiple jobs (and subtasks).
    tasks: HashMap<TaskId, Task>,

    /// Maps group IDs to their associated exploration task IDs
    ///
    /// This index provides efficient lookup of the exploration task for a specific group,
    /// enabling faster processing of group-related messages and dependency tracking.
    group_explorations_task_index: HashMap<GroupId, TaskId>,

    /// Maps optimization goals to their associated exploration task IDs
    ///
    /// This index provides efficient lookup of the exploration task for a specific goal,
    /// enabling faster processing of goal-related messages and implementation tracking.
    goal_exploration_task_index: HashMap<Goal, TaskId>,

    /// Tracks all uncompleted jobs that are pending (not yet started)
    ///
    /// Maps job IDs to their job definitions.
    pending_jobs: HashMap<JobId, Job>,

    /// Queue of pending job IDs in order they should be scheduled
    ///
    /// Jobs are dequeued from this queue for scheduling in FIFO order.
    job_schedule_queue: VecDeque<JobId>,

    /// Tracks all uncompleted jobs that are currently running
    ///
    /// Jobs are removed from this map when they complete their execution, which
    /// is signaled by receiving a message with their JobId.
    running_jobs: HashMap<JobId, Job>,

    /// Next available task ID for task creation
    next_task_id: TaskId,

    /// Next available job ID for job creation
    next_job_id: JobId,

    //
    // Representative tracking
    //
    /// Maps groups to their representatives for merging
    group_repr: Representative<GroupId>,

    /// Maps goals to their representatives for merging
    goal_repr: Representative<Goal>,

    //
    // Subscription management
    //
    /// Subscribers to logical expressions in groups
    group_subscribers: HashMap<GroupId, Vec<TaskId>>,

    /// Subscribers to *optimized* physical expressions for goals
    goal_subscribers: HashMap<Goal, Vec<TaskId>>,

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
    /// Create a new optimizer instance with the given memo and HIR context
    ///
    /// This method creates and initializes all the necessary state for the optimizer
    /// but does not start processing. Use `launch` to create and start the optimizer
    /// in one step.
    fn new(
        memo: M,
        hir: HIR,
        message_tx: Sender<OptimizerMessage>,
        message_rx: Receiver<OptimizerMessage>,
        optimize_rx: Receiver<OptimizeRequest>,
    ) -> Self {
        Self {
            //
            // Core optimization components
            //
            memo,
            rule_book: RuleBook::default(),
            hir_context: hir.context,

            //
            // Dependency tracking
            //
            pending_messages: Vec::new(),
            tasks: HashMap::new(),
            group_explorations_task_index: HashMap::new(),
            goal_exploration_task_index: HashMap::new(),
            pending_jobs: HashMap::new(),
            job_schedule_queue: VecDeque::new(),
            running_jobs: HashMap::new(),
            next_task_id: 0,
            next_job_id: 0,

            //
            // Representative tracking
            //
            group_repr: Representative::new(),
            goal_repr: Representative::new(),

            //
            // Subscription management
            //
            group_subscribers: HashMap::new(),
            goal_subscribers: HashMap::new(),

            //
            // Communication channels
            //
            message_tx,
            message_rx,
            optimize_rx,
        }
    }

    /// Launch a new optimizer with the given memo and HIR context
    ///
    /// This method creates, initializes, and launches the optimizer in one step.
    /// It returns a sender that can be used to communicate with the optimizer.
    pub fn launch(memo: M, hir: HIR) -> Sender<OptimizeRequest> {
        // Initialize all channels
        let (message_tx, message_rx) = mpsc::channel(0);
        let (optimize_tx, optimize_rx) = mpsc::channel(0);

        // Create the optimizer with initialized channels and state
        let optimizer = Self::new(memo, hir, message_tx.clone(), message_rx, optimize_rx);

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
                        message_tx.send(OptimizeRequestWrapper(request))
                            .await
                            .expect("Failed to forward optimize request");
                    });
                },
                Some(message) = self.message_rx.next() => {
                    match message {
                        OptimizeRequestWrapper(request) => {
                            self.process_optimize_request(request.plan, request.response_tx).await;
                        }
                        NewLogicalPartial(plan, group_id, job_id) => {
                            self.process_new_logical_partial(plan, group_id, job_id).await;
                        },
                        NewPhysicalPartial(plan, goal, job_id) => {
                            self.process_new_physical_partial(plan, goal, job_id).await;
                        },
                        NewOptimizedExpression(expr, goal, _) => {
                            self.process_new_optimized_expr(expr, goal).await;
                        },
                        CreateGroup(props, expr, job_id) => {
                            self.process_create_group(props, expr, job_id).await;
                        },
                        SubscribeGroup(group_id, cont, job_id) => {
                            self.process_group_subscription(group_id, cont, job_id).await;
                        },
                        SubscribeGoal(goal, cont, job_id) => {
                            self.process_goal_subscription(goal, cont, job_id).await;
                        },
                        RetrieveProperties(group_id, sender) => {
                            self.process_retrieve_properties(group_id, sender).await;
                        },
                    }

                    // TODO(later): cleanup tasks, remove job from task! check if orphan and no pending jobs.
                    // TODO(later): Execute new jobs according to limit!
                },
                else => break,
            }
        }
    }
}
