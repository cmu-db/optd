use crate::{
    cir::{
        expressions::{
            LogicalExpression, LogicalExpressionId, OptimizedExpression, PhysicalExpressionId,
        },
        goal::{Goal, GoalId},
        group::GroupId,
        plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan, PhysicalPlan},
        properties::{LogicalProperties, PhysicalProperties},
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
use optd_dsl::analyzer::{context::Context, hir::HIR};
use std::collections::{HashMap, HashSet, VecDeque};
use tasks::{Task, TaskId};
use OptimizerMessage::*;

mod egest;
mod generator;
mod handlers;
mod ingest;
mod jobs;
mod memo;
mod subscriptions;
mod tasks;

/// External client request to optimize a query in the optimizer.
///
/// Defines the public API for submitting a query and receiving execution plans.
#[derive(Clone, Debug)]
pub struct OptimizeRequest {
    /// The logical plan to optimize
    pub plan: LogicalPlan,

    /// Channel for receiving optimized physical plans
    ///
    /// Streams results back as they become available, allowing clients to:
    /// * Receive progressively better plans during optimization
    /// * Terminate early when a "good enough" plan is found
    pub response_tx: Sender<PhysicalPlan>,
}

/// Messages passed within the optimization system.
///
/// Each message that includes a JobId represents the result of a completed job,
/// allowing the optimizer to track which tasks are progressing.
enum OptimizerMessage {
    /// Process an optimization request
    OptimizeRequestWrapper(OptimizeRequest, TaskId),

    /// New logical plan alternative for a group from applying transformation rules
    NewLogicalPartial(PartialLogicalPlan, GroupId, JobId),

    /// New physical implementation for a goal, awaiting recursive optimization
    NewPhysicalPartial(PartialPhysicalPlan, Goal, JobId),

    /// Fully optimized physical expression with complete costing
    NewOptimizedExpression(OptimizedExpression, Goal, JobId),

    /// Create a new group with the provided logical properties
    CreateGroup(LogicalProperties, LogicalExpression, JobId),

    /// Subscribe to logical expressions in a specific group
    SubscribeGroup(GroupId, LogicalExprContinuation, JobId),

    /// Subscribe to optimized physical implementations for a goal
    SubscribeGoal(Goal, OptimizedExprContinuation, JobId),

    /// Retrieve logical properties for a specific group
    RetrieveProperties(GroupId, oneshot::Sender<LogicalProperties>),
}

/// A message that is waiting for dependencies before it can be processed.
///
/// Tracks the set of job IDs that must exist before the message can be handled.
struct PendingMessage {
    /// The message awaiting processing
    message: OptimizerMessage,

    /// Set of job IDs whose groups must be created before this message can be processed
    pending_dependencies: HashSet<JobId>,
}

/// The central access point to the OPTD optimizer.
///
/// Provides the interface to submit logical plans for optimization and receive
/// optimized physical plans in return.
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
    pending_messages: Vec<PendingMessage>,

    /// Mapping of task IDs to their corresponding tasks
    tasks: HashMap<TaskId, Task>,

    /// Maps group IDs to their exploration task IDs
    group_explorations_task_index: HashMap<GroupId, TaskId>,

    /// Maps logical expression IDs to their exploration task IDs
    expression_exploration_task_index: HashMap<LogicalExpressionId, TaskId>,

    /// Maps optimization goals to their exploration task IDs
    goal_exploration_task_index: HashMap<GoalId, TaskId>,

    /// Maps logical expression IDs to implementation tasks with physical properties
    expression_implementation_task_index:
        HashMap<LogicalExpressionId, Vec<(PhysicalProperties, TaskId)>>,

    /// Maps physical expression IDs to costing task IDs
    expression_costing_task_index: HashMap<PhysicalExpressionId, TaskId>,

    /// Tracks all uncompleted jobs that are pending (not yet started)
    pending_jobs: HashMap<JobId, Job>,

    /// Queue of pending job IDs in order they should be scheduled
    job_schedule_queue: VecDeque<JobId>,

    /// Tracks all uncompleted jobs that are currently running
    running_jobs: HashMap<JobId, Job>,

    /// Next available task ID for task creation
    next_task_id: TaskId,

    /// Next available job ID for job creation
    next_job_id: JobId,

    //
    // Subscription management
    //
    /// Subscribers to logical expressions in groups
    group_subscribers: HashMap<GroupId, Vec<TaskId>>,

    /// Subscribers to optimized physical expressions for goals
    goal_subscribers: HashMap<GoalId, Vec<TaskId>>,

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
    /// Use `launch` to create and start the optimizer in one step.
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
            expression_exploration_task_index: HashMap::new(),
            goal_exploration_task_index: HashMap::new(),
            expression_implementation_task_index: HashMap::new(),
            expression_costing_task_index: HashMap::new(),
            pending_jobs: HashMap::new(),
            job_schedule_queue: VecDeque::new(),
            running_jobs: HashMap::new(),
            next_task_id: TaskId(0),
            next_job_id: JobId(0),

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

    /// Launch a new optimizer and return a sender for client communication
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
    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(request) = self.optimize_rx.next() => {
                    let OptimizeRequest { plan, response_tx } = request.clone();
                    let task_id = self.launch_optimize_plan_task(plan, response_tx).await;
                    let mut message_tx = self.message_tx.clone();

                    // Forward the optimization request to the message processing loop
                    // in a new coroutine to avoid a deadlock.
                    tokio::spawn(
                        async move {
                            message_tx.send(OptimizeRequestWrapper(request, task_id))
                                .await
                                .expect("Failed to forward optimize request");
                        }
                    );
                },
                Some(message) = self.message_rx.next() => {
                    match message {
                        OptimizeRequestWrapper(request, task_id_opt) => {
                            self.process_optimize_request(request.plan, request.response_tx, task_id_opt).await;
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
