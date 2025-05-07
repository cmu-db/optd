use crate::{
    catalog::Catalog,
    cir::{
        Cost, Goal, GoalId, GroupId, LogicalExpressionId, LogicalPlan, LogicalProperties,
        PartialLogicalPlan, PartialPhysicalPlan, PhysicalExpressionId, PhysicalPlan, RuleBook,
    },
    dsl::{
        analyzer::hir::{HIR, Value, context::Context},
        engine::{Continuation, EngineResponse},
    },
    memo::Memo,
};
use errors::OptimizeError;
use jobs::{Job, JobId};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};
use tasks::{Task, TaskId};
use tokio::sync::mpsc::{self, Receiver, Sender};

mod errors;
mod handlers;
mod hir_cir;
mod jobs;
mod memo_io;
mod merge;
mod tasks;

/// Default maximum number of concurrent jobs to run in the optimizer.
const DEFAULT_MAX_CONCURRENT_JOBS: usize = 1000;

/// External client request to optimize a query in the optimizer.
///
/// Defines the public API for submitting a query and receiving execution plans.
#[derive(Clone, Debug)]
pub struct OptimizeRequest {
    /// The logical plan to optimize.
    pub plan: LogicalPlan,
    /// Channel for receiving optimized physical plans.
    ///
    /// Streams results back as they become available, allowing clients to:
    /// * Receive progressively better plans during optimization.
    /// * Terminate early when a "good enough" plan is found.
    pub response_tx: Sender<PhysicalPlan>,
}

/// Messages passed within the optimization system.
///
/// Each message that includes a JobId represents the result of a completed job,
/// allowing the optimizer to track which tasks are progressing.
enum OptimizerMessage {
    /// Process an optimization request.
    OptimizeRequestWrapper(OptimizeRequest, TaskId),
    /// New logical plan alternative for a group from applying transformation rules.
    NewLogicalPartial(PartialLogicalPlan, GroupId, JobId),
    /// New physical implementation for a goal, awaiting recursive optimization.
    NewPhysicalPartial(PartialPhysicalPlan, GoalId, JobId),
    /// Fully optimized physical expression with complete costing.
    NewCostedPhysical(PhysicalExpressionId, Cost, JobId),
    /// Create a new group with the provided logical properties.
    CreateGroup(LogicalExpressionId, LogicalProperties, JobId),
    /// Subscribe to logical expressions in a specific group.
    SubscribeGroup(
        GroupId,
        Continuation<Value, EngineResponse<OptimizerMessage>>,
        JobId,
    ),
    /// Subscribe to costed physical expressions for a goal.
    SubscribeGoal(
        Goal,
        Continuation<Value, EngineResponse<OptimizerMessage>>,
        JobId,
    ),
    /// Retrieve logical properties for a specific group.
    RetrieveProperties(GroupId, Sender<LogicalProperties>),
}

/// A message that is waiting for dependencies before it can be processed.
///
/// Tracks the set of job IDs that must exist before the message can be handled.
struct PendingMessage {
    /// The message stashed for later processing.
    message: OptimizerMessage,

    /// Set of job IDs whose groups must be created before this message can be processed.
    pending_dependencies: HashSet<JobId>,
}

/// The central access point to the optimizer.
///
/// Provides the interface to submit logical plans for optimization and receive
/// optimized physical plans in return.
pub struct Optimizer<M: Memo> {
    // Core components.
    memo: M,
    catalog: Arc<dyn Catalog>,
    rule_book: RuleBook,
    hir_context: Context,

    // Message handling.
    pending_messages: Vec<PendingMessage>,
    message_tx: Sender<OptimizerMessage>,
    message_rx: Receiver<OptimizerMessage>,
    optimize_rx: Receiver<OptimizeRequest>,

    // Task management.
    tasks: HashMap<TaskId, Task>,
    next_task_id: TaskId,

    // Job management.
    pending_jobs: HashMap<JobId, Job>,
    job_schedule_queue: VecDeque<JobId>,
    running_jobs: HashMap<JobId, Job>,
    next_job_id: JobId,
    max_concurrent_jobs: usize,

    // Task indexing.
    group_exploration_task_index: HashMap<GroupId, TaskId>,
    goal_optimization_task_index: HashMap<GoalId, TaskId>,
    cost_expression_task_index: HashMap<PhysicalExpressionId, TaskId>,

    // Subscriptions.
    group_subscribers: HashMap<GroupId, Vec<TaskId>>,
    goal_subscribers: HashMap<GoalId, Vec<TaskId>>,
}

impl<M: Memo> Optimizer<M> {
    /// Create a new optimizer instance with the given memo and HIR context.
    ///
    /// Use `launch` to create and start the optimizer.
    fn new(
        memo: M,
        hir: HIR,
        catalog: Arc<dyn Catalog>,
        message_tx: Sender<OptimizerMessage>,
        message_rx: Receiver<OptimizerMessage>,
        optimize_rx: Receiver<OptimizeRequest>,
    ) -> Self {
        Self {
            // Core components.
            memo,
            catalog,
            rule_book: RuleBook::default(),
            hir_context: hir.context,

            // Message handling.
            pending_messages: Vec::new(),
            message_tx,
            message_rx,
            optimize_rx,

            // Task management.
            tasks: HashMap::new(),
            next_task_id: TaskId(0),

            // Job management.
            pending_jobs: HashMap::new(),
            job_schedule_queue: VecDeque::new(),
            running_jobs: HashMap::new(),
            next_job_id: JobId(0),
            max_concurrent_jobs: DEFAULT_MAX_CONCURRENT_JOBS,

            // Task indexing.
            group_exploration_task_index: HashMap::new(),
            goal_optimization_task_index: HashMap::new(),
            cost_expression_task_index: HashMap::new(),

            // Subscriptions.
            group_subscribers: HashMap::new(),
            goal_subscribers: HashMap::new(),
        }
    }

    /// Launch a new optimizer and return a sender for client communication.
    pub fn launch(memo: M, catalog: Arc<dyn Catalog>, hir: HIR) -> Sender<OptimizeRequest> {
        let (message_tx, message_rx) = mpsc::channel(0);
        let (optimize_tx, optimize_rx) = mpsc::channel(0);

        // Start the background processing loop.
        let optimizer = Self::new(
            memo,
            hir,
            catalog,
            message_tx.clone(),
            message_rx,
            optimize_rx,
        );

        tokio::spawn(async move {
            // TODO: If an error occurs we could restart or reboot the memo.
            // Rather than failing (e.g. memo could be distributed).
            optimizer.run().await.expect("Optimizer failure");
        });

        optimize_tx
    }

    /// Run the optimizer's main processing loop.
    async fn run(mut self) -> Result<(), OptimizeError> {
        use OptimizerMessage::*;

        loop {
            tokio::select! {
                Some(request) = self.optimize_rx.recv() => {
                    /*let OptimizeRequest { plan, response_tx } = request.clone();
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
                    );*/
                },
                Some(message) = self.message_rx.recv() => {
                    // Process the next message in the channel.
                    match message {
                        OptimizeRequestWrapper(request, task_id_opt) => {
                            //self.process_optimize_request(request.plan, request.response_tx, task_id_opt).await?;
                        }
                        NewLogicalPartial(plan, group_id, job_id) => {
                            /*if self.get_related_task(job_id).is_some() {
                                self.process_new_logical_partial(plan, group_id, job_id).await?;
                                self.complete_job(job_id).await?;
                            }*/
                        }
                        NewPhysicalPartial(plan, goal_id, job_id) => {
                            /*if self.get_related_task(job_id).is_some() {
                                self.process_new_physical_partial(plan, goal_id, job_id).await?;
                                self.complete_job(job_id).await?;
                            }*/
                        }
                        NewCostedPhysical(expression_id, cost, job_id) => {
                            /*if self.get_related_task(job_id).is_some() {
                                self.process_new_costed_physical(expression_id, cost).await?;
                                self.complete_job(job_id).await?;
                            }*/
                        }
                        CreateGroup( expression_id, properties, job_id) => {
                            /*if self.get_related_task(job_id).is_some() {
                                self.process_create_group(expression_id, &properties, job_id).await?;
                                self.complete_job(job_id).await?;
                            }*/
                        }
                        SubscribeGroup(group_id, continuation, job_id) => {
                            /*if self.get_related_task(job_id).is_some() {
                                self.process_group_subscription(group_id, continuation, job_id).await?;
                                self.complete_job(job_id).await?;
                            }*/
                        }
                        SubscribeGoal(goal, continuation, job_id) => {
                            /*if self.get_related_task(job_id).is_some() {
                                self.process_goal_subscription(&goal, continuation, job_id).await?;
                                self.complete_job(job_id).await?;
                            }*/
                        }
                        RetrieveProperties(group_id, sender) => {
                            //self.process_retrieve_properties(group_id, sender).await?;
                        }
                    };

                    // Launch pending jobs according to a policy (currently FIFO).
                    // self.launch_pending_jobs().await?;
                },
                else => break Ok(()),
            }
        }
    }
}
