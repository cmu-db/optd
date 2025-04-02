use crate::cir::{
    Cost, Goal, GoalId, GroupId, LogicalExpressionId, LogicalPlan, LogicalProperties,
    PartialLogicalPlan, PartialPhysicalPlan, PhysicalExpressionId, PhysicalPlan, RuleBook,
};
use crate::error::Error;
use crate::memo::Memoize;
use EngineMessageKind::*;
use futures::StreamExt;
use futures::channel::oneshot;
use futures::{
    SinkExt,
    channel::mpsc::{self, Receiver, Sender},
};
use jobs::{Job, JobId};
use optd_dsl::analyzer::hir::Value;
use optd_dsl::analyzer::{context::Context, hir::HIR};
use optd_dsl::engine::{Continuation, EngineResponse};
use std::collections::{HashMap, HashSet, VecDeque};
use tasks::{Task, TaskId};

mod egest;
mod handlers;
mod ingest;
mod jobs;
mod merge;
mod subscriptions;
mod tasks;
mod tasks_deleted;

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
struct EngineMessage {
    /// The job ID of the sender.
    pub job_id: JobId,
    /// The kind of message being sent to the optimizer engine.
    pub kind: EngineMessageKind,
}

impl EngineMessage {
    /// Create a new engine message with the specified job ID and kind.
    pub fn new(job_id: JobId, kind: EngineMessageKind) -> Self {
        Self { job_id, kind }
    }
}

/// Messages sent to the optimizer by the DSL engine to change the state of the memo.
enum EngineMessageKind {
    /// New logical plan alternative for a group from applying transformation rules.
    /// Access the parent of related task (ExploreGroup) to get the group id.
    /// Possible sources: StartTransformRule | ContinueWithLogical
    NewLogicalPartial(PartialLogicalPlan, GroupId),

    /// New physical implementation for a goal, awaiting recursive optimization.
    /// Access the parent of related task (OptimizeGoal) to get the goal id.
    /// Possible sources: StartImplementRuleJob | ContinueWithLogical
    NewPhysicalPartial(PartialPhysicalPlan, GoalId),

    /// Fully optimized physical expression with complete costing.
    /// Possible sources: CostExpression  | ContinueWithCostedPhysical
    NewCostedPhysical(PhysicalExpressionId, Cost),

    /// Subscribe to logical expressions in a specific group.
    SubscribeGroup(
        GroupId,
        Continuation<Value, EngineResponse<EngineMessageKind>>,
    ),

    /// Subscribe to costed physical expressions for a goal.
    // TODO(yuchen): either pass in the budget or as part of the continuation.
    SubscribeGoal(Goal, Continuation<Value, EngineResponse<EngineMessageKind>>),

    /// Retrieve logical properties for a specific partial logical plan.
    // TODO(yuchen): This should be partial logical plan.
    #[allow(dead_code)]
    RetrieveProperties(GroupId, oneshot::Sender<LogicalProperties>),

    /// Associate logical properties with a group.
    /// Note: logical property are on-demand computed.
    #[allow(dead_code)]
    NewProperties(GroupId, LogicalProperties),

    // TODO(yuchen): Remove this once we refactor ingestion.
    /// Process an optimization request.
    OptimizeRequestWrapper(OptimizeRequest, TaskId),
    /// Create a new group with the provided logical properties.
    CreateGroup(LogicalExpressionId, LogicalProperties),
}

/// A message that is waiting for dependencies before it can be processed.
///
/// Tracks the set of job IDs that must exist before the message can be handled.
struct PendingMessage {
    /// The message stashed for later processing.
    message: EngineMessage,

    /// Set of job IDs whose groups must be created before this message can be processed.
    pending_dependencies: HashSet<JobId>,
}

/// The central access point to the optimizer.
///
/// Provides the interface to submit logical plans for optimization and receive
/// optimized physical plans in return.
pub struct Optimizer<M: Memoize> {
    // Core components.
    memo: M,
    rule_book: RuleBook,
    hir_context: Context,

    // Message handling.
    pending_messages: Vec<PendingMessage>,
    message_tx: Sender<EngineMessage>,
    message_rx: Receiver<EngineMessage>,
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
    // TODO(yuchen): Get rid of these when we fully switch to in-out subscription.
    group_subscribers: HashMap<GroupId, Vec<TaskId>>,
    goal_subscribers: HashMap<GoalId, Vec<TaskId>>,

    // Task index
    task_index: HashMap<TaskId, Task>,
}

impl<M: Memoize> Optimizer<M> {
    /// Create a new optimizer instance with the given memo and HIR context.
    ///
    /// Use `launch` to create and start the optimizer.
    fn new(
        memo: M,
        hir: HIR,
        message_tx: Sender<EngineMessage>,
        message_rx: Receiver<EngineMessage>,
        optimize_rx: Receiver<OptimizeRequest>,
    ) -> Self {
        Self {
            // Core components.
            memo,
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

            // Task index
            task_index: HashMap::new(),
        }
    }

    /// Launch a new optimizer and return a sender for client communication.
    pub fn launch(memo: M, hir: HIR) -> Sender<OptimizeRequest> {
        let (message_tx, message_rx) = mpsc::channel(0);
        let (optimize_tx, optimize_rx) = mpsc::channel(0);

        // Start the background processing loop.
        let optimizer = Self::new(memo, hir, message_tx.clone(), message_rx, optimize_rx);
        tokio::spawn(async move {
            // TODO(Alexis): If an error occurs we could restart or reboot the memo.
            // Rather than failing (e.g. memo could be distributed).
            optimizer.run().await.expect("Optimizer failure");
        });

        optimize_tx
    }

    /// Run the optimizer's main processing loop.
    async fn run(mut self) -> Result<(), Error> {
        loop {
            tokio::select! {
                Some(request) = self.optimize_rx.next() => {
                    let OptimizeRequest { plan, response_tx } = request.clone();
                    let task_id = self.create_optimize_plan_task(plan, response_tx).await;
                    let mut message_tx = self.message_tx.clone();

                    // Forward the optimization request to the message processing loop
                    // in a new coroutine to avoid a deadlock.
                    tokio::spawn(
                        async move {
                            message_tx.send(EngineMessage::new(JobId(-1), OptimizeRequestWrapper(request, task_id)))
                                .await
                                .expect("Failed to forward optimize request");
                        }
                    );
                },
                Some(message) = self.message_rx.next() => {
                    let EngineMessage {
                        job_id,
                        kind,
                    } = message;
                    // Process the next message in the channel.
                    match kind {
                        OptimizeRequestWrapper(request, task_id_opt) => {
                            self.process_optimize_request(request.plan, request.response_tx, task_id_opt).await?;
                        }
                        NewLogicalPartial(plan, group_id) => {
                            if self.get_related_task(message.job_id).is_some() {
                                self.process_new_logical_partial(plan, group_id, job_id).await?;
                                self.complete_job(job_id).await?;
                            }
                        }
                        NewPhysicalPartial(plan, goal_id) => {
                            if self.get_related_task(job_id).is_some() {
                                self.process_new_physical_partial(plan, goal_id, job_id).await?;
                                self.complete_job(job_id).await?;
                            }
                        }
                        NewCostedPhysical(expression_id, cost) => {
                            if self.get_related_task(job_id).is_some() {
                                self.process_new_costed_physical(expression_id, cost).await?;
                                self.complete_job(job_id).await?;
                            }
                        }
                        CreateGroup(expression_id, properties) => {
                            if self.get_related_task(job_id).is_some() {
                                self.process_create_group(expression_id, &properties, job_id).await?;
                                self.complete_job(job_id).await?;
                            }
                        }
                        SubscribeGroup(group_id, continuation) => {
                            if self.get_related_task(job_id).is_some() {
                                self.process_group_subscription(group_id, continuation, job_id).await?;
                                self.complete_job(job_id).await?;
                            }
                        }
                        SubscribeGoal(goal, continuation) => {
                            if self.get_related_task(job_id).is_some() {
                                self.process_goal_subscription(&goal, continuation, job_id).await?;
                                self.complete_job(job_id).await?;
                            }
                        }
                        RetrieveProperties(group_id, sender) => {
                            self.process_retrieve_properties(group_id, sender).await?;
                        }
                        NewProperties(group_id, properties) => {
                            if self.get_related_task(job_id).is_some() {
                                self.process_new_properties(group_id, properties, job_id).await?;
                                self.complete_job(job_id).await?;
                            }
                        }
                    };

                    // Launch pending jobs according to a policy (currently FIFO).
                    self.launch_pending_jobs().await?;
                },
                else => break Ok(()),
            }
        }
    }
}
