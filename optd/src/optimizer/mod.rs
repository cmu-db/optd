use crate::{
    catalog::Catalog,
    cir::*,
    dsl::analyzer::hir::{HIR, context::Context},
    memo::Memo,
};
use hashbrown::{HashMap, HashSet};
use hir_cir::extract_rulebook_from_hir;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::mpsc::{self, Receiver, Sender};

mod handlers;
pub mod hir_cir;
mod jobs;
mod memo_io;
mod merge;
mod retriever;
mod tasks;

use jobs::{Job, JobId, LogicalContinuation};
use retriever::OptimizerRetriever;
use tasks::{Task, TaskId};

/// Default maximum number of concurrent jobs to run in the optimizer.
const DEFAULT_MAX_CONCURRENT_JOBS: usize = 1000;

/// External client requests that can be sent to the optimizer.
#[derive(Clone, Debug)]
pub enum ClientRequest {
    /// Request to optimize a logical plan into a physical plan.
    Optimize(OptimizeRequest),

    /// Request to dump the contents of the memo for debugging purposes.
    DumpMemo,
}

/// Request to optimize a query in the optimizer.
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
    pub physical_tx: Sender<PhysicalPlan>,
}

/// Products produced by optimization engine components
#[derive(Clone)]
enum EngineProduct {
    /// New logical plan alternative for a group from applying transformation rules.
    NewLogicalPartial(PartialLogicalPlan, GroupId),

    /// New physical implementation for a goal, awaiting recursive optimization.
    NewPhysicalPartial(PartialPhysicalPlan, GoalId),

    /// Create a new group with the provided logical properties.
    CreateGroup(LogicalExpressionId, LogicalProperties),

    /// Subscribe to logical expressions in a specific group.
    SubscribeGroup(GroupId, LogicalContinuation),
}

/// Messages passed within the optimization system.
///
/// Each message represents either a client request or the result of completed work,
/// allowing the optimizer to track which tasks are progressing.
enum OptimizerMessage {
    /// Client request to the optimizer.
    Request(OptimizeRequest, TaskId),

    /// Request to retrieve the properties of a group.
    Retrieve(GroupId, Sender<LogicalProperties>),

    /// Product from an optimization engine component.
    Product(EngineProduct, JobId),
}

impl OptimizerMessage {
    /// Create a new product message with the given engine product and job ID.
    pub(super) fn product(product: EngineProduct, job_id: JobId) -> Self {
        Self::Product(product, job_id)
    }
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

impl PendingMessage {
    /// Create a new pending message with the given message and dependencies.
    fn new(message: OptimizerMessage, dependencies: HashSet<JobId>) -> Self {
        Self {
            message,
            pending_dependencies: dependencies,
        }
    }
}

/// The central access point to the optimizer.
///
/// Provides the interface to submit logical plans for optimization and receive
/// optimized physical plans in return.
pub struct Optimizer<M: Memo> {
    // Core components.
    memo: M,
    catalog: Arc<dyn Catalog>,
    retriever: Arc<OptimizerRetriever>,
    rule_book: RuleBook,
    hir_context: Context,

    // Message handling.
    pending_messages: Vec<PendingMessage>,
    message_tx: Sender<OptimizerMessage>,
    message_rx: Receiver<OptimizerMessage>,
    client_rx: Receiver<ClientRequest>,

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
    _cost_expression_task_index: HashMap<PhysicalExpressionId, TaskId>,
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
        client_rx: Receiver<ClientRequest>,
    ) -> Self {
        Self {
            // Core components.
            memo,
            catalog,
            retriever: Arc::new(OptimizerRetriever::new(message_tx.clone())),
            rule_book: extract_rulebook_from_hir(&hir),
            hir_context: hir.context,

            // Message handling.
            pending_messages: Vec::new(),
            message_tx,
            message_rx,
            client_rx,

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
            _cost_expression_task_index: HashMap::new(),
        }
    }

    /// Launch a new optimizer and return a sender for client communication.
    pub fn launch(memo: M, catalog: Arc<dyn Catalog>, hir: HIR) -> Sender<ClientRequest> {
        let (message_tx, message_rx) = mpsc::channel(1);
        let (client_tx, client_rx) = mpsc::channel(1);

        // Start the background processing loop.
        let optimizer = Self::new(
            memo,
            hir,
            catalog,
            message_tx.clone(),
            message_rx,
            client_rx,
        );

        tokio::spawn(async move {
            // TODO: If an error occurs we could restart or reboot the memo.
            // Rather than failing (e.g. memo could be distributed).
            optimizer.run().await.expect("Optimizer failure");
        });

        client_tx
    }

    /// Run the optimizer's main processing loop.
    async fn run(mut self) -> Result<(), M::MemoError> {
        use ClientRequest::*;
        use EngineProduct::*;
        use OptimizerMessage::*;

        loop {
            tokio::select! {
                Some(client_request) = self.client_rx.recv() => {
                    let message_tx = self.message_tx.clone();

                    match client_request {
                        Optimize(optimize_request) => {
                            // Create a task for the optimization request.
                            let task_id = self.create_optimize_plan_task(
                                optimize_request.plan.clone(),
                                optimize_request.physical_tx.clone()
                            );

                            // Forward the client request to the message processing loop
                            // in a new coroutine to avoid a deadlock.
                            tokio::spawn(async move {
                                message_tx.send(Request(optimize_request, task_id))
                                    .await
                                    .expect("Failed to forward client request - channel closed");
                            });
                        },
                        DumpMemo => {
                            self.memo.debug_dump().await?;
                        }
                    }
                },
                Some(message) = self.message_rx.recv() => {
                    // Process the next message in the channel.
                    match message {
                        Request(OptimizeRequest { plan, physical_tx }, task_id) =>
                                self.process_optimize_request(plan, physical_tx, task_id).await?,
                        Retrieve(group_id, response_tx) => {
                            self.process_retrieve_properties(group_id, response_tx).await?;
                        },
                        Product(product, job_id) => {
                            let task_id = self.get_related_task_id(job_id);

                            // Only process the product if the task is still active.
                            if self.get_task(task_id).is_some() {
                                match product {
                                    NewLogicalPartial(plan, group_id) => {
                                        self.process_new_logical_partial(plan, group_id, job_id).await?;
                                    }
                                    NewPhysicalPartial(plan, goal_id) => {
                                        self.process_new_physical_partial(plan, goal_id, job_id).await?;
                                    }
                                    CreateGroup(expression_id, properties) => {
                                        self.process_create_group(expression_id, &properties, job_id).await?;
                                    }
                                    SubscribeGroup(group_id, continuation) => {
                                        self.process_group_subscription(group_id, continuation, job_id).await?;
                                    }
                                }
                            }

                            // A job is guaranteed to be terminated, unless it has been added to the pending queue.
                            if !self.pending_messages.iter().any(|msg| matches!(msg.message, Product(_, j) if j == job_id)) {
                                self.running_jobs.remove(&job_id);
                            }
                        }
                    };

                    // Launch pending jobs according to a policy (currently LIFO).
                    self.launch_pending_jobs().await?;
                },
                else => break Ok(()),
            }
        }
    }
}
