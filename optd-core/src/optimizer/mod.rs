use crate::cir::{
    Cost, Goal, GoalId, GroupId, LogicalPlan, LogicalProperties, PartialLogicalPlan,
    PartialPhysicalPlan, PhysicalExpressionId, PhysicalPlan, RuleBook,
};
use crate::error::Error;
use crate::memo::Memoize;
use EngineMessageKind::*;
pub use client::{Client, QueryInstance};
use client::{ClientMessage, QueryInstanceId};
use futures::StreamExt;
use futures::channel::mpsc;
use futures::channel::oneshot;
use jobs::{Job, JobId};
use optd_dsl::analyzer::hir::Value;
use optd_dsl::analyzer::{context::Context, hir::HIR};
use optd_dsl::engine::{Continuation, EngineResponse};
use std::collections::{HashMap, HashSet, VecDeque};
use tasks::{Task, TaskId};

mod client;
mod egest;
mod handlers;
mod ingest;
mod jobs;
mod merge;
mod tasks;

/// Default maximum number of concurrent jobs to run in the optimizer.
const DEFAULT_MAX_CONCURRENT_JOBS: usize = 1000;

/// External client request to optimize a query in the optimizer.
///
/// Defines the public API for submitting a query and receiving execution plans.
#[derive(Debug)]
pub struct OptimizeRequest {
    /// The logical plan to optimize.
    pub plan: LogicalPlan,

    /// Channel for receiving optimized physical plans.
    ///
    /// Streams results back as they become available, allowing clients to:
    /// * Receive progressively better plans during optimization.
    /// * Terminate early when a "good enough" plan is found.
    pub response_tx: oneshot::Sender<PhysicalPlan>,
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
pub enum EngineMessageKind {
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
    message_tx: mpsc::Sender<EngineMessage>,
    message_rx: mpsc::Receiver<EngineMessage>,
    client_rx: mpsc::Receiver<ClientMessage>,

    // Query instance management.
    /// The next query instance id to be assigned.
    next_query_instance_id: QueryInstanceId,
    /// Query instance id to the `OptimizePlan` task id.
    query_instances: HashMap<QueryInstanceId, TaskId>,

    // Job management.
    /// Queue of jobs that are ready to be run.
    runnable_jobs: VecDeque<(JobId, Job)>,
    /// Map of currently running jobs. Some job is associated with a task, some are not.
    running_jobs: HashMap<JobId, Option<TaskId>>,
    next_job_id: JobId,
    max_concurrent_jobs: usize,

    // Task management.
    tasks: HashMap<TaskId, Task>,
    next_task_id: TaskId,

    // Task indexing.
    group_exploration_task_index: HashMap<GroupId, TaskId>,
    goal_optimization_task_index: HashMap<GoalId, TaskId>,
    cost_expression_task_index: HashMap<PhysicalExpressionId, TaskId>,
}

impl<M: Memoize> Optimizer<M> {
    /// Create a new optimizer instance with the given memo and HIR context.
    ///
    /// Use `launch` to create and start the optimizer.
    fn new(memo: M, hir: HIR, client_rx: mpsc::Receiver<ClientMessage>) -> Self {
        let (message_tx, message_rx) = mpsc::channel(0);

        Self {
            // Core components.
            memo,
            rule_book: RuleBook::default(),
            hir_context: hir.context,

            // Message handling.
            pending_messages: Vec::new(),
            message_tx,
            message_rx,
            client_rx,

            query_instances: HashMap::new(),
            next_query_instance_id: QueryInstanceId(0),

            // Job management.
            runnable_jobs: VecDeque::new(),
            running_jobs: HashMap::new(),
            next_job_id: JobId(0),
            max_concurrent_jobs: DEFAULT_MAX_CONCURRENT_JOBS,

            // Task management.
            tasks: HashMap::new(),
            next_task_id: TaskId(0),

            // Task indexing.
            group_exploration_task_index: HashMap::new(),
            goal_optimization_task_index: HashMap::new(),
            cost_expression_task_index: HashMap::new(),
            //
        }
    }

    /// Launch a new optimizer and return a sender for client communication.
    pub fn launch(memo: M, hir: HIR) -> Client {
        let (client_tx, client_rx) = mpsc::channel(0);

        tokio::spawn(async move {
            // Start the background processing loop.
            let optimizer = Self::new(memo, hir, client_rx);
            // TODO(Alexis): If an error occurs we could restart or reboot the memo.
            // Rather than failing (e.g. memo could be distributed).
            optimizer.run().await.expect("Optimizer failure");
        });

        let client = Client::new(client_tx);
        client
    }

    /// Run the optimizer's main processing loop.
    async fn run(mut self) -> Result<(), Error> {
        loop {
            tokio::select! {
                Some(request) = self.client_rx.next() => {
                    self.process_client_request(request).await?;
                },
                Some(message) = self.message_rx.next() => {
                    let EngineMessage {
                        job_id,
                        kind,
                    } = message;
                    // Process the next message in the channel.
                    match kind {
                        NewLogicalPartial(plan, group_id) => {
                            if self.get_related_task(message.job_id).is_some() {
                                self.process_new_logical_partial(plan, group_id).await?;
                                self.complete_job(job_id).await?;
                            }
                        }
                        NewPhysicalPartial(plan, goal_id) => {
                            if let Some(task_id) = self.get_related_task(job_id) {
                                self.process_new_physical_partial(plan, goal_id, task_id).await?;
                                self.complete_job(job_id).await?;
                            }
                        }
                        NewCostedPhysical(expression_id, cost) => {
                            if self.get_related_task(job_id).is_some() {
                                self.process_new_costed_physical(expression_id, cost).await?;
                                self.complete_job(job_id).await?;
                            }
                        }
                        SubscribeGroup(group_id, continuation) => {
                            if let Some(task_id) = self.get_related_task(job_id) {
                                self.process_group_subscription(group_id, continuation, task_id).await?;
                                self.complete_job(job_id).await?;
                            }
                        }
                        SubscribeGoal(goal, continuation) => {
                            if let Some(task_id) = self.get_related_task(job_id) {
                                self.process_goal_subscription(&goal, continuation, task_id).await?;
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
                    self.launch_runnable_jobs().await?;
                },
                else => break Ok(()),
            }
        }
    }

    fn next_task_id(&mut self) -> TaskId {
        let task_id = self.next_task_id;
        self.next_task_id.0 += 1;
        task_id
    }

    fn next_query_instance_id(&mut self) -> QueryInstanceId {
        let query_instance_id = self.next_query_instance_id;
        self.next_query_instance_id.0 += 1;
        query_instance_id
    }

    fn get_related_task(&self, job_id: JobId) -> Option<TaskId> {
        self.running_jobs
            .get(&job_id)
            .and_then(|task_id| task_id.clone())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        cir::{Operator, OperatorData},
        memo::mock::MockMemo,
    };

    #[tokio::test]
    async fn test_optimizer_simple() -> Result<(), Error> {
        let hir = HIR {
            context: Context::default(),
            annotations: HashMap::new(),
        };

        let mut client = Optimizer::launch(MockMemo, hir);
        let logical_plan = LogicalPlan(Operator::new(
            "Scan".to_string(),
            vec![OperatorData::String("t1".into())],
            vec![],
        ));
        let mut query_instance = client.create_query_instance(logical_plan).await?;

        let physical_plan = query_instance.recv_best_plan().await.unwrap();
        println!("Best plan: {:?}", physical_plan);

        let expected_physical_plan = PhysicalPlan(Operator::new(
            "TableScan".to_string(),
            vec![OperatorData::String("t1".into())],
            vec![],
        ));

        assert_eq!(physical_plan, expected_physical_plan);

        Ok(())
    }
}
