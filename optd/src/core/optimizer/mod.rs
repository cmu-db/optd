use crate::catalog::Catalog;
use crate::core::cir::{
    Cost, Goal, GoalId, GroupId, LogicalProperties, PartialLogicalPlan, PartialPhysicalPlan,
    PhysicalExpressionId, RuleBook,
};
use crate::core::error::Error;
use crate::core::memo::Memoize;
use crate::dsl::analyzer::hir::context::Context;
use EngineMessageKind::*;
pub use client::{Client, QueryInstance};
use client::{ClientMessage, QueryInstanceId};
use futures::StreamExt;
use futures::channel::mpsc;

use crate::dsl::analyzer::hir::HIR;
use crate::dsl::analyzer::hir::Value;
use crate::dsl::engine::{Continuation, EngineResponse};
use jobs::{Job, JobId};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tasks::{Task, TaskId};

mod client;
mod egest;
mod forward;
mod handlers;
mod ingest;
mod jobs;
mod merge;
mod tasks;

/// Default maximum number of concurrent jobs to run in the optimizer.
const DEFAULT_MAX_CONCURRENT_JOBS: usize = 1000;

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
#[derive(Clone)]
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

    /// Retrieve logical properties for a specific group.
    #[allow(unused)]
    RetrieveProperties(GroupId, mpsc::Sender<LogicalProperties>),

    /// Associate logical properties with a group.
    /// Note: logical property are on-demand computed.
    NewProperties(GroupId, LogicalProperties),
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
    catalog: Arc<dyn Catalog>,
    // Message handling.
    /// Sender for sending messages from the engine.
    engine_tx: mpsc::Sender<EngineMessage>,
    /// Receiver for receiving messages from the engine.
    engine_rx: mpsc::Receiver<EngineMessage>,
    /// Receiver for client messages.
    client_rx: mpsc::Receiver<ClientMessage>,

    // Query instance management.
    /// The next query instance id to be assigned.
    next_query_instance_id: QueryInstanceId,
    /// Query instance id to the `OptimizePlan` task id.
    query_instances: HashMap<QueryInstanceId, TaskId>,

    // Job management.
    /// Queue of jobs that are ready to be run.
    runnable_jobs: HashMap<JobId, Job>,
    /// Queue of job ids that are runnable.
    runnable_queue: VecDeque<JobId>,
    /// Map of currently running jobs. Some job is associated with a task, some are not.
    running_jobs: HashMap<JobId, Job>,

    /// Maps pending derives to a sinle job id and a list of senders that retrieve logical properties.
    pending_derives: HashMap<GroupId, (JobId, Vec<mpsc::Sender<LogicalProperties>>)>,

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
    fn new(
        memo: M,
        catalog: Arc<dyn Catalog>,
        hir: HIR,
        client_rx: mpsc::Receiver<ClientMessage>,
    ) -> Self {
        let (engine_tx, engine_rx) = mpsc::channel(0);

        Optimizer {
            // Core components.
            memo,
            rule_book: RuleBook::default(),
            hir_context: hir.context,
            catalog,
            // Message handling.
            engine_tx,
            engine_rx,
            client_rx,

            query_instances: HashMap::new(),
            next_query_instance_id: QueryInstanceId(0),

            // Job management.
            runnable_jobs: HashMap::new(),
            runnable_queue: VecDeque::new(),
            running_jobs: HashMap::new(),
            pending_derives: HashMap::new(),
            next_job_id: JobId(0),
            max_concurrent_jobs: DEFAULT_MAX_CONCURRENT_JOBS,

            // Task management.
            tasks: HashMap::new(),
            next_task_id: TaskId(0),

            // Task indexing.
            group_exploration_task_index: HashMap::new(),
            goal_optimization_task_index: HashMap::new(),
            cost_expression_task_index: HashMap::new(),
        }
    }

    /// Launch a new optimizer and return a sender for client communication.
    pub fn launch(memo: M, catalog: Arc<dyn Catalog>, hir: HIR) -> Client<M> {
        let (client_tx, client_rx) = mpsc::channel(0);

        let handle = tokio::spawn(async move {
            // Start the background processing loop.
            let optimizer = Self::new(memo, catalog, hir, client_rx);
            // TODO(Alexis): If an error occurs we could restart or reboot the memo.
            // Rather than failing (e.g. memo could be distributed).
            optimizer.run().await.expect("Optimizer failure")
        });

        Client::new(client_tx, handle)
    }

    /// Run the optimizer's main processing loop.
    async fn run(mut self) -> Result<M, Error> {
        loop {
            tokio::select! {
                Some(request) = self.client_rx.next() => {
                    let is_shutdown = self.process_client_request(request).await?;
                    if is_shutdown {
                        break Ok(self.memo);
                    }
                },
                Some(message) = self.engine_rx.next() => {
                    let EngineMessage {
                        job_id,
                        kind,
                    } = message;
                    let _job = self.running_jobs.remove(&job_id).unwrap();
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
                            self.process_new_properties(group_id, properties).await?;
                            self.complete_job(job_id).await?;
                        }
                    };

                    // Launch pending jobs according to a policy (currently FIFO).
                    self.launch_runnable_jobs().await?;
                },
                else => break Ok(self.memo),
            }
        }
    }

    // fn next_task_id(task_id: &mut TaskId) -> TaskId {
    //     let ret = *task_id;
    //     task_id.0 += 1;
    //     ret
    // }

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
        self.running_jobs.get(&job_id).and_then(|job| match job {
            Job::Task(task_id) => Some(*task_id),
            Job::Derive(_) => None,
        })
    }
}

#[cfg(test)]
mod tests {

    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use tokio::task::JoinSet;

    use super::*;
    use crate::{
        catalog::CatalogError,
        core::{
            cir::{
                Child, GoalMemberId, LogicalExpression, LogicalPlan, Operator, OperatorData,
                PhysicalExpression, PhysicalPlan, PhysicalProperties, PropertiesData,
            },
            memo::memory::MemoryMemo,
        },
    };
    #[derive(Debug)]
    struct MockCatalog;

    #[async_trait]
    impl Catalog for MockCatalog {
        async fn get_table_properties(
            &self,
            _table_name: &str,
        ) -> Result<HashMap<String, String>, CatalogError> {
            Ok(HashMap::new())
        }

        async fn get_table_columns(&self, _table_name: &str) -> Result<Vec<String>, CatalogError> {
            Ok(vec![])
        }
    }

    fn mock_catalog() -> Arc<dyn Catalog> {
        Arc::new(MockCatalog)
    }

    #[tokio::test]
    async fn test_optimizer_with_empty_hir() -> Result<(), Error> {
        let hir = HIR {
            context: Context::default(),
            annotations: HashMap::new(),
        };

        let mut memo = MemoryMemo::default();

        let mut client = Optimizer::launch(memo, mock_catalog(), hir);

        let logical_plan = LogicalPlan(Operator {
            tag: "Scan".to_string(),
            data: vec![OperatorData::String("t1".into())],
            children: vec![],
        });
        let mut query_instance = client.create_query_instance(logical_plan).await?;

        // Wait for three secs, should just directly timeout.
        tokio::time::timeout(Duration::from_secs(1), async {
            let _physical_plan = query_instance.recv_best_plan().await.unwrap();
            panic!("Should not receive a plan");
        })
        .await
        .unwrap_err();

        client.shutdown().await.unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_optimizer_with_empty_hir_memoized() -> Result<(), Error> {
        let mut memo = MemoryMemo::default();

        let (no_sort_goal_id, sort_goal_id, scan_group_id) = {
            let scan = LogicalExpression {
                tag: "Scan".to_string(),
                data: vec![OperatorData::String("t1".into())],
                children: vec![],
            };
            let logical_expr_id = memo.get_logical_expr_id(&scan).await?;
            let scan_group_id = memo.create_group(logical_expr_id).await?;

            let no_sort_goal_id = memo
                .get_goal_id(&Goal(scan_group_id, PhysicalProperties(None)))
                .await?;
            let properties =
                PhysicalProperties(Some(PropertiesData::String("order by t1.v1".into())));
            let sort_goal_id = memo.get_goal_id(&Goal(scan_group_id, properties)).await?;
            (no_sort_goal_id, sort_goal_id, scan_group_id)
        };

        let goal_id = {
            let sort = LogicalExpression {
                tag: "Sort".to_string(),
                data: vec![OperatorData::String("t1".into())],
                children: vec![Child::Singleton(scan_group_id)],
            };
            let logical_expr_id = memo.get_logical_expr_id(&sort).await?;
            let sort_group_id = memo.create_group(logical_expr_id).await?;
            let goal_id = memo
                .get_goal_id(&Goal(sort_group_id, PhysicalProperties(None)))
                .await?;

            // manually connect the goal to sort_goal_id.
            let result = memo
                .add_goal_member(goal_id, GoalMemberId::GoalId(sort_goal_id))
                .await?;
            assert!(result.is_none());
            goal_id
        };

        {
            let table_scan = PhysicalExpression {
                tag: "TableScan".to_string(),
                data: vec![OperatorData::String("t1".into())],
                children: vec![],
            };

            let physical_expr_id = memo.get_physical_expr_id(&table_scan).await?;
            let forward_result = memo
                .add_goal_member(
                    no_sort_goal_id,
                    GoalMemberId::PhysicalExpressionId(physical_expr_id),
                )
                .await?;

            assert!(forward_result.is_none());

            let result = memo
                .update_physical_expr_cost(physical_expr_id, Cost(40.0))
                .await?
                .unwrap();

            assert_eq!(result.best_cost, Cost(40.0));
            assert_eq!(result.physical_expr_id, physical_expr_id);
            assert_eq!(result.goals_forwarded.len(), 1);
            assert!(result.goals_forwarded.contains(&no_sort_goal_id));
        }

        let index_scan_expr_id = {
            let index_scan = PhysicalExpression {
                tag: "IndexScan".to_string(),
                data: vec![
                    OperatorData::String("t1".into()),
                    OperatorData::String("t1v1".into()),
                ],
                children: vec![],
            };

            let physical_expr_id = memo.get_physical_expr_id(&index_scan).await?;
            memo.add_goal_member(
                no_sort_goal_id,
                GoalMemberId::PhysicalExpressionId(physical_expr_id),
            )
            .await?;

            memo.update_physical_expr_cost(physical_expr_id, Cost(50.0))
                .await?;
            physical_expr_id
        };

        {
            let physical_sort = PhysicalExpression {
                tag: "EnforceSort".to_string(),
                data: vec![OperatorData::String("order_by t1.v1".into())],
                children: vec![Child::Singleton(GoalMemberId::GoalId(no_sort_goal_id))],
            };

            let physical_expr_id = memo.get_physical_expr_id(&physical_sort).await?;
            memo.add_goal_member(
                goal_id,
                GoalMemberId::PhysicalExpressionId(physical_expr_id),
            )
            .await?;

            memo.update_physical_expr_cost(physical_expr_id, Cost(60.0))
                .await?;
        }

        let hir = HIR {
            context: Context::default(),
            annotations: HashMap::new(),
        };
        let mut client = Optimizer::launch(memo, mock_catalog(), hir);

        let logical_scan_plan = LogicalPlan(Operator {
            tag: "Scan".to_string(),
            data: vec![OperatorData::String("t1".into())],
            children: vec![],
        });

        let physical_scan = Arc::new(PhysicalPlan(Operator {
            tag: "TableScan".to_string(),
            data: vec![OperatorData::String("t1".into())],
            children: vec![],
        }));

        let mut join_set = JoinSet::new();

        {
            let logical_plan = logical_scan_plan.clone();
            let mut query_instance = client.create_query_instance(logical_plan).await.unwrap();
            let expected = physical_scan.clone();
            join_set.spawn(async move {
                let physical_plan = query_instance.recv_best_plan().await.unwrap();
                println!("Best plan: {:?}", physical_plan);

                assert_eq!(&physical_plan, expected.as_ref());
            })
        };

        let enforce_sort = Arc::new(PhysicalPlan(Operator {
            tag: "EnforceSort".to_string(),
            data: vec![OperatorData::String("order_by t1.v1".into())],
            children: vec![Child::Singleton(physical_scan)],
        }));

        let logical_sort_plan = LogicalPlan(Operator {
            tag: "Sort".to_string(),
            data: vec![OperatorData::String("t1".into())],
            children: vec![Child::Singleton(Arc::new(logical_scan_plan))],
        });

        {
            let logical_plan = logical_sort_plan.clone();
            let mut query_instance = client.create_query_instance(logical_plan).await.unwrap();
            let expected = enforce_sort.clone();
            join_set.spawn(async move {
                let physical_plan = query_instance.recv_best_plan().await.unwrap();

                assert_eq!(&physical_plan, expected.as_ref());
            })
        };

        let _ = join_set.join_all().await;

        let mut memo = client.shutdown().await.unwrap();

        // Add index scan to the memo. We expect the index scan to be the best plan for the logical_sort query.
        {
            memo.add_goal_member(
                sort_goal_id,
                GoalMemberId::PhysicalExpressionId(index_scan_expr_id),
            )
            .await?;
        }

        let index_scan = Arc::new(PhysicalPlan(Operator {
            tag: "IndexScan".to_string(),
            data: vec![
                OperatorData::String("t1".into()),
                OperatorData::String("t1v1".into()),
            ],
            children: vec![],
        }));

        let hir = HIR {
            context: Context::default(),
            annotations: HashMap::new(),
        };
        let mut client = Optimizer::launch(memo, mock_catalog(), hir);

        {
            let logical_plan = logical_sort_plan.clone();
            let mut query_instance = client.create_query_instance(logical_plan).await.unwrap();
            let expected = index_scan;
            tokio::spawn(async move {
                let physical_plan = query_instance.recv_best_plan().await.unwrap();

                assert_eq!(&physical_plan, expected.as_ref());
            })
        };

        client.shutdown().await.unwrap();
        Ok(())
    }
}
