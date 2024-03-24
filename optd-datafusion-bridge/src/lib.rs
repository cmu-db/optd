#![allow(clippy::new_without_default)]

mod from_optd;
mod into_optd;
mod physical_collector;

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::DataType,
    catalog::CatalogList,
    error::Result,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::{
        Explain, LogicalPlan, PlanType, StringifiedPlan, TableSource, ToStringifiedPlan,
    },
    physical_plan::{displayable, explain::ExplainExec, ExecutionPlan},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use itertools::Itertools;
use optd_datafusion_repr::{
    plan_nodes::{
        ConstantType, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PhysicalHashJoin,
        PhysicalNestedLoopJoin, PlanNode,
    },
    properties::schema::Catalog,
    DatafusionOptimizer,
};
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

pub struct OptdPlanContext<'a> {
    tables: HashMap<String, Arc<dyn TableSource>>,
    session_state: &'a SessionState,
    pub optimizer: Option<&'a DatafusionOptimizer>,
}

impl<'a> OptdPlanContext<'a> {
    pub fn new(session_state: &'a SessionState) -> Self {
        Self {
            tables: HashMap::new(),
            session_state,
            optimizer: None,
        }
    }
}

pub struct DatafusionCatalog {
    catalog: Arc<dyn CatalogList>,
}

impl DatafusionCatalog {
    pub fn new(catalog: Arc<dyn CatalogList>) -> Self {
        Self { catalog }
    }
}

impl Catalog for DatafusionCatalog {
    fn get(&self, name: &str) -> optd_datafusion_repr::properties::schema::Schema {
        let catalog = self.catalog.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table = futures_lite::future::block_on(schema.table(name.as_ref())).unwrap();
        let schema = table.schema();
        let fields = schema.fields();
        let mut optd_fields = Vec::with_capacity(fields.len());
        for field in fields {
            let dt = match field.data_type() {
                DataType::Date32 => ConstantType::Date,
                DataType::Int32 => ConstantType::Int32,
                DataType::Int64 => ConstantType::Int64,
                DataType::Float64 => ConstantType::Decimal,
                DataType::Utf8 => ConstantType::Utf8String,
                DataType::Decimal128(_, _) => ConstantType::Decimal,
                dt => unimplemented!("{:?}", dt),
            };
            optd_fields.push(optd_datafusion_repr::properties::schema::Field {
                name: field.name().to_string(),
                typ: dt,
                nullable: field.is_nullable(),
            });
        }
        optd_datafusion_repr::properties::schema::Schema {
            fields: optd_fields,
        }
    }
}

pub struct OptdQueryPlanner {
    pub optimizer: Arc<Mutex<Option<Box<DatafusionOptimizer>>>>,
}

#[derive(Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
enum JoinOrder {
    Table(String),
    HashJoin(Box<Self>, Box<Self>),
    NestedLoopJoin(Box<Self>, Box<Self>),
}

impl JoinOrder {
    pub fn conv_into_logical_join_order(&self) -> LogicalJoinOrder {
        match self {
            JoinOrder::Table(name) => LogicalJoinOrder::Table(name.clone()),
            JoinOrder::HashJoin(left, right) => LogicalJoinOrder::Join(
                Box::new(left.conv_into_logical_join_order()),
                Box::new(right.conv_into_logical_join_order()),
            ),
            JoinOrder::NestedLoopJoin(left, right) => LogicalJoinOrder::Join(
                Box::new(left.conv_into_logical_join_order()),
                Box::new(right.conv_into_logical_join_order()),
            ),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
enum LogicalJoinOrder {
    Table(String),
    Join(Box<Self>, Box<Self>),
}

fn get_join_order(rel_node: OptRelNodeRef) -> Option<JoinOrder> {
    match rel_node.typ {
        OptRelNodeTyp::PhysicalHashJoin(_) => {
            let join = PhysicalHashJoin::from_rel_node(rel_node.clone()).unwrap();
            let left = get_join_order(join.left().into_rel_node())?;
            let right = get_join_order(join.right().into_rel_node())?;
            Some(JoinOrder::HashJoin(Box::new(left), Box::new(right)))
        }
        OptRelNodeTyp::PhysicalNestedLoopJoin(_) => {
            let join = PhysicalNestedLoopJoin::from_rel_node(rel_node.clone()).unwrap();
            let left = get_join_order(join.left().into_rel_node())?;
            let right = get_join_order(join.right().into_rel_node())?;
            Some(JoinOrder::NestedLoopJoin(Box::new(left), Box::new(right)))
        }
        OptRelNodeTyp::PhysicalScan => {
            let scan =
                optd_datafusion_repr::plan_nodes::PhysicalScan::from_rel_node(rel_node).unwrap();
            Some(JoinOrder::Table(
                scan.into_rel_node().data.as_ref().unwrap().to_string(),
            ))
        }
        _ => {
            for child in &rel_node.children {
                if let Some(res) = get_join_order(child.clone()) {
                    return Some(res);
                }
            }
            None
        }
    }
}

impl std::fmt::Display for LogicalJoinOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalJoinOrder::Table(name) => write!(f, "{}", name),
            LogicalJoinOrder::Join(left, right) => {
                write!(f, "(Join {} {})", left, right)
            }
        }
    }
}

impl std::fmt::Display for JoinOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinOrder::Table(name) => write!(f, "{}", name),
            JoinOrder::HashJoin(left, right) => {
                write!(f, "(HashJoin {} {})", left, right)
            }
            JoinOrder::NestedLoopJoin(left, right) => {
                write!(f, "(NLJ {} {})", left, right)
            }
        }
    }
}

impl OptdQueryPlanner {
    pub fn enable_adaptive(&self) {
        self.optimizer
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .enable_adaptive(true);
    }

    pub fn disable_adaptive(&self) {
        self.optimizer
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .enable_adaptive(false);
    }

    async fn create_physical_plan_inner(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
        if let LogicalPlan::Dml(_) | LogicalPlan::Ddl(_) | LogicalPlan::EmptyRelation(_) =
            logical_plan
        {
            let planner = DefaultPhysicalPlanner::default();
            return Ok(planner
                .create_physical_plan(logical_plan, session_state)
                .await?);
        }
        let (mut explains, verbose, logical_plan) = match logical_plan {
            LogicalPlan::Explain(Explain { plan, verbose, .. }) => {
                (Some(Vec::new()), *verbose, plan.as_ref())
            }
            _ => (None, false, logical_plan),
        };
        let mut ctx = OptdPlanContext::new(session_state);
        if let Some(explains) = &mut explains {
            explains.push(logical_plan.to_stringified(PlanType::OptimizedLogicalPlan {
                optimizer_name: "datafusion".to_string(),
            }));
        }
        let optd_rel = ctx.conv_into_optd(logical_plan)?;
        if let Some(explains) = &mut explains {
            explains.push(StringifiedPlan::new(
                PlanType::OptimizedLogicalPlan {
                    optimizer_name: "optd".to_string(),
                },
                PlanNode::from_rel_node(optd_rel.clone())
                    .unwrap()
                    .explain_to_string(None),
            ));
        }
        let mut optimizer = self.optimizer.lock().unwrap().take().unwrap();
        let (group_id, optimized_rel, meta) = optimizer.optimize(optd_rel)?;

        if let Some(explains) = &mut explains {
            explains.push(StringifiedPlan::new(
                PlanType::OptimizedPhysicalPlan {
                    optimizer_name: "optd".to_string(),
                },
                PlanNode::from_rel_node(optimized_rel.clone())
                    .unwrap()
                    .explain_to_string(if verbose { Some(&meta) } else { None }),
            ));
            let join_order = get_join_order(optimized_rel.clone());
            explains.push(StringifiedPlan::new(
                PlanType::OptimizedPhysicalPlan {
                    optimizer_name: "optd-join-order".to_string(),
                },
                if let Some(join_order) = join_order {
                    join_order.to_string()
                } else {
                    "None".to_string()
                },
            ));
            let bindings = optimizer
                .optd_optimizer()
                .get_all_group_bindings(group_id, true);
            let mut join_orders = BTreeSet::new();
            let mut logical_join_orders = BTreeSet::new();
            for binding in bindings {
                if let Some(join_order) = get_join_order(binding) {
                    logical_join_orders.insert(join_order.conv_into_logical_join_order());
                    join_orders.insert(join_order);
                }
            }
            explains.push(StringifiedPlan::new(
                PlanType::OptimizedPhysicalPlan {
                    optimizer_name: "optd-all-join-orders".to_string(),
                },
                join_orders.iter().map(|x| x.to_string()).join("\n"),
            ));
            explains.push(StringifiedPlan::new(
                PlanType::OptimizedPhysicalPlan {
                    optimizer_name: "optd-all-logical-join-orders".to_string(),
                },
                logical_join_orders.iter().map(|x| x.to_string()).join("\n"),
            ));
        }
        // println!(
        //     "{} cost={}",
        //     get_join_order(optimized_rel.clone()).unwrap(),
        //     optimizer.optd_optimizer().get_cost_of(group_id)
        // );
        // optimizer.dump(Some(group_id));
        ctx.optimizer = Some(&optimizer);
        let physical_plan = ctx.conv_from_optd(optimized_rel, meta).await?;
        if let Some(explains) = &mut explains {
            explains.push(
                displayable(&*physical_plan)
                    .to_stringified(false, datafusion::logical_expr::PlanType::FinalPhysicalPlan),
            );
        }
        self.optimizer.lock().unwrap().replace(optimizer);
        if let Some(explains) = explains {
            Ok(Arc::new(ExplainExec::new(
                LogicalPlan::explain_schema(),
                explains,
                true,
            )))
        } else {
            Ok(physical_plan)
        }
    }

    pub fn new(optimizer: DatafusionOptimizer) -> Self {
        Self {
            optimizer: Arc::new(Mutex::new(Some(Box::new(optimizer)))),
        }
    }
}

#[async_trait]
impl QueryPlanner for OptdQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self
            .create_physical_plan_inner(logical_plan, session_state)
            .await
            .unwrap())
    }
}
