#![allow(clippy::new_without_default)]

mod from_optd;
mod into_optd;
mod physical_collector;

use arrow_schema::DataType;
use async_trait::async_trait;
use datafusion::{
    catalog::CatalogList,
    error::Result,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::{LogicalPlan, TableSource},
    physical_plan::{displayable, ExecutionPlan},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use optd_datafusion_repr::{
    plan_nodes::{
        ConstantType, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PhysicalHashJoin,
        PhysicalNestedLoopJoin, PlanNode,
    },
    properties::schema::Catalog,
    DatafusionOptimizer,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

struct OptdPlanContext<'a> {
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
        let fields = table.schema();
        let mut optd_schema = vec![];
        for field in fields.fields() {
            let dt = match field.data_type() {
                DataType::Date32 => ConstantType::Date,
                DataType::Int32 => ConstantType::Int,
                DataType::Int64 => ConstantType::Int,
                DataType::Float64 => ConstantType::Decimal,
                DataType::Utf8 => ConstantType::Utf8String,
                DataType::Decimal128(_, _) => ConstantType::Decimal,
                dt => unimplemented!("{:?}", dt),
            };
            optd_schema.push(dt);
        }
        optd_datafusion_repr::properties::schema::Schema(optd_schema)
    }
}

pub struct OptdQueryPlanner {
    optimizer: Arc<Mutex<Option<Box<DatafusionOptimizer>>>>,
}

#[derive(Debug, Eq, PartialEq, Hash)]
enum JoinOrder {
    Table(String),
    HashJoin(Box<Self>, Box<Self>),
    NestedLoopJoin(Box<Self>, Box<Self>),
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
            // Some(JoinOrder::NestedLoopJoin(Box::new(left), Box::new(right)))
            None
        }
        OptRelNodeTyp::PhysicalScan => {
            let scan =
                optd_datafusion_repr::plan_nodes::PhysicalScan::from_rel_node(rel_node).unwrap();
            Some(JoinOrder::Table(scan.table().to_string()))
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
    async fn create_physical_plan_inner(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
        println!(
            "optd-datafusion-bridge: [datafusion_logical_plan] {:?}",
            logical_plan
        );
        let mut ctx = OptdPlanContext::new(session_state);
        let optd_rel = ctx.into_optd(logical_plan)?;
        let mut optimizer = self.optimizer.lock().unwrap().take().unwrap();
        println!("optd-datafusion-bridge: [optd_logical_plan] {}", optd_rel);
        let optimized_rel = optimizer.optimize(optd_rel);
        optimizer.dump();
        let (group_id, optimized_rel) = optimized_rel?;
        let bindings = optimizer
            .optd_optimizer()
            .get_all_group_physical_bindings(group_id);
        let mut join_orders = HashSet::new();
        for binding in bindings {
            join_orders.insert(get_join_order(binding));
        }
        for order in join_orders {
            if order.is_some() {
                println!("optd-datafusion-bridge: [join_order] {}", order.unwrap());
            }
        }
        println!(
            "optd-datafusion-bridge: [optd_optimized_plan]\n{}",
            PlanNode::from_rel_node(optimized_rel.clone())
                .unwrap()
                .explain_to_string()
        );
        ctx.optimizer = Some(&optimizer);
        let physical_plan = ctx.from_optd(optimized_rel).await?;
        let d = displayable(&*physical_plan).to_stringified(
            false,
            datafusion::logical_expr::PlanType::OptimizedPhysicalPlan {
                optimizer_name: "optd-datafusion".to_string(),
            },
        );
        println!("optd-datafusion-bridge: [physical_plan] {}", d.plan);
        self.optimizer.lock().unwrap().replace(optimizer);
        Ok(physical_plan)
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
        match self
            .create_physical_plan_inner(logical_plan, session_state)
            .await
        {
            Ok(x) => Ok(x),
            Err(e) => {
                println!("[ERROR] optd-datafusion-bridge: {:#}", e);
                let planner = DefaultPhysicalPlanner::default();
                planner
                    .create_physical_plan(logical_plan, session_state)
                    .await
            }
        }
    }
}
