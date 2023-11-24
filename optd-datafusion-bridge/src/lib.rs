#![allow(clippy::new_without_default)]

mod from_optd;
mod into_optd;

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
    plan_nodes::{ConstantType, OptRelNode, PlanNode},
    properties::schema::Catalog,
    DatafusionOptimizer,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

struct OptdPlanContext<'a> {
    tables: HashMap<String, Arc<dyn TableSource>>,
    session_state: &'a SessionState,
}

impl<'a> OptdPlanContext<'a> {
    pub fn new(session_state: &'a SessionState) -> Self {
        Self {
            tables: HashMap::new(),
            session_state,
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
        println!("optd-datafusion-bridge: [optd_logical_plan] {}", optd_rel);
        let mut optimizer = self.optimizer.lock().unwrap().take().unwrap();
        let optimized_rel = optimizer.optimize(optd_rel);
        optimizer.dump();
        let optimized_rel = optimized_rel?;
        println!(
            "optd-datafusion-bridge: [optd_optimized_plan]\n{}",
            PlanNode::from_rel_node(optimized_rel.clone())
                .unwrap()
                .explain_to_string()
        );
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
