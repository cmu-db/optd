#![allow(clippy::new_without_default)]

mod from_optd;
mod into_optd;

use async_trait::async_trait;
use datafusion::{
    error::Result,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::{LogicalPlan, TableSource},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use optd_datafusion_repr::DatafusionOptimizer;
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
            "optd-datafusion-bridge: [optd_optimized_plan] {}",
            optimized_rel
        );
        let physical_plan = ctx.from_optd(optimized_rel).await?;
        println!(
            "optd-datafusion-bridge: [physical_plan] {:?}",
            physical_plan
        );
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
