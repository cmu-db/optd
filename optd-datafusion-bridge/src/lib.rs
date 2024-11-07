// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

#![allow(clippy::new_without_default)]

mod from_optd;
mod into_optd;
mod physical_collector;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::CatalogList;
use datafusion::error::Result;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{
    Explain, LogicalPlan, PlanType, StringifiedPlan, TableSource, ToStringifiedPlan,
};
use datafusion::physical_plan::explain::ExplainExec;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use itertools::Itertools;
use optd_datafusion_repr::plan_nodes::{
    dispatch_plan_explain_to_string, ArcDfPlanNode, ConstantType, DfNodeType, DfReprPlanNode,
    PhysicalHashJoin, PhysicalNestedLoopJoin,
};
use optd_datafusion_repr::properties::schema::Catalog;
use optd_datafusion_repr::{DatafusionOptimizer, MemoExt};

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
        let mut optd_rel = ctx.conv_into_optd(logical_plan)?;

        if let Some(explains) = &mut explains {
            explains.push(StringifiedPlan::new(
                PlanType::OptimizedLogicalPlan {
                    optimizer_name: "optd".to_string(),
                },
                dispatch_plan_explain_to_string(optd_rel.clone(), None),
            ));
        }

        tracing::trace!(
            optd_unoptimized_plan = %("\n".to_string()
            + &ArcDfPlanNode::from_plan_node(optd_rel.clone())
                .unwrap()
                .explain_to_string(None)));

        let mut optimizer = self.optimizer.lock().unwrap().take().unwrap();

        if optimizer.is_heuristic_enabled() {
            // TODO: depjoin pushdown might need to run multiple times
            optd_rel = optimizer.heuristic_optimize(optd_rel);
            if let Some(explains) = &mut explains {
                explains.push(StringifiedPlan::new(
                    PlanType::OptimizedLogicalPlan {
                        optimizer_name: "optd-heuristic".to_string(),
                    },
                    dispatch_plan_explain_to_string(optd_rel.clone(), None),
                ))
            }
            tracing::trace!(
                optd_optimized_plan = %("\n".to_string()
                + &dispatch_plan_explain_to_string(optd_rel.clone(), None)));
        }

        let (group_id, optimized_rel, meta) = optimizer.cascades_optimize(optd_rel)?;

        if let Some(explains) = &mut explains {
            explains.push(StringifiedPlan::new(
                PlanType::OptimizedPhysicalPlan {
                    optimizer_name: "optd".to_string(),
                },
                dispatch_plan_explain_to_string(
                    optimized_rel.clone(),
                    if verbose { Some(&meta) } else { None },
                ),
            ));
            let join_orders = optimizer
                .optd_cascades_optimizer()
                .memo()
                .enumerate_join_order(group_id);
            explains.push(StringifiedPlan::new(
                PlanType::OptimizedPhysicalPlan {
                    optimizer_name: "optd-all-logical-join-orders".to_string(),
                },
                join_orders.iter().map(|x| x.to_string()).join("\n"),
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
        }

        tracing::trace!(
            optd_physical_plan = %("\n".to_string()
            + &dispatch_plan_explain_to_string(optimized_rel.clone(), None)));

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

#[derive(Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
enum JoinOrder {
    Table(String),
    HashJoin(Box<Self>, Box<Self>),
    NestedLoopJoin(Box<Self>, Box<Self>),
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

fn get_join_order(rel_node: ArcDfPlanNode) -> Option<JoinOrder> {
    match rel_node.typ {
        DfNodeType::PhysicalHashJoin(_) => {
            let join = PhysicalHashJoin::from_plan_node(rel_node.clone()).unwrap();
            let left = get_join_order(join.left().unwrap_plan_node())?;
            let right = get_join_order(join.right().unwrap_plan_node())?;
            Some(JoinOrder::HashJoin(Box::new(left), Box::new(right)))
        }
        DfNodeType::PhysicalNestedLoopJoin(_) => {
            let join = PhysicalNestedLoopJoin::from_plan_node(rel_node.clone()).unwrap();
            let left = get_join_order(join.left().unwrap_plan_node())?;
            let right = get_join_order(join.right().unwrap_plan_node())?;
            Some(JoinOrder::NestedLoopJoin(Box::new(left), Box::new(right)))
        }
        DfNodeType::PhysicalScan => {
            let scan =
                optd_datafusion_repr::plan_nodes::PhysicalScan::from_plan_node(rel_node).unwrap();
            Some(JoinOrder::Table(scan.table().to_string()))
        }
        _ => {
            for child in &rel_node.children {
                if let Some(res) = get_join_order(child.unwrap_plan_node()) {
                    return Some(res);
                }
            }
            None
        }
    }
}
