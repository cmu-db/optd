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
    plan_nodes::{ConstantType, OptRelNode, PlanNode},
    properties::schema::Catalog,
    DatafusionOptimizer, MemoExt,
};
use std::{
    collections::HashMap,
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
                PlanNode::from_rel_node(optd_rel.clone())
                    .unwrap()
                    .explain_to_string(None),
            ));
        }

        tracing::trace!(
            optd_unoptimized_plan = %("\n".to_string()
            + &PlanNode::from_rel_node(optd_rel.clone())
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
                    PlanNode::from_rel_node(optd_rel.clone())
                        .unwrap()
                        .explain_to_string(None),
                ))
            }
            tracing::trace!(
                optd_optimized_plan = %("\n".to_string()
                + &PlanNode::from_rel_node(optd_rel.clone())
                    .unwrap()
                    .explain_to_string(None)));
        }

        let (group_id, optimized_rel, meta) = optimizer.cascades_optimize(optd_rel)?;

        if let Some(explains) = &mut explains {
            explains.push(StringifiedPlan::new(
                PlanType::OptimizedPhysicalPlan {
                    optimizer_name: "optd".to_string(),
                },
                PlanNode::from_rel_node(optimized_rel.clone())
                    .unwrap()
                    .explain_to_string(if verbose { Some(&meta) } else { None }),
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
        }

        tracing::trace!(
            optd_physical_plan = %("\n".to_string()
            + &PlanNode::from_rel_node(optimized_rel.clone())
                .unwrap()
                .explain_to_string(None)));

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
