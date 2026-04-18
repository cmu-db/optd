mod get_or_create_query_id;
mod get_query;
mod get_query_by_sql;
mod get_query_instance;
mod get_query_instances;
mod get_query_plans;
mod log_query_instance;

use chrono::{DateTime, Utc};
use sea_orm::prelude::Json;

use crate::entity::{query, query_instance, query_plan};

pub use get_or_create_query_id::*;
pub use get_query::*;
pub use get_query_by_sql::*;
pub use get_query_instance::*;
pub use get_query_instances::*;
pub use get_query_plans::*;
pub use log_query_instance::*;

/// JSON-encoded query plan payload.
pub type QueryPlan = Json;

pub const INITIAL_PLAN_DESCRIPTION: &str = "initial-plan";
pub const FINAL_PLAN_DESCRIPTION: &str = "final-plan";

/// Information needed to log a query plan.
#[derive(Debug, Clone, PartialEq)]
pub struct LogQueryPlanInfo {
    /// JSON-encoded plan payload.
    pub plan: QueryPlan,
    /// Description of this plan.
    pub description: String,
}

/// Information needed to log a query instance.
#[derive(Debug, Clone, PartialEq)]
pub struct LogQueryInstanceInfo {
    /// SQL text for the query. Matching is exact, without normalization.
    pub sql: String,
    /// Snapshot id the query instance runs against.
    pub snapshot_id: i64,
    /// Query plans recorded for this instance.
    pub query_plans: Vec<LogQueryPlanInfo>,
}

/// Query instance lookup selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryInstanceSelector {
    /// Select query instances by query id.
    QueryId(i64),
    /// Select query instances whose query SQL matches exactly.
    Sql(String),
}

/// Stored SQL query metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryInfo {
    /// Numeric query id.
    pub query_id: i64,
    /// SQL text for the query.
    pub sql: String,
}

/// Stored query instance metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryInstanceInfo {
    /// Numeric query instance id.
    pub id: i64,
    /// Query id reused across exact SQL matches.
    pub query_id: i64,
    /// Snapshot id the query instance runs against.
    pub snapshot_id: i64,
    /// Time at which the query instance was created.
    pub query_time: DateTime<Utc>,
    /// Query plans recorded for this instance.
    pub query_plans: Vec<QueryPlanInfo>,
}

/// Stored query plan metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlanInfo {
    /// Numeric query plan id.
    pub id: i64,
    /// Query instance this plan belongs to.
    pub query_instance_id: i64,
    /// JSON-encoded plan payload.
    pub plan: QueryPlan,
    /// Description of this plan.
    pub description: String,
}

impl From<query::Model> for QueryInfo {
    fn from(query: query::Model) -> Self {
        Self {
            query_id: query.query_id,
            sql: query.sql,
        }
    }
}

impl From<query_plan::Model> for QueryPlanInfo {
    fn from(query_plan: query_plan::Model) -> Self {
        Self {
            id: query_plan.id,
            query_instance_id: query_plan.query_instance_id,
            plan: query_plan.plan,
            description: query_plan.description,
        }
    }
}

fn query_instance_info_from_parts(
    query_instance: query_instance::Model,
    query_plans: Vec<QueryPlanInfo>,
) -> QueryInstanceInfo {
    QueryInstanceInfo {
        id: query_instance.id,
        query_id: query_instance.query_id,
        snapshot_id: query_instance.snapshot_id,
        query_time: query_instance.query_time,
        query_plans,
    }
}
