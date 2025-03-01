use std::sync::Arc;

use super::{
    goal::GoalId,
    groups::{RelationalGroupId, ScalarGroupId},
};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use std::fmt::Debug;

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum OperatorData {
    Int64(i64),
    Float64(OrderedFloat<f64>),
    String(String),
    Bool(bool),
    Struct(String, Vec<OperatorData>),
    Array(Vec<OperatorData>),
}

impl OperatorData {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            OperatorData::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            OperatorData::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            OperatorData::Int64(i) => Some(*i),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Child<T> {
    Singleton(T),
    VarLength(Vec<T>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum PartialLogicalPlan {
    PartialMaterialized {
        tag: String,
        data: Vec<OperatorData>,
        relational_children: Vec<Child<PartialLogicalPlan>>,
        scalar_children: Vec<Child<PartialScalarPlan>>,
        group_id: RelationalGroupId,
    },
    UnMaterialized(RelationalGroupId),
}
pub struct LogicalExpression {
    tag: String,
    data: Vec<OperatorData>,
    relational_children: Vec<Child<RelationalGroupId>>,
    scalar_children: Vec<Child<ScalarGroupId>>,
    group_id: RelationalGroupId,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PartialScalarPlan {
    PartialMaterialized {
        tag: String,
        data: Vec<OperatorData>,
        scalar_children: Vec<Child<PartialScalarPlan>>,
        group_id: ScalarGroupId,
    },
    UnMaterialized(ScalarGroupId),
}
pub struct ScalarExpression {
    tag: String,
    data: Vec<OperatorData>,
    scalar_children: Vec<Child<ScalarGroupId>>,
    group_id: ScalarGroupId,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PartialPhysicalPlan {
    PartialMaterialized {
        tag: String,
        data: Vec<OperatorData>,
        relational_children: Vec<Child<PartialPhysicalPlan>>,
        scalar_children: Vec<Child<PartialScalarPlan>>,
        properties: PhysicalProperties,
        group_id: RelationalGroupId,
    },
    UnMaterialized(GoalId),
}
pub struct PhysicalExpression {
    tag: String,
    data: Vec<OperatorData>,
    relational_children: Vec<Child<RelationalGroupId>>,
    scalar_children: Vec<Child<ScalarGroupId>>,
    group_id: RelationalGroupId,
}

/// A fully materialized physical query plan.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PhysicalPlan {
    tag: String,
    relational_children: Vec<Child<PhysicalPlan>>,
    scalar_children: Vec<Child<PhysicalPlan>>,
    data: Vec<OperatorData>,
}

/// A fully materialized scalar query plan.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScalarPlan {
    tag: String,
    children: Vec<Child<ScalarPlan>>,
    data: Vec<OperatorData>,
}

/// A fully materialized logical query plan.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogicalPlan {
    tag: String,
    relational_children: Vec<Child<LogicalPlan>>,
    scalar_children: Vec<Child<ScalarPlan>>,
    data: Vec<OperatorData>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct PhysicalProperties {
    pub data: Vec<PropertyData>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct LogicalProperties {
    data: Vec<PropertyData>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum PropertyData {
    Int64(i64),
    Scalar(ScalarPlan),
}

/// A stored logical expression in the memo table.
pub type StoredLogicalExpression = (LogicalExpression, LogicalExpressionId);

/// A stored physical expression in the memo table.
pub type StoredPhysicalExpression = (PhysicalExpression, PhysicalExpressionId);

/// A unique identifier for a logical expression in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Deserialize)]
#[sqlx(transparent)]
pub struct LogicalExpressionId(pub i64);

/// A unique identifier for a physical expression in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Deserialize)]
#[sqlx(transparent)]
pub struct PhysicalExpressionId(pub i64);

/// A unique identifier for a scalar expression in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Deserialize)]
#[sqlx(transparent)]
pub struct ScalarExpressionId(pub i64);
