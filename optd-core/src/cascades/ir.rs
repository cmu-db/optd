use std::sync::Arc;

use super::{
    goal::GoalId,
    groups::{RelationalGroupId, ScalarGroupId},
};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

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
    data: Vec<PropertyData>,
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