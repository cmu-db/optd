use super::groups::{RelationalGroupId, ScalarGroupId};
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

#[derive(Clone, Debug, PartialEq)]
pub enum Children<T> {
    Singleton(T),
    VarLength(Vec<T>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum PartialLogicalPlan {
    PartialMaterialized {
        tag: String,
        data: Vec<OperatorData>,
        relational_children: Vec<Children<PartialLogicalPlan>>,
        scalar_children: Vec<Children<PartialScalarPlan>>,
    },
    UnMaterialized(RelationalGroupId),
}

#[derive(Clone, Debug, PartialEq)]
pub enum PartialScalarPlan {
    PartialMaterialized {
        tag: String,
        data: Vec<OperatorData>,
        scalar_children: Vec<Children<PartialScalarPlan>>,
    },
    UnMaterialized(ScalarGroupId),
}

#[derive(Clone, Debug, PartialEq)]
pub enum PartialPhysicalPlan {
    PartialMaterialized {
        tag: String,
        data: Vec<OperatorData>,
        relational_children: Vec<Children<PartialPhysicalPlan>>,
        scalar_children: Vec<Children<PartialScalarPlan>>,
    },
    UnMaterialized(RelationalGroupId),
}