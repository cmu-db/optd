use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use super::plans::ScalarPlan;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LogicalProperties {
    data: Vec<PropertyData>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct PhysicalProperties {
    pub data: Vec<PropertyData>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum PropertyData {
    Int64(i64),
    Float64(OrderedFloat<f64>),
    String(String),
    Bool(bool),
    Struct(String, Vec<PropertyData>),
    Array(Vec<PropertyData>),
    Scalar(ScalarPlan),
}
