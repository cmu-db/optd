use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use super::plans::ScalarPlan;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LogicalProperties(pub PropertiesData);

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct PhysicalProperties(pub PropertiesData);

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum PropertiesData {
    Int64(i64),
    Float64(OrderedFloat<f64>),
    String(String),
    Bool(bool),
    Struct(String, Vec<PropertiesData>),
    Array(Vec<PropertiesData>),
    Scalar(ScalarPlan),
}
