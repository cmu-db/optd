use serde::{Deserialize, Serialize};

use super::plans::ScalarPlan;

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
