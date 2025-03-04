use super::plans::ScalarPlan;

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalProperties(pub Option<PropertiesData>);

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalProperties(pub Option<PropertiesData>);

#[derive(Debug, Clone, PartialEq)]
pub enum PropertiesData {
    Int64(i64),
    Float64(f64),
    String(String),
    Bool(bool),
    Struct(String, Vec<PropertiesData>),
    Array(Vec<PropertiesData>),
    Scalar(ScalarPlan),
}
