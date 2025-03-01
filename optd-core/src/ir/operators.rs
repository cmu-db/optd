use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Child<T> {
    Singleton(T),
    VarLength(Vec<T>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScalarOperator<S> {
    tag: String,
    data: Vec<OperatorData>,
    children: Vec<Child<S>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalOperator<R, S> {
    tag: String,
    data: Vec<OperatorData>,
    relational_children: Vec<Child<R>>,
    scalar_children: Vec<Child<S>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalOperator<R, S> {
    tag: String,
    data: Vec<OperatorData>,
    relational_children: Vec<Child<R>>,
    scalar_children: Vec<Child<S>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperatorData {
    Int64(i64),
    Float64(OrderedFloat<f64>),
    String(String),
    Bool(bool),
    Struct(String, Vec<OperatorData>),
    Array(Vec<OperatorData>),
}
