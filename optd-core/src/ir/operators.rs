use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Child<T> {
    Singleton(T),
    VarLength(Vec<T>),
}

impl<T> Child<T> {
    pub fn from<U>(original: Child<U>) -> Child<T>
    where
        T: From<U>,
    {
        match original {
            Child::Singleton(u) => Child::Singleton(T::from(u)),
            Child::VarLength(us) => Child::VarLength(us.into_iter().map(T::from).collect()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScalarOperator<S> {
    pub tag: String,
    pub data: Vec<OperatorData>,
    pub children: Vec<Child<S>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalOperator<R, S> {
    pub tag: String,
    pub data: Vec<OperatorData>,
    pub relational_children: Vec<Child<R>>,
    pub scalar_children: Vec<Child<S>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalOperator<R, S> {
    pub tag: String,
    pub data: Vec<OperatorData>,
    pub relational_children: Vec<Child<R>>,
    pub scalar_children: Vec<Child<S>>,
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
