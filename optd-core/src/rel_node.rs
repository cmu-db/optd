//! The RelNode is the basic data structure of the optimizer. It is dynamically typed and is
//! the internal representation of the plan nodes.

use std::{
    fmt::{Debug, Display},
    hash::Hash,
    sync::Arc,
};

use ordered_float::OrderedFloat;

use crate::cascades::GroupId;

pub type RelNodeRef<T> = Arc<RelNode<T>>;

pub trait RelNodeTyp:
    PartialEq + Eq + Hash + Clone + Copy + 'static + Display + Debug + Send + Sync
{
    fn is_logical(&self) -> bool;

    fn group_typ(group_id: GroupId) -> Self;

    fn extract_group(&self) -> Option<GroupId>;

    fn list_typ() -> Self;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    Int(i64),
    Float(OrderedFloat<f64>),
    String(Arc<str>),
    Bool(bool),
    Serialized(Arc<[u8]>),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(x) => write!(f, "{x}"),
            Self::Float(x) => write!(f, "{x}"),
            Self::String(x) => write!(f, "\"{x}\""),
            Self::Bool(x) => write!(f, "{x}"),
            Self::Serialized(x) => write!(f, "<len:{}>", x.len()),
        }
    }
}

impl Value {
    pub fn as_i64(&self) -> i64 {
        match self {
            Value::Int(i) => *i,
            _ => panic!("Value is not an i64"),
        }
    }

    pub fn as_str(&self) -> Arc<str> {
        match self {
            Value::String(i) => i.clone(),
            _ => panic!("Value is not a string"),
        }
    }
}

/// A RelNode is consisted of a plan node type and some children.
#[derive(Clone, Debug)]
pub struct RelNode<T: RelNodeTyp> {
    pub typ: T,
    pub children: Vec<RelNodeRef<T>>,
    pub data: Option<Value>,
}

impl<T: RelNodeTyp> std::fmt::Display for RelNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}", self.typ)?;
        if let Some(ref data) = self.data {
            write!(f, " {}", data)?;
        }
        for child in &self.children {
            write!(f, " {}", child)?;
        }
        write!(f, ")")
    }
}

impl<T: RelNodeTyp> RelNode<T> {
    pub fn child(&self, idx: usize) -> RelNodeRef<T> {
        if idx >= self.children.len() {
            panic!("child index {} out of range: {}", idx, self);
        }
        self.children[idx].clone()
    }

    pub fn new_leaf(typ: T) -> Self {
        Self {
            typ,
            data: None,
            children: Vec::new(),
        }
    }

    pub fn new_group(group_id: GroupId) -> Self {
        Self::new_leaf(T::group_typ(group_id))
    }

    pub fn new_list(items: Vec<RelNodeRef<T>>) -> Self {
        Self {
            typ: T::list_typ(),
            data: None,
            children: items,
        }
    }
}
