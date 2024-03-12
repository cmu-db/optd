//! The RelNode is the basic data structure of the optimizer. It is dynamically typed and is
//! the internal representation of the plan nodes.

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    hash::Hash,
    sync::Arc,
};

use ordered_float::OrderedFloat;

use crate::{cascades::GroupId, cost::Cost};

pub type RelNodeRef<T> = Arc<RelNode<T>>;

pub trait RelNodeTyp:
    PartialEq + Eq + Hash + Clone + 'static + Display + Debug + Send + Sync
{
    fn is_logical(&self) -> bool;

    fn group_typ(group_id: GroupId) -> Self;

    fn extract_group(&self) -> Option<GroupId>;

    fn list_typ() -> Self;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    Float(OrderedFloat<f64>),
    String(Arc<str>),
    Bool(bool),
    Date32(i32),
    Decimal128(i128),
    Serialized(Arc<[u8]>),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UInt8(x) => write!(f, "{x}"),
            Self::UInt16(x) => write!(f, "{x}"),
            Self::UInt32(x) => write!(f, "{x}"),
            Self::UInt64(x) => write!(f, "{x}"),
            Self::Int8(x) => write!(f, "{x}"),
            Self::Int16(x) => write!(f, "{x}"),
            Self::Int32(x) => write!(f, "{x}"),
            Self::Int64(x) => write!(f, "{x}"),
            Self::Int128(x) => write!(f, "{x}"),
            Self::Float(x) => write!(f, "{x}"),
            Self::String(x) => write!(f, "\"{x}\""),
            Self::Bool(x) => write!(f, "{x}"),
            Self::Date32(x) => write!(f, "{x}"),
            Self::Decimal128(x) => write!(f, "{x}"),
            Self::Serialized(x) => write!(f, "<len:{}>", x.len()),
        }
    }
}

impl Value {
    pub fn as_u8(&self) -> u8 {
        match self {
            Value::UInt8(i) => *i,
            _ => panic!("Value is not an u8"),
        }
    }

    pub fn as_u16(&self) -> u16 {
        match self {
            Value::UInt16(i) => *i,
            _ => panic!("Value is not an u16"),
        }
    }

    pub fn as_u32(&self) -> u32 {
        match self {
            Value::UInt32(i) => *i,
            _ => panic!("Value is not an u32"),
        }
    }

    pub fn as_u64(&self) -> u64 {
        match self {
            Value::UInt64(i) => *i,
            _ => panic!("Value is not an u64"),
        }
    }

    pub fn as_i8(&self) -> i8 {
        match self {
            Value::Int8(i) => *i,
            _ => panic!("Value is not an i8"),
        }
    }

    pub fn as_i16(&self) -> i16 {
        match self {
            Value::Int16(i) => *i,
            _ => panic!("Value is not an i16"),
        }
    }

    pub fn as_i32(&self) -> i32 {
        match self {
            Value::Int32(i) => *i,
            _ => panic!("Value is not an i32"),
        }
    }

    pub fn as_i64(&self) -> i64 {
        match self {
            Value::Int64(i) => *i,
            _ => panic!("Value is not an i64"),
        }
    }

    pub fn as_i128(&self) -> i128 {
        match self {
            Value::Int128(i) => *i,
            _ => panic!("Value is not an i128"),
        }
    }

    pub fn as_f64(&self) -> f64 {
        match self {
            Value::Float(i) => **i,
            _ => panic!("Value is not an f64"),
        }
    }

    pub fn as_bool(&self) -> bool {
        match self {
            Value::Bool(i) => *i,
            _ => panic!("Value is not a bool"),
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
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
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

/// Metadata for a rel node.
#[derive(Clone, Debug, PartialEq)]
pub struct RelNodeMeta {
    /// The group (id) of the `RelNode`
    pub group_id: GroupId,
    /// Cost of the `RelNode`
    pub cost: Cost,
}

impl RelNodeMeta {
    pub fn new(group_id: GroupId, cost: Cost) -> Self {
        RelNodeMeta { group_id, cost }
    }
}

/// A hash table storing `RelNode` (memory address, metadata) pairs.
pub type RelNodeMetaMap = HashMap<usize, RelNodeMeta>;
