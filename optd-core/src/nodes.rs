// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! The RelNode is the basic data structure of the optimizer. It is dynamically typed and is
//! the internal representation of the plan nodes.

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::sync::Arc;

use arrow_schema::DataType;
use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::cascades::GroupId;
use crate::cost::{Cost, Statistics};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SerializableOrderedF64(pub OrderedFloat<f64>);

impl Serialize for SerializableOrderedF64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Directly serialize the inner f64 value of the OrderedFloat
        self.0 .0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SerializableOrderedF64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialize an f64 and wrap it in an OrderedFloat
        let float = f64::deserialize(deserializer)?;
        Ok(SerializableOrderedF64(OrderedFloat(float)))
    }
}

// TODO: why not use arrow types here? Do we really need to define our own Value type?
// Shouldn't we at least remove this from the core/engine?
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
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
    Float(SerializableOrderedF64),
    String(Arc<str>),
    Bool(bool),
    Date32(i32),
    Decimal128(i128),
    Serialized(Arc<[u8]>),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UInt8(x) => write!(f, "{x}(u8)"),
            Self::UInt16(x) => write!(f, "{x}(u16)"),
            Self::UInt32(x) => write!(f, "{x}(u32)"),
            Self::UInt64(x) => write!(f, "{x}(u64)"),
            Self::Int8(x) => write!(f, "{x}(i8)"),
            Self::Int16(x) => write!(f, "{x}(i16)"),
            Self::Int32(x) => write!(f, "{x}(i32)"),
            Self::Int64(x) => write!(f, "{x}(i64)"),
            Self::Int128(x) => write!(f, "{x}(i128)"),
            Self::Float(x) => write!(f, "{}(float)", x.0),
            Self::String(x) => write!(f, "\"{x}\""),
            Self::Bool(x) => write!(f, "{x}"),
            Self::Date32(x) => write!(f, "{x}(date32)"),
            Self::Decimal128(x) => write!(f, "{x}(decimal128)"),
            Self::Serialized(x) => write!(f, "<len:{}>", x.len()),
        }
    }
}

/// The `as_*()` functions do not perform conversions. This is *unlike* the `as`
/// keyword in rust.
///
/// If you want to perform conversions, use the `to_*()` functions.
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
            Value::Float(i) => *i.0,
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

    pub fn as_slice(&self) -> Arc<[u8]> {
        match self {
            Value::Serialized(i) => i.clone(),
            _ => panic!("Value is not a serialized"),
        }
    }

    pub fn convert_to_type(&self, typ: DataType) -> Value {
        match typ {
            DataType::Int32 => Value::Int32(match self {
                Value::Int32(i32) => *i32,
                Value::Int64(i64) => (*i64).try_into().unwrap(),
                _ => panic!("{self} could not be converted into an Int32"),
            }),
            DataType::Int64 => Value::Int64(match self {
                Value::Int64(i64) => *i64,
                Value::Int32(i32) => (*i32).into(),
                _ => panic!("{self} could not be converted into an Int64"),
            }),
            DataType::UInt64 => Value::UInt64(match self {
                Value::Int64(i64) => (*i64).try_into().unwrap(),
                Value::UInt64(i64) => *i64,
                Value::UInt32(i32) => (*i32).into(),
                _ => panic!("{self} could not be converted into an UInt64"),
            }),
            DataType::Date32 => Value::Date32(match self {
                Value::Date32(date32) => *date32,
                Value::String(str) => {
                    let date = NaiveDate::parse_from_str(str, "%Y-%m-%d").unwrap();
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let duration_since_epoch = date.signed_duration_since(epoch);
                    let days_since_epoch: i32 = duration_since_epoch.num_days() as i32;
                    days_since_epoch
                }
                _ => panic!("{self} could not be converted into an Date32"),
            }),
            _ => unimplemented!("Have not implemented convert_to_type for DataType {typ}"),
        }
    }
}

pub trait NodeType:
    PartialEq + Eq + Hash + Clone + 'static + Display + Debug + Send + Sync
{
    type PredType: PartialEq + Eq + Hash + Clone + 'static + Display + Debug + Send + Sync;

    fn is_logical(&self) -> bool;
}

/// A pointer to a plan node
pub type ArcPlanNode<T> = Arc<PlanNode<T>>;

/// A pointer to a predicate node
pub type ArcPredNode<T> = Arc<PredNode<T>>;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum PlanNodeOrGroup<T: NodeType> {
    PlanNode(ArcPlanNode<T>),
    Group(GroupId),
}

impl<T: NodeType> PlanNodeOrGroup<T> {
    pub fn is_materialized(&self) -> bool {
        match self {
            PlanNodeOrGroup::PlanNode(_) => true,
            PlanNodeOrGroup::Group(_) => false,
        }
    }

    pub fn unwrap_typ(&self) -> T {
        self.unwrap_plan_node().typ.clone()
    }

    pub fn unwrap_plan_node(&self) -> ArcPlanNode<T> {
        match self {
            PlanNodeOrGroup::PlanNode(node) => node.clone(),
            PlanNodeOrGroup::Group(_) => panic!("Expected PlanNode, found Group"),
        }
    }

    pub fn unwrap_group(&self) -> GroupId {
        match self {
            PlanNodeOrGroup::PlanNode(_) => panic!("Expected Group, found PlanNode"),
            PlanNodeOrGroup::Group(group_id) => *group_id,
        }
    }
}

impl<T: NodeType> std::fmt::Display for PlanNodeOrGroup<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanNodeOrGroup::PlanNode(node) => write!(f, "{}", node),
            PlanNodeOrGroup::Group(group_id) => write!(f, "{}", group_id),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PlanNode<T: NodeType> {
    /// A generic plan node type
    pub typ: T,
    /// Child plan nodes, which may be materialized or placeholder group IDs
    /// based on how this node was initialized
    pub children: Vec<PlanNodeOrGroup<T>>,
    /// Predicate nodes, which are always materialized
    pub predicates: Vec<ArcPredNode<T>>,
}

impl<T: NodeType> std::fmt::Display for PlanNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}", self.typ)?;
        for child in &self.children {
            write!(f, " {}", child)?;
        }
        write!(f, ")")
    }
}

impl<T: NodeType> PlanNode<T> {
    pub fn child(&self, idx: usize) -> PlanNodeOrGroup<T> {
        self.children[idx].clone()
    }

    pub fn child_rel(&self, idx: usize) -> ArcPlanNode<T> {
        self.child(idx).unwrap_plan_node()
    }

    pub fn predicate(&self, idx: usize) -> ArcPredNode<T> {
        self.predicates[idx].clone()
    }
}

impl<T: NodeType> From<PlanNode<T>> for PlanNodeOrGroup<T> {
    fn from(value: PlanNode<T>) -> Self {
        Self::PlanNode(value.into())
    }
}

impl<T: NodeType> From<ArcPlanNode<T>> for PlanNodeOrGroup<T> {
    fn from(value: ArcPlanNode<T>) -> Self {
        Self::PlanNode(value)
    }
}

impl<T: NodeType> From<GroupId> for PlanNodeOrGroup<T> {
    fn from(value: GroupId) -> Self {
        Self::Group(value)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PredNode<T: NodeType> {
    /// A generic predicate node type
    pub typ: T::PredType,
    /// Child predicate nodes, always materialized
    pub children: Vec<ArcPredNode<T>>,
    /// Data associated with the predicate, if any
    pub data: Option<Value>,
}

impl<T: NodeType> std::fmt::Display for PredNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}", self.typ)?;
        for child in &self.children {
            write!(f, " {}", child)?;
        }
        if let Some(data) = &self.data {
            write!(f, " {}", data)?;
        }
        write!(f, ")")
    }
}
impl<T: NodeType> PredNode<T> {
    pub fn child(&self, idx: usize) -> ArcPredNode<T> {
        self.children[idx].clone()
    }

    pub fn unwrap_data(&self) -> Value {
        self.data.clone().unwrap()
    }
}

/// Metadata for a rel node.
#[derive(Clone)]
pub struct PlanNodeMeta {
    /// The group (id) of the `RelNode`
    pub group_id: GroupId,
    /// Weighted cost of the `RelNode`
    pub weighted_cost: f64,
    /// Cost of the `RelNode`
    pub cost: Cost,
    /// Statistics
    pub stat: Arc<Statistics>,
    /// Cost in display string
    /// TODO: this should be lazily processed and generated
    pub cost_display: String,
    /// Statistics in display string
    /// TODO: this should be lazily processed and generated
    pub stat_display: String,
}

impl PlanNodeMeta {
    pub fn new(
        group_id: GroupId,
        weighted_cost: f64,
        cost: Cost,
        stat: Arc<Statistics>,
        cost_display: String,
        stat_display: String,
    ) -> Self {
        Self {
            group_id,
            weighted_cost,
            cost,
            stat,
            cost_display,
            stat_display,
        }
    }
}

/// A hash table storing `RelNode` (memory address, metadata) pairs.
pub type PlanNodeMetaMap = HashMap<usize, PlanNodeMeta>;
