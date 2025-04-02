//! Custom Map implementation for the DSL.
//!
//! This module provides a Map type that enforces specific key constraints at runtime
//! rather than compile time. It bridges between the flexible HIR Value type and
//! a more restricted internal MapKey type that implements Hash and Eq.
//!
//! The Map implementation supports a subset of Value types as keys:
//! - Literals (except Float64, which doesn't implement Hash/Eq)
//! - Nested structures (Option, Logical, Physical, Fail, Struct, Tuple)
//!   if all their contents are also valid key types
//!
//! The implementation provides:
//! - Conversion from Value to MapKey (with runtime validation)
//! - Efficient key lookup (O(1) via HashMap)
//! - Basic map operations (get, concat)

use super::hir::{
    CoreData, Goal, GroupId, Literal, LogicalOp, Materializable, Operator, PhysicalOp, Value,
};
use CoreData::*;
use std::collections::HashMap;
use std::hash::Hash;

/// Map key representation of a logical operator
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct OperatorMapKey<T> {
    pub tag: String,
    pub data: Vec<T>,
    pub children: Vec<T>,
}

/// Map key representation of a logical operator
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct LogicalMapOpKey {
    pub operator: OperatorMapKey<MapKey>,
    pub group_id: Option<GroupId>,
}

/// Map key representation of a physical operator
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PhysicalMapOpKey {
    pub operator: OperatorMapKey<MapKey>,
    pub goal: Option<GoalMapKey>,
    pub cost: Option<MapKey>,
}

/// Map key representation of a goal
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct GoalMapKey {
    pub group_id: GroupId,
    pub properties: MapKey,
}

/// Map key representation of logical operators (materialized or unmaterialized)
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum LogicalMapKey {
    Materialized(LogicalMapOpKey),
    UnMaterialized(GroupId),
}

/// Map key representation of physical operators (materialized or unmaterialized)
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum PhysicalMapKey {
    Materialized(PhysicalMapOpKey),
    UnMaterialized(GoalMapKey),
}

/// Internal key type that implements Hash, Eq, PartialEq
/// These are the only types that can be used as keys in the Map
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum MapKey {
    /// Primitive literal values (except Float64)
    Int64(i64),
    String(String),
    Bool(bool),
    Unit,

    /// Nested structures
    Tuple(Vec<MapKey>),
    Struct(String, Vec<MapKey>),

    /// Query structures
    Logical(Box<LogicalMapKey>),
    Physical(Box<PhysicalMapKey>),

    /// or representation
    Fail(Box<MapKey>),

    /// None value
    None,
}

/// Custom Map implementation that enforces key type constraints at runtime
#[derive(Clone)]
pub struct Map {
    inner: HashMap<MapKey, Value>,
}

impl Map {
    /// Creates a new empty Map
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Creates a Map from a collection of key-value pairs
    pub fn from_pairs(pairs: Vec<(Value, Value)>) -> Self {
        pairs.into_iter().fold(Self::new(), |mut map, (k, v)| {
            map.insert(k, v);
            map
        })
    }

    /// Gets a value by key, returning None (as a Value) if not found
    pub fn get(&self, key: &Value) -> Value {
        self.inner
            .get(&value_to_map_key(key))
            .unwrap_or(&Value(None))
            .clone()
    }

    /// Combines two maps, with values from other overriding values from self when keys collide
    pub fn concat(&self, other: &Map) -> Self {
        let mut result = self.clone();
        result.inner.extend(other.inner.clone());
        result
    }

    /// Checks if the map is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Inserts a key-value pair into the map
    /// Panics if the key type is not supported (e.g., Float64 or Array)
    pub fn insert(&mut self, key: Value, value: Value) {
        let map_key = value_to_map_key(&key);
        self.inner.insert(map_key, value);
    }

    /// Converts the map into a Value representation
    pub fn to_value(&self) -> Value {
        let pairs = self
            .inner
            .iter()
            .map(|(k, v)| (map_key_to_value(k), v.clone()))
            .collect();
        Value(Map(pairs))
    }
}

// Key conversion functions

/// Converts a Value to a MapKey, enforcing valid key types
/// This performs runtime validation that the key type is supported
/// and will return an error for invalid key types
fn value_to_map_key(value: &Value) -> MapKey {
    match &value.0 {
        Literal(lit) => match lit {
            Literal::Int64(i) => MapKey::Int64(*i),
            Literal::String(s) => MapKey::String(s.clone()),
            Literal::Bool(b) => MapKey::Bool(*b),
            Literal::Unit => MapKey::Unit,
            Literal::Float64(_) => panic!("Invalid map key: Float64"),
        },
        Tuple(items) => {
            let key_items = items.iter().map(value_to_map_key).collect();
            MapKey::Tuple(key_items)
        }
        Struct(name, fields) => {
            let key_fields = fields.iter().map(value_to_map_key).collect();
            MapKey::Struct(name.clone(), key_fields)
        }
        Logical(materializable) => match materializable {
            Materializable::UnMaterialized(group_id) => {
                MapKey::Logical(Box::new(LogicalMapKey::UnMaterialized(*group_id)))
            }
            Materializable::Materialized(logical_op) => {
                let map_op = value_to_logical_map_op(logical_op);
                MapKey::Logical(Box::new(LogicalMapKey::Materialized(map_op)))
            }
        },
        Physical(materializable) => match materializable {
            Materializable::UnMaterialized(goal) => {
                let properties = value_to_map_key(&goal.properties);
                let map_goal = GoalMapKey {
                    group_id: goal.group_id,
                    properties,
                };
                MapKey::Physical(Box::new(PhysicalMapKey::UnMaterialized(map_goal)))
            }
            Materializable::Materialized(physical_op) => {
                let map_op = value_to_physical_map_op(physical_op);
                MapKey::Physical(Box::new(PhysicalMapKey::Materialized(map_op)))
            }
        },
        Fail(inner) => {
            let inner_key = value_to_map_key(inner);
            MapKey::Fail(Box::new(inner_key))
        }
        None => MapKey::None,
        _ => panic!("Invalid map key: {:?}", value),
    }
}

/// Converts an Operator to a map key operator
fn value_to_operator_map_key<T>(
    operator: &Operator<T>,
    value_converter: &dyn Fn(&T) -> MapKey,
) -> OperatorMapKey<MapKey> {
    let data = operator.data.iter().map(value_converter).collect();

    let children = operator.children.iter().map(value_converter).collect();

    OperatorMapKey {
        tag: operator.tag.clone(),
        data,
        children,
    }
}

/// Converts a LogicalOp to a map key
fn value_to_logical_map_op(logical_op: &LogicalOp<Value>) -> LogicalMapOpKey {
    let operator = value_to_operator_map_key(&logical_op.operator, &value_to_map_key);

    LogicalMapOpKey {
        operator,
        group_id: logical_op.group_id,
    }
}

/// Converts a PhysicalOp to a map key
fn value_to_physical_map_op(physical_op: &PhysicalOp<Value>) -> PhysicalMapOpKey {
    let operator = value_to_operator_map_key(&physical_op.operator, &value_to_map_key);

    let goal = physical_op.goal.as_ref().map(|g| GoalMapKey {
        group_id: g.group_id,
        properties: value_to_map_key(&g.properties),
    });

    let cost = physical_op.cost.as_ref().map(|c| value_to_map_key(c));

    PhysicalMapOpKey {
        operator,
        goal,
        cost,
    }
}

/// Converts a MapKey back to a Value
fn map_key_to_value(key: &MapKey) -> Value {
    match key {
        MapKey::Int64(i) => Value(Literal(Literal::Int64(*i))),
        MapKey::String(s) => Value(Literal(Literal::String(s.clone()))),
        MapKey::Bool(b) => Value(Literal(Literal::Bool(*b))),
        MapKey::Unit => Value(Literal(Literal::Unit)),
        MapKey::Tuple(items) => {
            let values = items.iter().map(map_key_to_value).collect();
            Value(Tuple(values))
        }
        MapKey::Struct(name, fields) => {
            let values = fields.iter().map(map_key_to_value).collect();
            Value(Struct(name.clone(), values))
        }
        MapKey::Logical(logical_key) => match &**logical_key {
            LogicalMapKey::Materialized(op) => {
                let operator_value = logical_map_op_to_value(op);
                Value(Logical(Materializable::Materialized(operator_value)))
            }
            LogicalMapKey::UnMaterialized(id) => {
                Value(Logical(Materializable::UnMaterialized(*id)))
            }
        },
        MapKey::Physical(physical_key) => match &**physical_key {
            PhysicalMapKey::Materialized(op) => {
                let operator_value = physical_map_op_to_value(op);
                Value(Physical(Materializable::Materialized(operator_value)))
            }
            PhysicalMapKey::UnMaterialized(g) => {
                let goal = Goal {
                    group_id: g.group_id,
                    properties: Box::new(map_key_to_value(&g.properties)),
                };
                Value(Physical(Materializable::UnMaterialized(goal)))
            }
        },
        MapKey::Fail(inner) => {
            let inner_value = map_key_to_value(inner);
            Value(Fail(Box::new(inner_value)))
        }
        MapKey::None => Value(None),
    }
}

/// Converts an operator map key back to a Value operator
fn operator_map_key_to_value<T>(
    operator: &OperatorMapKey<MapKey>,
    key_converter: &dyn Fn(&MapKey) -> T,
) -> Operator<T> {
    let data = operator.data.iter().map(key_converter).collect();
    let children = operator.children.iter().map(key_converter).collect();

    Operator {
        tag: operator.tag.clone(),
        data,
        children,
    }
}

/// Converts a logical map op key back to a Value logical op
fn logical_map_op_to_value(logical_op: &LogicalMapOpKey) -> LogicalOp<Value> {
    let operator = operator_map_key_to_value(&logical_op.operator, &map_key_to_value);

    LogicalOp {
        operator,
        group_id: logical_op.group_id,
    }
}

/// Converts a physical map op key back to a Value physical op
fn physical_map_op_to_value(physical_op: &PhysicalMapOpKey) -> PhysicalOp<Value> {
    let operator = operator_map_key_to_value(&physical_op.operator, &map_key_to_value);

    let goal = physical_op.goal.as_ref().map(|g| Goal {
        group_id: g.group_id,
        properties: Box::new(map_key_to_value(&g.properties)),
    });

    let cost = physical_op
        .cost
        .as_ref()
        .map(|c| Box::new(map_key_to_value(c)));

    PhysicalOp {
        operator,
        goal,
        cost,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::tests::{
        array_val, assert_values_equal, boolean, int, lit_val, string, struct_val,
    };

    // Helper to create Value literals
    fn int_val(i: i64) -> Value {
        lit_val(int(i))
    }

    fn bool_val(b: bool) -> Value {
        lit_val(boolean(b))
    }

    fn string_val(s: &str) -> Value {
        lit_val(string(s))
    }

    fn float_val(f: f64) -> Value {
        Value(Literal(Literal::Float64(f)))
    }

    fn tuple_val(items: Vec<Value>) -> Value {
        Value(Tuple(items))
    }

    #[test]
    fn test_simple_map_operations() {
        let mut map = Map::new();

        // Insert key-value pairs
        map.insert(int_val(1), string_val("one"));
        map.insert(int_val(2), string_val("two"));

        // Check retrieval
        assert_values_equal(&map.get(&int_val(1)), &string_val("one"));
        assert_values_equal(&map.get(&int_val(2)), &string_val("two"));
        assert_values_equal(&map.get(&int_val(3)), &Value(None)); // Non-existent key
    }

    #[test]
    fn test_map_from_pairs() {
        let pairs = vec![
            (int_val(1), string_val("one")),
            (int_val(2), string_val("two")),
        ];

        let map = Map::from_pairs(pairs);

        assert_values_equal(&map.get(&int_val(1)), &string_val("one"));
        assert_values_equal(&map.get(&int_val(2)), &string_val("two"));
    }

    #[test]
    fn test_map_concat() {
        let mut map1 = Map::new();
        map1.insert(int_val(1), string_val("one"));
        map1.insert(int_val(2), string_val("two"));

        let mut map2 = Map::new();
        map2.insert(int_val(2), string_val("TWO")); // Overwrite key 2
        map2.insert(int_val(3), string_val("three"));

        let combined = map1.concat(&map2);

        assert_values_equal(&combined.get(&int_val(1)), &string_val("one"));
        assert_values_equal(&combined.get(&int_val(2)), &string_val("TWO")); // Overwritten
        assert_values_equal(&combined.get(&int_val(3)), &string_val("three"));
    }

    #[test]
    fn test_complex_keys() {
        let mut map = Map::new();

        // Tuple key
        let tuple_key = tuple_val(vec![int_val(1), string_val("test")]);
        map.insert(tuple_key.clone(), string_val("tuple value"));

        // Struct key
        let struct_key = struct_val("Person", vec![string_val("John"), int_val(30)]);
        map.insert(struct_key.clone(), string_val("struct value"));

        // Retrieve values
        assert_values_equal(&map.get(&tuple_key), &string_val("tuple value"));
        assert_values_equal(&map.get(&struct_key), &string_val("struct value"));
    }

    #[test]
    #[should_panic(expected = "Invalid map key")]
    fn test_float_key_panics() {
        let mut map = Map::new();
        map.insert(float_val(3.14), string_val("pi"));
    }

    #[test]
    #[should_panic(expected = "Invalid map key")]
    fn test_tuple_with_float_panics() {
        let mut map = Map::new();
        let tuple_with_float = tuple_val(vec![int_val(1), float_val(2.5)]);
        map.insert(tuple_with_float, string_val("invalid"));
    }

    #[test]
    #[should_panic(expected = "Invalid map key")]
    fn test_array_key_panics() {
        let mut map = Map::new();
        let array_key = array_val(vec![int_val(1), int_val(2)]);
        map.insert(array_key, string_val("invalid"));
    }

    #[test]
    fn test_get_with_invalid_key() {
        let mut map = Map::new();
        map.insert(int_val(1), string_val("one"));

        // Getting with a float key should return None
        assert_values_equal(&map.get(&float_val(1.0)), &Value(None));

        // Getting with an array key should return None
        let array_key = array_val(vec![int_val(1)]);
        assert_values_equal(&map.get(&array_key), &Value(None));
    }

    #[test]
    fn test_to_value() {
        let mut map = Map::new();
        map.insert(int_val(1), string_val("one"));
        map.insert(bool_val(true), int_val(42));

        let value = map.to_value();

        if let Map(pairs) = value.0 {
            assert_eq!(pairs.len(), 2);

            // Check that the pairs contain our expected key-value pairs
            let mut found_int_key = false;
            let mut found_bool_key = false;

            for (k, v) in pairs {
                match k.0 {
                    Literal(Literal::Int64(1)) => {
                        found_int_key = true;
                        assert_values_equal(&v, &string_val("one"));
                    }
                    Literal(Literal::Bool(true)) => {
                        found_bool_key = true;
                        assert_values_equal(&v, &int_val(42));
                    }
                    _ => panic!("Unexpected key in map value"),
                }
            }

            assert!(found_int_key, "Integer key not found in map value");
            assert!(found_bool_key, "Boolean key not found in map value");
        } else {
            panic!("to_value() did not return a Map CoreData");
        }
    }
}
