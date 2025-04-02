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

use super::hir::ExprMetadata;
use super::hir::{
    CoreData, GroupId, Literal, LogicalOp, Materializable, Operator, PhysicalOp, Value,
};
use std::collections::HashMap;
use std::hash::Hash;

/// Map key representation of a logical operator
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct OperatorMapKey<T> {
    pub tag: String,
    pub data: Vec<T>,
    pub children: Vec<T>,
}

/// Map key representation of a logical operator
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogicalMapOpKey {
    pub operator: OperatorMapKey<MapKey>,
    pub group_id: Option<GroupId>,
}

/// Map key representation of a physical operator
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PhysicalMapOpKey {
    pub operator: OperatorMapKey<MapKey>,
    pub goal: Option<GoalMapKey>,
    pub cost: Option<MapKey>,
}

/// Map key representation of a goal
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct GoalMapKey {
    pub group_id: GroupId,
    pub properties: MapKey,
}

/// Map key representation of logical operators (materialized or unmaterialized)
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum LogicalMapKey {
    Materialized(LogicalMapOpKey),
    UnMaterialized(GroupId),
}

/// Map key representation of physical operators (materialized or unmaterialized)
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum PhysicalMapKey {
    Materialized(PhysicalMapOpKey),
    UnMaterialized(GoalMapKey),
}

/// Internal key type that implements Hash, Eq, PartialEq
/// These are the only types that can be used as keys in the Map
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
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
#[derive(Clone, Debug, Default)]
pub struct Map {
    inner: HashMap<MapKey, Value>,
}

impl Map {
    /// Creates a Map from a collection of key-value pairs
    pub fn from_pairs(pairs: Vec<(Value, Value)>) -> Self {
        pairs.into_iter().fold(Self::default(), |mut map, (k, v)| {
            let map_key = value_to_map_key(&k);
            map.inner.insert(map_key, v);
            map
        })
    }

    /// Gets a value by key, returning None (as a Value) if not found
    pub fn get<M: ExprMetadata>(&self, key: &Value<M>) -> Value {
        self.inner
            .get(&value_to_map_key(key))
            .unwrap_or(&Value::new(CoreData::None))
            .clone()
    }

    /// Combines two maps, with values from other overriding values from self when keys collide
    pub fn concat(&mut self, other: Map) {
        self.inner.extend(other.inner);
    }
}

// Key conversion functions

/// Converts a Value to a MapKey, enforcing valid key types
/// This performs runtime validation that the key type is supported
/// and will return an error for invalid key types
fn value_to_map_key<M: ExprMetadata>(value: &Value<M>) -> MapKey {
    match &value.data {
        CoreData::Literal(lit) => match lit {
            Literal::Int64(i) => MapKey::Int64(*i),
            Literal::String(s) => MapKey::String(s.clone()),
            Literal::Bool(b) => MapKey::Bool(*b),
            Literal::Unit => MapKey::Unit,
            Literal::Float64(_) => panic!("Invalid map key: Float64"),
        },
        CoreData::Tuple(items) => {
            let key_items = items.iter().map(value_to_map_key).collect();
            MapKey::Tuple(key_items)
        }
        CoreData::Struct(name, fields) => {
            let key_fields = fields.iter().map(value_to_map_key).collect();
            MapKey::Struct(name.clone(), key_fields)
        }
        CoreData::Logical(materializable) => match materializable {
            Materializable::UnMaterialized(group_id) => {
                MapKey::Logical(Box::new(LogicalMapKey::UnMaterialized(*group_id)))
            }
            Materializable::Materialized(logical_op) => {
                let map_op = value_to_logical_map_op(logical_op);
                MapKey::Logical(Box::new(LogicalMapKey::Materialized(map_op)))
            }
        },
        CoreData::Physical(materializable) => match materializable {
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
        CoreData::Fail(inner) => {
            let inner_key = value_to_map_key(inner);
            MapKey::Fail(Box::new(inner_key))
        }
        CoreData::None => MapKey::None,
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
fn value_to_logical_map_op<M: ExprMetadata>(logical_op: &LogicalOp<Value<M>>) -> LogicalMapOpKey {
    let operator =
        value_to_operator_map_key(&logical_op.operator, &(|v: &Value<M>| value_to_map_key(v)));

    LogicalMapOpKey {
        operator,
        group_id: logical_op.group_id,
    }
}

/// Converts a PhysicalOp to a map key
fn value_to_physical_map_op<M: ExprMetadata>(
    physical_op: &PhysicalOp<Value<M>, M>,
) -> PhysicalMapOpKey {
    let operator =
        value_to_operator_map_key(&physical_op.operator, &(|v: &Value<M>| value_to_map_key(v)));

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::tests::{
        assert_values_equal, create_logical_operator, create_physical_operator,
    };

    // Helper to create Value literals
    fn int_val(i: i64) -> Value {
        Value::new(CoreData::Literal(Literal::Int64(i)))
    }

    fn bool_val(b: bool) -> Value {
        Value::new(CoreData::Literal(Literal::Bool(b)))
    }

    fn string_val(s: &str) -> Value {
        Value::new(CoreData::Literal(Literal::String(s.to_string())))
    }

    fn float_val(f: f64) -> Value {
        Value::new(CoreData::Literal(Literal::Float64(f)))
    }

    fn unit_val() -> Value {
        Value::new(CoreData::Literal(Literal::Unit))
    }

    fn tuple_val(items: Vec<Value>) -> Value {
        Value::new(CoreData::Tuple(items))
    }

    fn struct_val(name: &str, fields: Vec<Value>) -> Value {
        Value::new(CoreData::Struct(name.to_string(), fields))
    }

    fn array_val(items: Vec<Value>) -> Value {
        Value::new(CoreData::Array(items))
    }

    fn none_val() -> Value {
        Value::new(CoreData::None)
    }

    fn fail_val(inner: Value) -> Value {
        Value::new(CoreData::Fail(Box::new(inner)))
    }

    #[test]
    fn test_simple_map_operations() {
        let mut map = Map::default();
        let map_key1 = value_to_map_key(&int_val(1));
        let map_key2 = value_to_map_key(&int_val(2));

        // Insert key-value pairs directly into inner HashMap
        map.inner.insert(map_key1, string_val("one"));
        map.inner.insert(map_key2, string_val("two"));

        // Check retrieval
        assert_values_equal(&map.get(&int_val(1)), &string_val("one"));
        assert_values_equal(&map.get(&int_val(2)), &string_val("two"));
        assert_values_equal(&map.get(&int_val(3)), &none_val()); // Non-existent key

        // Check map size
        assert_eq!(map.inner.len(), 2);
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
        assert_eq!(map.inner.len(), 2);
    }

    #[test]
    fn test_map_concat() {
        let mut map1 = Map::default();
        map1.inner
            .insert(value_to_map_key(&int_val(1)), string_val("one"));
        map1.inner
            .insert(value_to_map_key(&int_val(2)), string_val("two"));

        let mut map2 = Map::default();
        map2.inner
            .insert(value_to_map_key(&int_val(2)), string_val("TWO")); // Overwrite key 2
        map2.inner
            .insert(value_to_map_key(&int_val(3)), string_val("three"));

        map1.concat(map2);

        assert_values_equal(&map1.get(&int_val(1)), &string_val("one"));
        assert_values_equal(&map1.get(&int_val(2)), &string_val("TWO")); // Overwritten
        assert_values_equal(&map1.get(&int_val(3)), &string_val("three"));
        assert_eq!(map1.inner.len(), 3);
    }

    #[test]
    fn test_various_key_types() {
        let mut map = Map::default();

        // Basic literals
        map.inner
            .insert(value_to_map_key(&int_val(1)), string_val("int"));
        map.inner
            .insert(value_to_map_key(&bool_val(true)), string_val("bool"));
        map.inner
            .insert(value_to_map_key(&string_val("key")), string_val("string"));
        map.inner
            .insert(value_to_map_key(&unit_val()), string_val("unit"));

        // Compound types
        map.inner.insert(
            value_to_map_key(&tuple_val(vec![int_val(1), string_val("test")])),
            string_val("tuple"),
        );
        map.inner.insert(
            value_to_map_key(&struct_val("Person", vec![string_val("John"), int_val(30)])),
            string_val("struct"),
        );
        map.inner
            .insert(value_to_map_key(&none_val()), string_val("none"));
        map.inner.insert(
            value_to_map_key(&fail_val(string_val("error"))),
            string_val("fail"),
        );

        // Operator types
        let logical_op = create_logical_operator("filter", vec![int_val(1)], vec![]);
        let physical_op = create_physical_operator("scan", vec![string_val("table")], vec![]);
        map.inner
            .insert(value_to_map_key(&logical_op), string_val("logical"));
        map.inner
            .insert(value_to_map_key(&physical_op), string_val("physical"));

        // Verify all keys work
        assert_values_equal(&map.get(&int_val(1)), &string_val("int"));
        assert_values_equal(&map.get(&bool_val(true)), &string_val("bool"));
        assert_values_equal(&map.get(&string_val("key")), &string_val("string"));
        assert_values_equal(&map.get(&unit_val()), &string_val("unit"));
        assert_values_equal(
            &map.get(&tuple_val(vec![int_val(1), string_val("test")])),
            &string_val("tuple"),
        );
        assert_values_equal(
            &map.get(&struct_val("Person", vec![string_val("John"), int_val(30)])),
            &string_val("struct"),
        );
        assert_values_equal(&map.get(&none_val()), &string_val("none"));
        assert_values_equal(
            &map.get(&fail_val(string_val("error"))),
            &string_val("fail"),
        );
        assert_values_equal(&map.get(&logical_op), &string_val("logical"));
        assert_values_equal(&map.get(&physical_op), &string_val("physical"));

        assert_eq!(map.inner.len(), 10);
    }

    #[test]
    #[should_panic(expected = "Invalid map key: Float64")]
    fn test_float_key_panics() {
        value_to_map_key(&float_val(3.14));
    }

    #[test]
    #[should_panic(expected = "Invalid map key: Float64")]
    fn test_tuple_with_float_panics() {
        let tuple_with_float = tuple_val(vec![int_val(1), float_val(2.5)]);
        value_to_map_key(&tuple_with_float);
    }

    #[test]
    #[should_panic]
    fn test_array_key_panics() {
        let array_key = array_val(vec![int_val(1), int_val(2)]);
        value_to_map_key(&array_key);
    }

    #[test]
    fn test_empty_map() {
        let map = Map::default();
        assert_eq!(map.inner.len(), 0);
        assert!(map.inner.is_empty());
    }

    #[test]
    fn test_map_key_conversion() {
        // Test conversion between Value and MapKey
        let values = vec![
            int_val(42),
            string_val("hello"),
            bool_val(true),
            unit_val(),
            tuple_val(vec![int_val(1), bool_val(false)]),
            struct_val("Test", vec![string_val("field"), int_val(123)]),
            none_val(),
            fail_val(int_val(404)),
        ];

        for value in values {
            // Convert Value to MapKey
            let map_key = value_to_map_key(&value);

            // Use the map_key in a map
            let mut map = Map::default();
            map.inner.insert(map_key, string_val("value"));

            // Verify we can retrieve with the original value
            assert_values_equal(&map.get(&value), &string_val("value"));
        }
    }

    #[test]
    fn test_deep_nesting() {
        // Create deeply nested structure to test recursive key conversion
        let nested_value = struct_val(
            "Root",
            vec![
                tuple_val(vec![
                    int_val(1),
                    struct_val(
                        "Inner",
                        vec![
                            string_val("deep"),
                            tuple_val(vec![bool_val(true), unit_val()]),
                        ],
                    ),
                ]),
                fail_val(none_val()),
            ],
        );

        // Should not panic
        let key = value_to_map_key(&nested_value);

        // Use it as a key
        let mut map = Map::default();
        map.inner.insert(key, string_val("complex"));

        // Verify we can retrieve it
        assert_values_equal(&map.get(&nested_value), &string_val("complex"));
    }

    #[test]
    fn test_operator_key_conversion() {
        // Test LogicalOp conversion
        let logical_op = create_logical_operator(
            "join",
            vec![string_val("id"), int_val(10)],
            vec![int_val(1), int_val(2)],
        );

        let logical_key = value_to_map_key(&logical_op);
        assert!(matches!(logical_key, MapKey::Logical(_)));

        // Test PhysicalOp conversion
        let physical_op = create_physical_operator(
            "hash_join",
            vec![string_val("id")],
            vec![int_val(1), int_val(2)],
        );

        let physical_key = value_to_map_key(&physical_op);
        assert!(matches!(physical_key, MapKey::Physical(_)));
    }

    #[test]
    fn test_key_equality() {
        // Test that equal values create equal map keys
        let key1 = value_to_map_key(&tuple_val(vec![int_val(1), string_val("test")]));
        let key2 = value_to_map_key(&tuple_val(vec![int_val(1), string_val("test")]));

        assert_eq!(key1, key2);

        // Test that different values create different map keys
        let key3 = value_to_map_key(&tuple_val(vec![int_val(2), string_val("test")]));
        assert_ne!(key1, key3);
    }
}
