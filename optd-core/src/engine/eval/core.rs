//! This module provides evaluation functions for core expression types, transforming
//! expressions into value streams that handle all possible evaluation paths.

use super::{
    operator::{evaluate_logical_operator, evaluate_physical_operator},
    Engine, Evaluate, Expander,
};
use crate::{
    capture,
    engine::utils::streams::{
        evaluate_all_combinations, propagate_success, stream_from_result, ValueStream,
    },
};
use futures::StreamExt;
use optd_dsl::analyzer::hir::{CoreData, Expr, Value};
use std::sync::Arc;
use CoreData::*;

/// Evaluates a core expression by generating all possible evaluation paths.
///
/// This function dispatches to specialized handlers based on the expression type,
/// generating a stream of all possible values the expression could evaluate to.
///
/// # Parameters
/// * `data` - The core expression data to evaluate
/// * `engine` - The evaluation engine
///
/// # Returns
/// A stream of all possible evaluation results
pub(super) fn evaluate_core_expr<E>(data: CoreData<Arc<Expr>>, engine: Engine<E>) -> ValueStream
where
    E: Expander,
{
    match data.clone() {
        Literal(lit) => propagate_success(Value(Literal(lit))),
        Array(items) => evaluate_collection(items, data, engine),
        Tuple(items) => evaluate_collection(items, data, engine),
        Struct(_, items) => evaluate_collection(items, data, engine),
        Map(items) => evaluate_map(items, engine),
        Function(fun_type) => propagate_success(Value(Function(fun_type))),
        Fail(msg) => evaluate_fail(*msg, engine),
        Logical(op) => evaluate_logical_operator(op, engine),
        Physical(op) => evaluate_physical_operator(op, engine),
        Null => propagate_success(Value(Null)),
    }
}

/// Evaluates a collection expression (Array, Tuple, or Struct).
fn evaluate_collection<E>(
    items: Vec<Arc<Expr>>,
    data_clone: CoreData<Arc<Expr>>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    evaluate_all_combinations(items.into_iter(), engine)
        .map(move |result| {
            result.map(|items| match &data_clone {
                Array(_) => Value(Array(items)),
                Tuple(_) => Value(Tuple(items)),
                Struct(name, _) => Value(Struct(name.clone(), items)),
                _ => unreachable!("Unexpected collection type"),
            })
        })
        .boxed()
}

/// Evaluates a map expression by generating all combinations of keys and values.
fn evaluate_map<E>(items: Vec<(Arc<Expr>, Arc<Expr>)>, engine: Engine<E>) -> ValueStream
where
    E: Expander,
{
    // Extract keys and values
    let keys: Vec<_> = items.iter().map(|(k, _)| k.clone()).collect();
    let values: Vec<_> = items.iter().map(|(_, v)| v.clone()).collect();

    // First evaluate all key expressions
    evaluate_all_combinations(keys.into_iter(), engine.clone())
        .flat_map(move |keys_result| {
            // Process keys result
            stream_from_result(
                keys_result,
                capture!([values, engine], move |keys| {
                    // Then evaluate all value expressions
                    evaluate_all_combinations(values.into_iter(), engine)
                        .map(capture!([keys], move |values_result| {
                            // Create map from keys and values
                            values_result.map(|values| {
                                Value(Map(keys.iter().cloned().zip(values).collect()))
                            })
                        }))
                        .boxed()
                }),
            )
        })
        .boxed()
}

/// Evaluates a fail expression.
fn evaluate_fail<E>(msg: Arc<Expr>, engine: Engine<E>) -> ValueStream
where
    E: Expander,
{
    msg.evaluate(engine)
        .map(|result| result.map(|value| Value(Fail(Box::new(value)))))
        .boxed()
}

#[cfg(test)]
mod tests {
    use crate::engine::{eval::core::evaluate_core_expr, utils::tests::*, Engine};
    use optd_dsl::analyzer::{
        context::Context,
        hir::{
            BinOp, CoreData, Expr, FunKind, Goal, GroupId, Literal, LogicalOp, Materializable,
            Operator, PhysicalOp, Value,
        },
    };
    use std::sync::Arc;
    use CoreData::*;
    use Expr::*;
    use Literal::*;
    use Materializable::*;

    #[test]
    fn test_evaluate_literal() {
        let engine = create_test_engine();

        // Test integer literal
        let expr = CoreData::Literal(Int64(42));
        let stream = evaluate_core_expr(expr, engine.clone());
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(42))));

        // Test string literal
        let expr = CoreData::Literal(String("hello".to_string()));
        let stream = evaluate_core_expr(expr, engine.clone());
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(String(s)) if s == "hello"));

        // Test boolean literal
        let expr = CoreData::Literal(Bool(true));
        let stream = evaluate_core_expr(expr, engine.clone());
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Bool(true))));

        // Test unit literal
        let expr = CoreData::Literal(Unit);
        let stream = evaluate_core_expr(expr, engine);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Unit)));
    }

    #[test]
    fn test_evaluate_array() {
        let engine = create_test_engine();

        // Test array of literals
        let array_expr = Array(vec![
            arc(CoreVal(int_val(1))),
            arc(CoreVal(int_val(2))),
            arc(CoreVal(int_val(3))),
        ]);

        let stream = evaluate_core_expr(array_expr, engine);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        if let Array(items) = &values[0].0 {
            assert_eq!(items.len(), 3);
            assert!(matches!(&items[0].0, Literal(Int64(1))));
            assert!(matches!(&items[1].0, Literal(Int64(2))));
            assert!(matches!(&items[2].0, Literal(Int64(3))));
        } else {
            panic!("Expected Array, got {:?}", values[0]);
        }
    }

    #[test]
    fn test_evaluate_tuple() {
        let engine = create_test_engine();

        // Test tuple with mixed types
        let tuple_expr = Tuple(vec![
            arc(CoreVal(int_val(42))),
            arc(CoreVal(string_val("hello"))),
            arc(CoreVal(bool_val(true))),
        ]);

        let stream = evaluate_core_expr(tuple_expr, engine);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        if let Tuple(items) = &values[0].0 {
            assert_eq!(items.len(), 3);
            assert!(matches!(&items[0].0, Literal(Int64(42))));
            assert!(matches!(&items[1].0, Literal(String(s)) if s == "hello"));
            assert!(matches!(&items[2].0, Literal(Bool(true))));
        } else {
            panic!("Expected Tuple, got {:?}", values[0]);
        }
    }

    #[test]
    fn test_evaluate_struct() {
        let engine = create_test_engine();

        // Test struct with name and fields
        let struct_expr = Struct(
            "Person".to_string(),
            vec![arc(CoreVal(string_val("Alice"))), arc(CoreVal(int_val(30)))],
        );

        let stream = evaluate_core_expr(struct_expr, engine);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        if let Struct(name, fields) = &values[0].0 {
            assert_eq!(name, "Person");
            assert_eq!(fields.len(), 2);
            assert!(matches!(&fields[0].0, Literal(String(s)) if s == "Alice"));
            assert!(matches!(&fields[1].0, Literal(Int64(30))));
        } else {
            panic!("Expected Struct, got {:?}", values[0]);
        }
    }

    #[test]
    fn test_evaluate_map() {
        let engine = create_test_engine();

        // Test map with string keys and mixed value types
        let map_expr = Map(vec![
            (
                arc(CoreVal(string_val("name"))),
                arc(CoreVal(string_val("Bob"))),
            ),
            (arc(CoreVal(string_val("age"))), arc(CoreVal(int_val(25)))),
        ]);

        let stream = evaluate_core_expr(map_expr, engine);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        if let Map(entries) = &values[0].0 {
            assert_eq!(entries.len(), 2);

            // Find the name entry
            let name_entry = entries
                .iter()
                .find(|(k, _)| matches!(&k.0, Literal(String(key)) if key == "name"));
            assert!(name_entry.is_some());
            let (_, v) = name_entry.unwrap();
            assert!(matches!(&v.0, Literal(String(val)) if val == "Bob"));

            // Find the age entry
            let age_entry = entries
                .iter()
                .find(|(k, _)| matches!(&k.0, Literal(String(key)) if key == "age"));
            assert!(age_entry.is_some());
            let (_, v) = age_entry.unwrap();
            assert!(matches!(&v.0, Literal(Int64(25))));
        } else {
            panic!("Expected Map, got {:?}", values[0]);
        }
    }

    #[test]
    fn test_evaluate_function() {
        let engine = create_test_engine();

        // Test closure function
        let closure = FunKind::Closure(vec!["x".to_string()], arc(CoreVal(int_val(42))));

        let func_expr = Function(closure);
        let stream = evaluate_core_expr(func_expr, engine);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        if let Function(FunKind::Closure(params, _)) = &values[0].0 {
            assert_eq!(params.len(), 1);
            assert_eq!(params[0], "x");
        } else {
            panic!("Expected Function with Closure, got {:?}", values[0]);
        }
    }

    #[test]
    fn test_evaluate_fail() {
        let engine = create_test_engine();

        // Test fail expression with string message
        let fail_expr =
            Fail(Arc::new(CoreExpr(Literal(String("Error occurred".to_string())))).into());
        let stream = evaluate_core_expr(fail_expr, engine);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        if let Fail(boxed_value) = &values[0].0 {
            assert!(matches!(&boxed_value.0, Literal(String(msg)) if msg == "Error occurred"));
        } else {
            panic!("Expected Fail, got {:?}", values[0]);
        }
    }

    #[test]
    fn test_evaluate_null() {
        let engine = create_test_engine();

        // Test null value
        let null_expr = CoreData::Null;
        let stream = evaluate_core_expr(null_expr, engine);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Null));
    }

    #[test]
    fn test_evaluate_complex_expressions() {
        let engine = create_test_engine();

        // Test a more complex expression: [1, 2 + 3, "hello"]
        // This would typically be created by evaluating expressions,
        // but for testing we'll directly create it
        let array_expr = CoreData::Array(vec![
            arc(CoreVal(int_val(1))),
            arc(Binary(
                arc(CoreVal(int_val(2))),
                BinOp::Add,
                arc(CoreVal(int_val(3))),
            )),
            arc(CoreVal(string_val("hello"))),
        ]);

        let stream = evaluate_core_expr(array_expr, engine);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        if let Array(items) = &values[0].0 {
            assert_eq!(items.len(), 3);
            assert!(matches!(&items[0].0, Literal(Int64(1))));
            // The second item is a computed expression (2 + 3 = 5)
            // which should have been evaluated
            assert!(matches!(&items[2].0, Literal(String(s)) if s == "hello"));
        } else {
            panic!("Expected Array, got {:?}", values[0]);
        }
    }

    #[test]
    fn test_evaluate_logical_group() {
        // Create a MockExpander that provides expansions for logical groups
        let expander = MockExpander::new(
            |group_id| {
                if group_id == GroupId(2) {
                    // Create two logical operators
                    let op1 = Operator {
                        tag: "Filter".to_string(),
                        data: vec![bool_val(true)],
                        children: vec![],
                    };
                    let op2 = Operator {
                        tag: "Project".to_string(),
                        data: vec![int_val(42)],
                        children: vec![],
                    };

                    vec![
                        Value(Logical(LogicalOp(Materialized(op1)))),
                        Value(Logical(LogicalOp(Materialized(op2)))),
                    ]
                } else {
                    vec![]
                }
            },
            |_| panic!("Physical expansion not expected"),
        );

        let engine = Engine::new(Context::default(), expander);

        // Create a logical group reference
        let logical_expr = Logical(LogicalOp(UnMaterialized(GroupId(2))));

        // Evaluate the logical group
        let stream = evaluate_core_expr(logical_expr, engine);
        let values = collect_stream_values(stream);

        // The group reference should be returned as-is (no expansion happens here)
        assert_eq!(values.len(), 1);
        assert!(matches!(
            &values[0].0,
            Logical(LogicalOp(UnMaterialized(group_id))) if *group_id == GroupId(2)
        ));
    }

    #[test]
    fn test_evaluate_physical_goal() {
        // Create a goal for testing
        let goal = Goal {
            group_id: GroupId(4),
            properties: Box::new(unit_val()),
        };

        // Create a MockExpander that provides implementation for physical goals
        let expander = MockExpander::new(
            |_| vec![],
            |physical_goal| {
                if physical_goal.group_id == GroupId(4) {
                    // Create a physical operator
                    let op = Operator {
                        tag: "HashJoin".to_string(),
                        data: vec![int_val(1)],
                        children: vec![],
                    };

                    Value(Physical(PhysicalOp(Materialized(op))))
                } else {
                    panic!("Unexpected physical goal")
                }
            },
        );

        let engine = Engine::new(Context::default(), expander);

        // Create a physical goal reference
        let physical_expr = Physical(PhysicalOp(Materializable::UnMaterialized(goal)));

        // Evaluate the physical goal
        let stream = evaluate_core_expr(physical_expr, engine);
        let values = collect_stream_values(stream);

        // The goal reference should be returned as-is (no expansion happens here)
        assert_eq!(values.len(), 1);
        assert!(matches!(
            &values[0].0,
            Physical(PhysicalOp(UnMaterialized(ref g))) if g.group_id == GroupId(4)
        ));
    }

    #[test]
    fn test_evaluate_nested_combinatorial() {
        // Create a MockExpander that provides multiple expansions for different groups
        let expander = MockExpander::new(
            |group_id| match group_id {
                GroupId(1) => vec![int_val(1), int_val(2)],
                _ => vec![],
            },
            |_| panic!("Physical expansion not expected"),
        );

        let engine = Engine::new(Context::default(), expander);

        // Create nested tuples with group references:
        // (
        //   [<logical_group1>],
        // )

        let logical_group_ref = Value(Logical(LogicalOp(UnMaterialized(GroupId(1)))));

        let tuple_expr = Tuple(vec![arc(CoreExpr(Array(vec![arc(CoreVal(
            logical_group_ref,
        ))])))]);

        // Evaluate the nested structure
        let stream = evaluate_core_expr(tuple_expr, engine);
        let values = collect_stream_values(stream);

        // Should produce one result
        assert_eq!(values.len(), 1);

        // Check the structure of the result
        if let Tuple(items) = &values[0].0 {
            assert_eq!(items.len(), 1);

            // First item should be an array containing a logical group reference
            if let Array(array1) = &items[0].0 {
                assert_eq!(array1.len(), 1);
                assert!(matches!(
                    &array1[0].0,
                    Logical(LogicalOp(UnMaterialized(group_id))) if *group_id == GroupId(1)
                ));
            } else {
                panic!("Expected Array, got {:?}", items[0]);
            }
        } else {
            panic!("Expected Tuple, got {:?}", values[0]);
        }
    }
}
