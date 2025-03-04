//! This module provides evaluation functions for core expression types, transforming
//! expressions into value streams that handle all possible evaluation paths.

use crate::{
    analyzer::hir::{CoreData, Expr, FunKind, Literal, Materializable, OperatorKind, Value},
    capture,
    engine::{
        utils::streams::{
            evaluate_all_combinations, propagate_success, stream_from_result, ValueStream,
        },
        Context,
    },
};
use futures::StreamExt;

use CoreData::*;
use Materializable::*;

use super::operator::evaluate_operator;

/// Evaluates a core expression by generating all possible evaluation paths.
///
/// This function dispatches to specialized handlers based on the expression type,
/// generating a stream of all possible values the expression could evaluate to.
///
/// # Parameters
/// * `data` - The core expression data to evaluate
/// * `context` - The evaluation context
///
/// # Returns
/// A stream of all possible evaluation results
pub(super) fn evaluate_core_expr(data: CoreData<Expr>, context: Context) -> ValueStream {
    match data.clone() {
        Literal(lit) => evaluate_literal(lit),
        Array(items) => evaluate_collection(items, data, context),
        Tuple(items) => evaluate_collection(items, data, context),
        Struct(_, items) => evaluate_collection(items, data, context),
        Map(items) => evaluate_map(items, context),
        Function(fun_type) => evaluate_function(fun_type),
        Fail(msg) => evaluate_fail(*msg, context),
        Operator(Data(op)) => evaluate_operator(op, context),
        Operator(Group(id, kind)) => evaluate_group(id, kind),
    }
}

/// Evaluates a literal value.
fn evaluate_literal(lit: Literal) -> ValueStream {
    propagate_success(Value(CoreData::Literal(lit.clone())))
}

/// Evaluates a collection expression (Array, Tuple, or Struct).
fn evaluate_collection(
    items: Vec<Expr>,
    data_clone: CoreData<Expr>,
    context: Context,
) -> ValueStream {
    evaluate_all_combinations(items.into_iter(), context)
        .map(move |result| {
            result.map(|items| match &data_clone {
                Array(_) => Value(Array(items)),
                Tuple(_) => Value(Tuple(items)),
                Struct(name, _) => Value(Struct(name.clone(), items)),
                _ => unreachable!(),
            })
        })
        .boxed()
}

/// Evaluates a map expression by generating all combinations of keys and values.
fn evaluate_map(items: Vec<(Expr, Expr)>, context: Context) -> ValueStream {
    let keys: Vec<_> = items.iter().map(|(k, _)| k).cloned().collect();
    let values: Vec<_> = items.iter().map(|(_, v)| v).cloned().collect();

    // First evaluate all key expressions
    evaluate_all_combinations(keys.into_iter(), context.clone())
        .flat_map(move |keys_result| {
            // Process keys result
            stream_from_result(
                keys_result,
                capture!([values, context], move |keys| {
                    // Then evaluate all value expressions
                    evaluate_all_combinations(values.into_iter(), context)
                        .map(capture!([keys], move |values_result| {
                            // Create map from keys and values
                            values_result.map(|values| {
                                Value(CoreData::Map(keys.iter().cloned().zip(values).collect()))
                            })
                        }))
                        .boxed()
                }),
            )
        })
        .boxed()
}

/// Evaluates a function expression.
fn evaluate_function(fun_type: FunKind) -> ValueStream {
    propagate_success(Value(CoreData::Function(fun_type)))
}

/// Evaluates a fail expression.
fn evaluate_fail(msg: Expr, context: Context) -> ValueStream {
    msg.evaluate(context)
        .map(|result| result.map(|value| Value(CoreData::Fail(Box::new(value)))))
        .boxed()
}

/// Evaluates a group expression.
fn evaluate_group(id: i64, kind: OperatorKind) -> ValueStream {
    propagate_success(Value(CoreData::Operator(Materializable::Group(id, kind))))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{
        analyzer::hir::{
            BinOp, CoreData, Expr, FunKind, Literal, Materializable, OperatorKind, Value,
        },
        engine::{utils::streams::ValueStream, Context},
    };
    use futures::executor::block_on_stream;
    use BinOp::*;
    use Expr::*;
    use Literal::*;

    // Helper functions to create values
    fn int_val(i: i64) -> Value {
        Value(Literal(Int64(i)))
    }

    fn string_val(s: &str) -> Value {
        Value(Literal(String(s.to_string())))
    }

    fn bool_val(b: bool) -> Value {
        Value(Literal(Bool(b)))
    }

    // Helper to collect all successful values from a stream
    fn collect_stream_values(stream: ValueStream) -> Vec<Value> {
        block_on_stream(stream).filter_map(Result::ok).collect()
    }

    #[test]
    fn test_evaluate_literal() {
        // Test integer literal
        let int_stream = evaluate_literal(Int64(42));
        let values = collect_stream_values(int_stream);
        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(42))));

        // Test string literal
        let string_stream = evaluate_literal(String("hello".to_string()));
        let values = collect_stream_values(string_stream);
        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(String(s)) if s == "hello"));

        // Test boolean literal
        let bool_stream = evaluate_literal(Bool(true));
        let values = collect_stream_values(bool_stream);
        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Bool(true))));

        // Test unit literal
        let unit_stream = evaluate_literal(Unit);
        let values = collect_stream_values(unit_stream);
        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Unit)));
    }

    #[test]
    fn test_evaluate_array() {
        // Create context
        let context = Context::new(HashMap::new());

        // Test array of literals
        let array_expr = CoreData::Array(vec![
            CoreVal(int_val(1)),
            CoreVal(int_val(2)),
            CoreVal(int_val(3)),
        ]);

        let array_stream = evaluate_core_expr(array_expr, context);
        let values = collect_stream_values(array_stream);

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
        // Create context
        let context = Context::new(HashMap::new());

        // Test tuple with mixed types
        let tuple_expr = CoreData::Tuple(vec![
            CoreVal(int_val(42)),
            CoreVal(string_val("hello")),
            CoreVal(bool_val(true)),
        ]);

        let tuple_stream = evaluate_core_expr(tuple_expr, context);
        let values = collect_stream_values(tuple_stream);

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
        // Create context
        let context = Context::new(HashMap::new());

        // Test struct evaluation
        let struct_expr = CoreData::Struct(
            "Person".to_string(),
            vec![CoreVal(string_val("Alice")), CoreVal(int_val(30))],
        );

        let struct_stream = evaluate_core_expr(struct_expr, context);
        let values = collect_stream_values(struct_stream);

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
        // Create context
        let context = Context::new(HashMap::new());

        // Test map evaluation
        let map_expr = CoreData::Map(vec![
            (CoreVal(string_val("name")), CoreVal(string_val("Bob"))),
            (CoreVal(string_val("age")), CoreVal(int_val(25))),
        ]);

        let map_stream = evaluate_core_expr(map_expr, context);
        let values = collect_stream_values(map_stream);

        assert_eq!(values.len(), 1);
        if let Map(entries) = &values[0].0 {
            assert_eq!(entries.len(), 2);

            // Convert to a more easily testable form
            let map: HashMap<std::string::String, _> = entries
                .iter()
                .map(|(k, v)| {
                    if let Literal(String(key)) = &k.0 {
                        (key.clone(), v.clone())
                    } else {
                        panic!("Expected String key");
                    }
                })
                .collect();

            assert_eq!(map.len(), 2);
            assert!(matches!(&map.get("name").unwrap().0, Literal(String(s)) if s == "Bob"));
            assert!(matches!(&map.get("age").unwrap().0, Literal(Int64(25))));
        } else {
            panic!("Expected Map, got {:?}", values[0]);
        }
    }

    #[test]
    fn test_evaluate_function() {
        // Test closure function
        let closure = FunKind::Closure(vec!["x".to_string()], Box::new(CoreVal(int_val(42))));

        let closure_stream = evaluate_function(closure.clone());
        let values = collect_stream_values(closure_stream);

        assert_eq!(values.len(), 1);
        if let Function(func) = &values[0].0 {
            match func {
                FunKind::Closure(params, _) => {
                    assert_eq!(params.len(), 1);
                    assert_eq!(params[0], "x");
                }
                _ => panic!("Expected Closure"),
            }
        } else {
            panic!("Expected Function, got {:?}", values[0]);
        }

        // Test RustUDF function
        fn test_udf(_args: Vec<Value>) -> Value {
            int_val(99)
        }

        let rust_udf = FunKind::RustUDF(test_udf as fn(Vec<Value>) -> Value);
        let udf_stream = evaluate_function(rust_udf);
        let values = collect_stream_values(udf_stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Function(FunKind::RustUDF(_))));
    }

    #[test]
    fn test_evaluate_fail() {
        // Create context
        let context = Context::new(HashMap::new());

        // Test fail expression with string message
        let fail_expr = CoreVal(string_val("Error occurred"));
        let fail_stream = evaluate_fail(fail_expr, context);
        let values = collect_stream_values(fail_stream);

        assert_eq!(values.len(), 1);
        if let Fail(boxed_value) = &values[0].0 {
            assert!(matches!(&boxed_value.0, Literal(String(s)) if s == "Error occurred"));
        } else {
            panic!("Expected Fail, got {:?}", values[0]);
        }
    }

    #[test]
    fn test_evaluate_group() {
        // Test operator group
        let group_id = 123;
        let kind = OperatorKind::Logical;

        let group_stream = evaluate_group(group_id, kind);
        let values = collect_stream_values(group_stream);

        assert_eq!(values.len(), 1);
        if let Operator(Materializable::Group(id, op_kind)) = &values[0].0 {
            assert_eq!(*id, group_id);
            assert_eq!(*op_kind, kind);
        } else {
            panic!("Expected Operator::Group, got {:?}", values[0]);
        }
    }

    #[test]
    fn test_evaluate_complex_expression() {
        // Create context
        let context = Context::new(HashMap::new());

        // Create a complex expression: [1, 2 + 3, "hello"]
        let array_expr = CoreData::Array(vec![
            CoreVal(int_val(1)),
            Binary(
                Box::new(CoreVal(int_val(2))),
                Add,
                Box::new(CoreVal(int_val(3))),
            ),
            CoreVal(string_val("hello")),
        ]);

        let array_stream = evaluate_core_expr(array_expr, context);
        let values = collect_stream_values(array_stream);

        assert_eq!(values.len(), 1);
        if let Array(items) = &values[0].0 {
            assert_eq!(items.len(), 3);
            assert!(matches!(&items[0].0, Literal(Int64(1))));
            assert!(matches!(&items[1].0, Literal(Int64(5)))); // 2 + 3 = 5
            assert!(matches!(&items[2].0, Literal(String(s)) if s == "hello"));
        } else {
            panic!("Expected Array, got {:?}", values[0]);
        }
    }
}
