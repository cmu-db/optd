//! This module provides pattern matching functionality for expressions.
//!
//! Pattern matching is a powerful feature that allows for destructuring and examining
//! values against patterns. This implementation supports matching against literals,
//! wildcards, bindings, structs, arrays, and operators, with special handling for operators.

use super::{Engine, Evaluate, Generator};
use crate::engine::utils::streams::{
    process_items_in_sequence, MatchResultStream, VecMatchResultStream,
};
use crate::error::Error;
use crate::{
    capture,
    engine::utils::streams::{propagate_success, stream_from_result, ValueStream},
};
use futures::{Stream, StreamExt};
use optd_dsl::analyzer::{
    context::Context,
    hir::{CoreData, LogicalOp, MatchArm, Materializable, Operator, Pattern, PhysicalOp, Value},
};
use Materializable::*;
use Pattern::*;

/// Type representing a match result: a value and an optional context
/// None means the match failed, Some(context) means it succeeded
pub(crate) type MatchResult = (Value, Option<Context>);

/// Tries to match a value against a sequence of match arms.
///
/// Attempts to match the value against each arm's pattern in order.
/// Returns a stream of results from evaluating matching arms' expressions.
pub(super) fn try_match_arms<E>(
    value: Value,
    match_arms: Vec<MatchArm>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Generator,
{
    if match_arms.is_empty() {
        // No arms matched - this is a runtime error
        panic!("Pattern match exhausted: no patterns provided");
    }

    // Start by attempting to match against the first arm
    let first_arm = match_arms[0].clone();
    let remaining_arms = match_arms[1..].to_vec();

    // Match the value against the arm's pattern
    match_pattern(value, first_arm.pattern.clone(), engine.clone())
        .flat_map(move |match_result| {
            stream_from_result(
                match_result,
                capture!([engine, first_arm, remaining_arms], move |(
                    value,
                    context_opt,
                )| {
                    match context_opt {
                        // If we got a match, evaluate the arm's expression with the context
                        Some(context) => {
                            let engine_with_ctx = engine.with_context(context);
                            first_arm.expr.evaluate(engine_with_ctx)
                        }
                        // If no match, try the next arm with this value
                        None => try_match_arms(value, remaining_arms, engine),
                    }
                }),
            )
        })
        .boxed()
}

/// Attempts to match a value against a pattern.
///
/// Returns a stream of Result<(Value, Option<Context>), Error> pairs:
/// - For successful matches: Ok((Value, Some(Context)))
/// - For failed matches: Ok((Value, None))
/// - For errors: Err(Error)
fn match_pattern<E>(value: Value, pattern: Pattern, engine: Engine<E>) -> MatchResultStream
where
    E: Generator,
{
    match (pattern, &value.0) {
        // Wildcard pattern matches anything
        (Wildcard, _) => propagate_success((value, Some(engine.context))),

        // Binding pattern: bind the value to the identifier and continue matching
        (Bind(ident, inner_pattern), _) => {
            // First check if the inner pattern matches without binding
            match_pattern(value, *inner_pattern, engine)
                .map(move |result| {
                    result.map(|(matched_value, ctx_opt)| {
                        // Only bind if the inner pattern matched
                        if let Some(ctx) = ctx_opt {
                            // Create a new context with the binding
                            let mut new_ctx = ctx.clone();
                            new_ctx.bind(ident.clone(), matched_value.clone());
                            (matched_value, Some(new_ctx))
                        } else {
                            // Inner pattern didn't match, propagate failure
                            (matched_value, None)
                        }
                    })
                })
                .boxed()
        }

        // Literal pattern: match if literals are equal
        (Literal(pattern_lit), CoreData::Literal(value_lit)) => {
            let context_opt = (pattern_lit == *value_lit).then_some(engine.context);
            propagate_success((value, context_opt))
        }

        // Empty array pattern
        (EmptyArray, CoreData::Array(arr)) if arr.is_empty() => {
            propagate_success((value, Some(engine.context)))
        }

        // List decomposition pattern: match first element and rest of the array
        (ArrayDecomp(head_pattern, tail_pattern), CoreData::Array(arr)) => {
            if arr.is_empty() {
                return propagate_success((value, None));
            }

            match_array_pattern(*head_pattern, *tail_pattern, arr, engine)
        }

        // Struct pattern: match name and recursively match fields
        (Struct(pat_name, field_patterns), CoreData::Struct(val_name, field_values)) => {
            if pat_name != *val_name || field_patterns.len() != field_values.len() {
                return propagate_success((value, None));
            }

            match_struct_pattern(pat_name, &field_patterns, field_values, engine)
        }

        // Materialized operators
        (Operator(op_pattern), CoreData::Logical(LogicalOp(Materialized(operator)))) => {
            match_operator_pattern(value.clone(), &op_pattern, operator, engine)
        }
        (Operator(op_pattern), CoreData::Physical(PhysicalOp(Materialized(operator)))) => {
            match_operator_pattern(value.clone(), &op_pattern, operator, engine)
        }

        // Unmaterialized operators
        (Operator(op_pattern), CoreData::Logical(LogicalOp(UnMaterialized(group_id)))) => {
            let expanded_values = engine.expander.expand_all_exprs(*group_id);
            match_against_expanded_values(op_pattern, expanded_values, engine)
        }
        (Operator(op_pattern), CoreData::Physical(PhysicalOp(UnMaterialized(physical_goal)))) => {
            let expanded_values = engine.expander.expand_optimized_expr(physical_goal);
            match_against_expanded_values(op_pattern, expanded_values, engine)
        }

        // No match for other combinations
        _ => propagate_success((value, None)),
    }
}

/// Matches patterns against values and collects individual match results.
///
/// This is a specialized version of `process_items_in_sequence` for pattern matching.
/// It processes value-pattern pairs one at a time and combines their results.
fn match_pattern_combinations<E, I>(pairs: I, engine: Engine<E>) -> VecMatchResultStream
where
    E: Generator,
    I: Iterator<Item = (Value, Pattern)> + Send + Clone + 'static,
{
    // Use the generic process_items_in_sequence with a matcher function
    process_items_in_sequence(pairs, engine, |pair, eng| {
        match_pattern(pair.0, pair.1, eng)
    })
}

/// Helper function to match array decomposition patterns
fn match_array_pattern<E>(
    head_pattern: Pattern,
    tail_pattern: Pattern,
    arr: &[Value],
    engine: Engine<E>,
) -> MatchResultStream
where
    E: Generator,
{
    if arr.is_empty() {
        return propagate_success((Value(CoreData::Array(Vec::new())), None));
    }

    // Split array into head and tail
    let head = arr[0].clone();
    let tail_elements = arr[1..].to_vec();
    let tail = Value(CoreData::Array(tail_elements));

    // Create value-pattern pairs
    let pairs = vec![(head, head_pattern), (tail, tail_pattern)];

    // Use match_pattern_combinations to process all pairs
    match_pattern_combinations(pairs.into_iter(), engine.clone())
        .map(move |result| {
            result.map(|pair_results| {
                // Destructure and take ownership of the results
                let [(head_value, head_ctx_opt), (tail_value, tail_ctx_opt)] =
                    pair_results.try_into().unwrap_or_else(|_| {
                        panic!("Expected exactly 2 results from array decomposition")
                    });

                // Extract tail elements and take ownership
                let tail_elements = match tail_value.0 {
                    CoreData::Array(elements) => elements,
                    _ => panic!("Expected Array in tail result"),
                };

                // Create new array with head + tail elements
                let new_array = Value(CoreData::Array(
                    std::iter::once(head_value).chain(tail_elements).collect(),
                ));

                // Both patterns need to match for the overall match to succeed
                let combined_ctx_opt = match (head_ctx_opt, tail_ctx_opt) {
                    (Some(head_ctx), Some(mut tail_ctx)) => {
                        tail_ctx.merge(head_ctx);
                        Some(tail_ctx)
                    }
                    _ => None,
                };

                (new_array, combined_ctx_opt)
            })
        })
        .boxed()
}

/// Helper function to match struct patterns
fn match_struct_pattern<E>(
    struct_name: String,
    field_patterns: &[Pattern],
    field_values: &[Value],
    engine: Engine<E>,
) -> MatchResultStream
where
    E: Generator,
{
    // Create pairs of field values and patterns
    let pairs: Vec<_> = field_values
        .iter()
        .cloned()
        .zip(field_patterns.iter().cloned())
        .collect();

    // Use match_pattern_combinations to process all pairs
    match_pattern_combinations(pairs.into_iter(), engine.clone())
        .map(move |result| {
            result.map(capture!([struct_name, engine], move |pair_results| {
                // Reconstruct struct with matched values regardless of match success
                let matched_values = pair_results.iter().map(|(v, _)| v.clone()).collect();
                let new_struct = Value(CoreData::Struct(struct_name, matched_values));

                // Check if all fields matched for context handling
                let all_matched = pair_results.iter().all(|(_, ctx)| ctx.is_some());

                if all_matched {
                    // Combine all contexts
                    let mut combined_ctx = engine.context;
                    for (_, ctx_opt) in pair_results {
                        if let Some(ctx) = ctx_opt {
                            combined_ctx.merge(ctx);
                        }
                    }

                    (new_struct, Some(combined_ctx))
                } else {
                    // Struct pattern match failed, but still return expanded values
                    (new_struct, None)
                }
            }))
        })
        .boxed()
}

/// Matches a pattern against a materialized operator.
///
/// This is the core operator matching function that matches a pattern against
/// a specific operator's components.
fn match_operator_pattern<E>(
    original_value: Value,
    op_pattern: &Operator<Pattern>,
    op: &Operator<Value>,
    engine: Engine<E>,
) -> MatchResultStream
where
    E: Generator,
{
    // Match tag and component counts
    if op_pattern.tag != op.tag
        || op_pattern.data.len() != op.data.len()
        || op_pattern.children.len() != op.children.len()
    {
        return propagate_success((original_value, None));
    }

    // Create a single vector of all value-pattern pairs
    let pairs: Vec<_> = op_pattern
        .data
        .iter()
        .cloned()
        .zip(op.data.iter().cloned())
        .map(|(pattern, value)| (value, pattern))
        .chain(
            op_pattern
                .children
                .iter()
                .cloned()
                .zip(op.children.iter().cloned())
                .map(|(pattern, value)| (value, pattern)),
        )
        .collect();

    // Tag to use when reconstructing the operator
    let op_tag = op.tag.clone();
    let data_len = op.data.len();

    // Use match_pattern_combinations to process all pairs
    match_pattern_combinations(pairs.into_iter(), engine.clone())
        .map(move |result| {
            result.map(capture!(
                [op_tag, engine, original_value],
                move |pair_results| {
                    // Reconstruct operator with matched values regardless of match success
                    let matched_values: Vec<_> =
                        pair_results.iter().map(|(v, _)| v.clone()).collect();

                    // Create new operator with matched components
                    let new_op = Operator {
                        tag: op_tag,
                        data: matched_values[..data_len].to_vec(),
                        children: matched_values[data_len..].to_vec(),
                    };

                    // Create appropriate value type based on original
                    let new_value = match &original_value.0 {
                        CoreData::Logical(_) => {
                            Value(CoreData::Logical(LogicalOp(Materialized(new_op))))
                        }
                        CoreData::Physical(_) => {
                            Value(CoreData::Physical(PhysicalOp(Materialized(new_op))))
                        }
                        _ => panic!("Expected Logical or Physical operator"),
                    };

                    // Check if all components matched for context handling
                    let all_matched = pair_results.iter().all(|(_, ctx)| ctx.is_some());

                    if all_matched {
                        let mut combined_ctx = engine.context;
                        for (_, ctx_opt) in pair_results {
                            if let Some(ctx) = ctx_opt {
                                combined_ctx.merge(ctx);
                            }
                        }

                        (new_value, Some(combined_ctx))
                    } else {
                        // Operator pattern match failed, but still return expanded values
                        (new_value, None)
                    }
                }
            ))
        })
        .boxed()
}

/// Helper function to match a pattern against a stream of expanded values
fn match_against_expanded_values<E, S>(
    op_pattern: Operator<Pattern>,
    expanded_values: S,
    engine: Engine<E>,
) -> MatchResultStream
where
    E: Generator,
    S: Stream<Item = Result<Value, Error>> + Send + 'static,
{
    expanded_values
        .flat_map(move |expanded_value_result| {
            stream_from_result(
                expanded_value_result,
                capture!([op_pattern, engine], move |expanded_value| {
                    match_pattern(expanded_value, Operator(op_pattern), engine)
                }),
            )
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use crate::engine::{
        eval::r#match::{match_pattern, match_pattern_combinations},
        utils::tests::{
            bool_val, collect_stream_values, create_test_engine, int_val, string_val, unit_val,
            MockExpander,
        },
        Engine,
    };
    use optd_dsl::analyzer::{
        context::Context,
        hir::{CoreData, GroupId, Literal, LogicalOp, Materializable, Operator, Pattern, Value},
    };
    use Literal::*;
    use Materializable::*;
    use Pattern::*;

    // Test pattern matching against a literal value
    #[test]
    fn test_match_literal() {
        let engine = create_test_engine();
        let value = int_val(42);
        let pattern = Pattern::Literal(Int64(42));

        let results = collect_stream_values(match_pattern(value, pattern, engine));

        assert_eq!(results.len(), 1);
        let (result_value, context) = &results[0];
        assert!(context.is_some());

        // Check value matches using our helper
        if let CoreData::Literal(Int64(i)) = &result_value.0 {
            assert_eq!(*i, 42);
        } else {
            panic!("Expected Int64(42), got {:?}", result_value);
        }
    }

    // Test pattern matching with wildcard
    #[test]
    fn test_match_wildcard() {
        let engine = create_test_engine();

        // Create various values to match against
        let values = vec![int_val(42), string_val("hello"), bool_val(true), unit_val()];

        // Create a wildcard pattern
        let pattern = Wildcard;

        // Match each value against the pattern
        for value in values {
            let results = collect_stream_values(match_pattern(
                value.clone(),
                pattern.clone(),
                engine.clone(),
            ));

            assert_eq!(results.len(), 1);
            let (result_value, context) = &results[0];
            assert!(context.is_some());

            // Check the result matches the input
            match &value.0 {
                CoreData::Literal(Int64(i)) => {
                    if let CoreData::Literal(Int64(j)) = &result_value.0 {
                        assert_eq!(i, j);
                    } else {
                        panic!("Expected Int64, got {:?}", result_value);
                    }
                }
                CoreData::Literal(String(s)) => {
                    if let CoreData::Literal(String(t)) = &result_value.0 {
                        assert_eq!(s, t);
                    } else {
                        panic!("Expected String, got {:?}", result_value);
                    }
                }
                CoreData::Literal(Bool(b)) => {
                    if let CoreData::Literal(Bool(c)) = &result_value.0 {
                        assert_eq!(b, c);
                    } else {
                        panic!("Expected Bool, got {:?}", result_value);
                    }
                }
                CoreData::Literal(Unit) => {
                    if let CoreData::Literal(Unit) = &result_value.0 {
                        // Unit matches Unit
                    } else {
                        panic!("Expected Unit, got {:?}", result_value);
                    }
                }
                _ => panic!("Unexpected value type"),
            }
        }
    }

    // Test pattern matching with binding
    #[test]
    fn test_match_binding() {
        let engine = create_test_engine();
        let value = int_val(42);

        // Create a binding pattern: x @ 42
        let pattern = Bind("x".to_string(), Box::new(Pattern::Literal(Int64(42))));

        let results = collect_stream_values(match_pattern(value.clone(), pattern, engine));

        assert_eq!(results.len(), 1);
        let (result_value, context_opt) = &results[0];
        assert!(context_opt.is_some());
        let context = context_opt.as_ref().unwrap();

        // Check that the variable was bound
        let bound_value = context.lookup("x").expect("Variable x should be bound");
        assert!(matches!(&bound_value.0, CoreData::Literal(Int64(42))));

        // Check the result value
        if let CoreData::Literal(Int64(i)) = &result_value.0 {
            assert_eq!(*i, 42);
        } else {
            panic!("Expected Int64(42), got {:?}", result_value);
        }
    }

    // Test pattern matching against a struct
    #[test]
    fn test_match_struct() {
        let engine = create_test_engine();

        // Create a struct value: Person("Alice", 30)
        let person = Value(CoreData::Struct(
            "Person".to_string(),
            vec![string_val("Alice"), int_val(30)],
        ));

        // Create a struct pattern: Person(name, age)
        let pattern = Pattern::Struct(
            "Person".to_string(),
            vec![
                Bind("name".to_string(), Box::new(Wildcard)),
                Bind("age".to_string(), Box::new(Wildcard)),
            ],
        );

        let results = collect_stream_values(match_pattern(person.clone(), pattern, engine));

        assert_eq!(results.len(), 1);
        let (result_value, context_opt) = &results[0];
        assert!(context_opt.is_some());
        let context = context_opt.as_ref().unwrap();

        // Check that variables were bound correctly
        let name = context
            .lookup("name")
            .expect("Variable name should be bound");
        let age = context.lookup("age").expect("Variable age should be bound");

        assert!(matches!(&name.0, CoreData::Literal(String(s)) if s == "Alice"));
        assert!(matches!(&age.0, CoreData::Literal(Int64(30))));

        // Values should be preserved in the result
        if let CoreData::Struct(struct_name, fields) = &result_value.0 {
            assert_eq!(struct_name, "Person");
            assert_eq!(fields.len(), 2);
            assert!(matches!(&fields[0].0, CoreData::Literal(String(s)) if s == "Alice"));
            assert!(matches!(&fields[1].0, CoreData::Literal(Int64(30))));
        } else {
            panic!("Expected struct, got {:?}", result_value);
        }
    }

    // Test pattern matching against an array decomposition
    #[test]
    fn test_match_array_decomposition() {
        let engine = create_test_engine();

        // Create an array value: [1, 2, 3]
        let array = Value(CoreData::Array(vec![int_val(1), int_val(2), int_val(3)]));

        // Create an array decomposition pattern: head :: tail
        let pattern = ArrayDecomp(
            Box::new(Bind("head".to_string(), Box::new(Wildcard))),
            Box::new(Bind("tail".to_string(), Box::new(Wildcard))),
        );

        let results = collect_stream_values(match_pattern(array.clone(), pattern, engine));

        assert_eq!(results.len(), 1);
        let (result_value, context_opt) = &results[0];
        assert!(context_opt.is_some());
        let context = context_opt.as_ref().unwrap();

        // Check that variables were bound correctly
        let head = context
            .lookup("head")
            .expect("Variable head should be bound");
        let tail = context
            .lookup("tail")
            .expect("Variable tail should be bound");

        // Head should be the first element
        assert!(matches!(&head.0, CoreData::Literal(Int64(1))));

        // Tail should be the rest of the array [2, 3]
        if let CoreData::Array(tail_items) = &tail.0 {
            assert_eq!(tail_items.len(), 2);
            assert!(matches!(&tail_items[0].0, CoreData::Literal(Int64(2))));
            assert!(matches!(&tail_items[1].0, CoreData::Literal(Int64(3))));
        } else {
            panic!("Expected Array for tail, got {:?}", tail);
        }

        // Result value should still be the array [1, 2, 3]
        if let CoreData::Array(items) = &result_value.0 {
            assert_eq!(items.len(), 3);
            assert!(matches!(&items[0].0, CoreData::Literal(Int64(1))));
            assert!(matches!(&items[1].0, CoreData::Literal(Int64(2))));
            assert!(matches!(&items[2].0, CoreData::Literal(Int64(3))));
        } else {
            panic!("Expected Array, got {:?}", result_value);
        }
    }

    // Test pattern matching with failed struct field match
    #[test]
    fn test_match_struct_field_failure() {
        let engine = create_test_engine();

        // Create a struct value: Person("Alice", 30)
        let person = Value(CoreData::Struct(
            "Person".to_string(),
            vec![string_val("Alice"), int_val(30)],
        ));

        // Create a struct pattern with a field that won't match: Person(name, 25)
        let pattern = Pattern::Struct(
            "Person".to_string(),
            vec![
                Bind("name".to_string(), Box::new(Wildcard)),
                Pattern::Literal(Int64(25)), // This won't match age 30
            ],
        );

        let results = collect_stream_values(match_pattern(person.clone(), pattern, engine));

        assert_eq!(results.len(), 1);
        let (result_value, context) = &results[0];

        // Context should be None because of the failed match
        assert!(context.is_none());

        // But the values should still be transformed/preserved
        if let CoreData::Struct(struct_name, fields) = &result_value.0 {
            assert_eq!(struct_name, "Person");
            assert_eq!(fields.len(), 2);
            // First field should be matched value
            assert!(matches!(&fields[0].0, CoreData::Literal(String(s)) if s == "Alice"));
            // Second field should be original value since pattern didn't match
            assert!(matches!(&fields[1].0, CoreData::Literal(Int64(30))));
        } else {
            panic!("Expected struct, got {:?}", result_value);
        }
    }

    // Test pattern matching against an operator
    #[test]
    fn test_match_operator() {
        let engine = create_test_engine();

        // Create a logical operator: Filter(true, [1, 2, 3])
        let array_child = Value(CoreData::Array(vec![int_val(1), int_val(2), int_val(3)]));
        let filter_op = Operator {
            tag: "Filter".to_string(),
            data: vec![bool_val(true)],
            children: vec![array_child.clone()],
        };

        let logical_value = Value(CoreData::Logical(LogicalOp(Materialized(filter_op))));

        // Create an operator pattern: Filter(predicate, relation)
        let op_pattern = Operator {
            tag: "Filter".to_string(),
            data: vec![Bind("predicate".to_string(), Box::new(Wildcard))],
            children: vec![Bind("relation".to_string(), Box::new(Wildcard))],
        };

        let pattern = Pattern::Operator(op_pattern);

        let results = collect_stream_values(match_pattern(logical_value.clone(), pattern, engine));

        assert_eq!(results.len(), 1);
        let (result_value, context_opt) = &results[0];
        assert!(context_opt.is_some());
        let context = context_opt.as_ref().unwrap();

        // Check that variables were bound correctly
        let predicate = context
            .lookup("predicate")
            .expect("Variable predicate should be bound");
        let relation = context
            .lookup("relation")
            .expect("Variable relation should be bound");

        assert!(matches!(&predicate.0, CoreData::Literal(Bool(true))));

        // Check relation is the array child
        if let CoreData::Array(items) = &relation.0 {
            assert_eq!(items.len(), 3);
            assert!(matches!(&items[0].0, CoreData::Literal(Int64(1))));
            assert!(matches!(&items[1].0, CoreData::Literal(Int64(2))));
            assert!(matches!(&items[2].0, CoreData::Literal(Int64(3))));
        } else {
            panic!("Expected Array relation, got {:?}", relation);
        }

        // Result value should be equivalent to the original
        if let CoreData::Logical(LogicalOp(Materialized(op))) = &result_value.0 {
            assert_eq!(op.tag, "Filter");
            assert_eq!(op.data.len(), 1);
            assert_eq!(op.children.len(), 1);
            assert!(matches!(&op.data[0].0, CoreData::Literal(Bool(true))));
        } else {
            panic!("Expected logical operator, got {:?}", result_value);
        }
    }

    // Test match_pattern_combinations with multiple pairs
    #[test]
    fn test_match_pattern_combinations() {
        let engine = create_test_engine();

        // Create value-pattern pairs
        let pairs = vec![
            (int_val(42), Bind("x".to_string(), Box::new(Wildcard))),
            (
                string_val("hello"),
                Bind("y".to_string(), Box::new(Wildcard)),
            ),
            (bool_val(true), Bind("z".to_string(), Box::new(Wildcard))),
        ];

        let results = collect_stream_values(match_pattern_combinations(pairs.into_iter(), engine));

        assert_eq!(results.len(), 1);
        let vec_results = &results[0];

        // Should have 3 results for 3 pairs
        assert_eq!(vec_results.len(), 3);

        // Check first pair
        let (val_1, ctx_1) = &vec_results[0];
        assert!(matches!(&val_1.0, CoreData::Literal(Int64(42))));
        assert!(ctx_1.is_some());
        if let Some(ctx) = ctx_1 {
            let x = ctx.lookup("x").expect("Variable x should be bound");
            assert!(matches!(&x.0, CoreData::Literal(Int64(42))));
        }

        // Check second pair
        let (val_2, ctx_2) = &vec_results[1];
        assert!(matches!(&val_2.0, CoreData::Literal(String(s)) if s == "hello"));
        assert!(ctx_2.is_some());
        if let Some(ctx) = ctx_2 {
            let y = ctx.lookup("y").expect("Variable y should be bound");
            assert!(matches!(&y.0, CoreData::Literal(String(s)) if s == "hello"));
        }

        // Check third pair
        let (val_3, ctx_3) = &vec_results[2];
        assert!(matches!(&val_3.0, CoreData::Literal(Bool(true))));
        assert!(ctx_3.is_some());
        if let Some(ctx) = ctx_3 {
            let z = ctx.lookup("z").expect("Variable z should be bound");
            assert!(matches!(&z.0, CoreData::Literal(Bool(true))));
        }
    }

    // Test pattern matching combinations with match failures
    #[test]
    fn test_match_pattern_combinations_with_failures() {
        let engine = create_test_engine();

        // Create value-pattern pairs, where the second pattern won't match
        let pairs = vec![
            (int_val(42), Bind("x".to_string(), Box::new(Wildcard))),
            (
                string_val("hello"),
                Pattern::Literal(String("world".to_string())),
            ), // This won't match
            (bool_val(true), Bind("z".to_string(), Box::new(Wildcard))),
        ];

        let results = collect_stream_values(match_pattern_combinations(pairs.into_iter(), engine));

        assert_eq!(results.len(), 1);
        let vec_results = &results[0];

        // Should have 3 results for 3 pairs
        assert_eq!(vec_results.len(), 3);

        // Check first pair - should match
        let (val_1, ctx_1) = &vec_results[0];
        assert!(matches!(&val_1.0, CoreData::Literal(Int64(42))));
        assert!(ctx_1.is_some());

        // Check second pair - should have None context because it didn't match
        let (val_2, ctx_2) = &vec_results[1];
        assert!(matches!(&val_2.0, CoreData::Literal(String(s)) if s == "hello"));
        assert!(ctx_2.is_none());

        // Check third pair - should match
        let (val_3, ctx_3) = &vec_results[2];
        assert!(matches!(&val_3.0, CoreData::Literal(Bool(true))));
        assert!(ctx_3.is_some());
    }

    #[test]
    fn test_complex_nested_expansion_with_partial_match() {
        use CoreData::*;
        // Create a complex MockExpander that provides nested operators with group references
        let expander = MockExpander::new(
            |group_id| {
                match group_id {
                    // Root operator: Union
                    GroupId(100) => {
                        let union_op = Operator {
                            tag: "Union".to_string(),
                            data: vec![],
                            children: vec![
                                // Left child is a reference to group 101 (Join)
                                Value(Logical(LogicalOp(UnMaterialized(GroupId(101))))),
                                // Right child is a reference to group 105 (Filter)
                                Value(Logical(LogicalOp(UnMaterialized(GroupId(105))))),
                            ],
                        };

                        vec![Value(Logical(LogicalOp(Materialized(union_op))))]
                    }
                    // Join operator with two group references
                    GroupId(101) => {
                        let join_op = Operator {
                            tag: "Join".to_string(),
                            data: vec![string_val("user_id")],
                            children: vec![
                                // Left child is a reference to group 102 (Project)
                                Value(Logical(LogicalOp(UnMaterialized(GroupId(102))))),
                                // Right child is a reference to group 103 (Filter)
                                Value(Logical(LogicalOp(UnMaterialized(GroupId(103))))),
                            ],
                        };

                        vec![Value(Logical(LogicalOp(Materialized(join_op))))]
                    }
                    // Project operator
                    GroupId(102) => {
                        let project_op = Operator {
                            tag: "Project".to_string(),
                            data: vec![int_val(42)],
                            children: vec![
                                // Child is a TableScan
                                Value(Logical(LogicalOp(Materialized(Operator {
                                    tag: "TableScan".to_string(),
                                    data: vec![string_val("users")],
                                    children: vec![],
                                })))),
                            ],
                        };

                        vec![Value(Logical(LogicalOp(Materialized(project_op))))]
                    }
                    // Filter operator
                    GroupId(103) => {
                        let filter_op = Operator {
                            tag: "Filter".to_string(),
                            data: vec![bool_val(true)],
                            children: vec![
                                // Child is a reference to group 104 (Aggregate)
                                Value(Logical(LogicalOp(UnMaterialized(GroupId(104))))),
                            ],
                        };

                        vec![Value(Logical(LogicalOp(Materialized(filter_op))))]
                    }
                    // Aggregate operator
                    GroupId(104) => {
                        let agg_op = Operator {
                            tag: "Aggregate".to_string(),
                            data: vec![string_val("count")],
                            children: vec![Value(Logical(LogicalOp(Materialized(Operator {
                                tag: "TableScan".to_string(),
                                data: vec![string_val("orders")],
                                children: vec![],
                            }))))],
                        };

                        vec![Value(Logical(LogicalOp(Materialized(agg_op))))]
                    }
                    // Another Filter operator with a group reference
                    GroupId(105) => {
                        let filter_op = Operator {
                            tag: "Filter".to_string(),
                            data: vec![bool_val(false)],
                            children: vec![
                                // Child is a reference to group 106 (Project)
                                Value(Logical(LogicalOp(UnMaterialized(GroupId(106))))),
                            ],
                        };

                        vec![Value(Logical(LogicalOp(Materialized(filter_op))))]
                    }
                    // Project operator that will NOT match our pattern
                    GroupId(106) => {
                        let project_op = Operator {
                            tag: "Project".to_string(),
                            // This has string data instead of int, so won't match our pattern
                            data: vec![string_val("column")],
                            children: vec![Value(Logical(LogicalOp(Materialized(Operator {
                                tag: "TableScan".to_string(),
                                data: vec![string_val("products")],
                                children: vec![],
                            }))))],
                        };

                        vec![Value(Logical(LogicalOp(Materialized(project_op))))]
                    }
                    _ => vec![],
                }
            },
            |_| panic!("Physical expansion not expected in this test"),
        );

        let context = Context::default();
        let engine = Engine::new(context, expander);

        // Create a reference to our root Union operator
        let root_group_ref = Value(Logical(LogicalOp(UnMaterialized(GroupId(100)))));

        // Create a complex pattern that:
        // 1. Matches the Union structure
        // 2. For the left child (Join), it will match deeply and bind variables
        // 3. For the right child (Filter), it will try to match but fail on the Project's data type

        // First, let's create patterns for the deepest operators
        let table_scan_pattern = Pattern::Operator(Operator {
            tag: "TableScan".to_string(),
            data: vec![Bind("table_name".to_string(), Box::new(Wildcard))],
            children: vec![],
        });

        // Pattern for Project with int data (will match left side, but not right)
        let project_with_int_pattern = Pattern::Operator(Operator {
            tag: "Project".to_string(),
            data: vec![Bind(
                "column_id".to_string(),
                Box::new(Pattern::Literal(Int64(42))),
            )], // Specific int value
            children: vec![Bind(
                "project_input".to_string(),
                Box::new(table_scan_pattern.clone()),
            )],
        });

        // Pattern for Aggregate
        let aggregate_pattern = Pattern::Operator(Operator {
            tag: "Aggregate".to_string(),
            data: vec![Bind("agg_func".to_string(), Box::new(Wildcard))],
            children: vec![Bind(
                "agg_input".to_string(),
                Box::new(table_scan_pattern.clone()),
            )],
        });

        // Pattern for Filter with Aggregate child
        let filter_agg_pattern = Pattern::Operator(Operator {
            tag: "Filter".to_string(),
            data: vec![Bind("filter_pred".to_string(), Box::new(Wildcard))],
            children: vec![Bind(
                "filter_child".to_string(),
                Box::new(aggregate_pattern),
            )],
        });

        // Pattern for Join with Project and Filter children
        let join_pattern = Pattern::Operator(Operator {
            tag: "Join".to_string(),
            data: vec![Bind("join_key".to_string(), Box::new(Wildcard))],
            children: vec![
                Bind(
                    "left_join_child".to_string(),
                    Box::new(project_with_int_pattern.clone()),
                ),
                Bind("right_join_child".to_string(), Box::new(filter_agg_pattern)),
            ],
        });

        // Pattern for Filter with Project child (will not match on right side of Union)
        let filter_project_pattern = Pattern::Operator(Operator {
            tag: "Filter".to_string(),
            data: vec![Bind("filter2_pred".to_string(), Box::new(Wildcard))],
            children: vec![Bind(
                "filter2_child".to_string(),
                Box::new(project_with_int_pattern),
            )],
        });

        // Root Union pattern
        let union_pattern = Pattern::Operator(Operator {
            tag: "Union".to_string(),
            data: vec![],
            children: vec![
                Bind("union_left".to_string(), Box::new(join_pattern.clone())),
                Bind("union_right".to_string(), Box::new(filter_project_pattern)),
            ],
        });

        // Now match the pattern against the root group reference
        let results = collect_stream_values(match_pattern(
            root_group_ref.clone(),
            union_pattern,
            engine.clone(),
        ));

        // Should get one result
        assert_eq!(results.len(), 1);
        let (result_value, context_opt) = &results[0];

        // Context should be None because the overall pattern match fails
        // (the right side Project has a string value, not an int value)
        assert!(context_opt.is_none());

        // But the operator structure should be expanded and transformed
        if let CoreData::Logical(LogicalOp(Materialized(union_op))) = &result_value.0 {
            assert_eq!(union_op.tag, "Union");
            assert_eq!(union_op.children.len(), 2);

            // Check left child (Join)
            if let CoreData::Logical(LogicalOp(Materialized(join))) = &union_op.children[0].0 {
                assert_eq!(join.tag, "Join");
                assert!(matches!(&join.data[0].0, CoreData::Literal(String(s)) if s == "user_id"));

                // Check Join's left child (Project)
                if let CoreData::Logical(LogicalOp(Materialized(project))) = &join.children[0].0 {
                    assert_eq!(project.tag, "Project");
                    assert!(matches!(&project.data[0].0, CoreData::Literal(Int64(42))));

                    // Check Project's child (TableScan)
                    if let CoreData::Logical(LogicalOp(Materialized(scan))) = &project.children[0].0
                    {
                        assert_eq!(scan.tag, "TableScan");
                        assert!(
                            matches!(&scan.data[0].0, CoreData::Literal(String(s)) if s == "users")
                        );
                    } else {
                        panic!(
                            "Expected TableScan under Project, got {:?}",
                            project.children[0]
                        );
                    }
                } else {
                    panic!("Expected Project, got {:?}", join.children[0]);
                }

                // Check Join's right child (Filter)
                if let CoreData::Logical(LogicalOp(Materialized(filter))) = &join.children[1].0 {
                    assert_eq!(filter.tag, "Filter");
                    assert!(matches!(&filter.data[0].0, CoreData::Literal(Bool(true))));

                    // Check Filter's child (Aggregate)
                    if let CoreData::Logical(LogicalOp(Materialized(agg))) = &filter.children[0].0 {
                        assert_eq!(agg.tag, "Aggregate");
                        assert!(
                            matches!(&agg.data[0].0, CoreData::Literal(String(s)) if s == "count")
                        );

                        // Check Aggregate's child (TableScan)
                        if let CoreData::Logical(LogicalOp(Materialized(scan))) = &agg.children[0].0
                        {
                            assert_eq!(scan.tag, "TableScan");
                            assert!(
                                matches!(&scan.data[0].0, CoreData::Literal(String(s)) if s == "orders")
                            );
                        } else {
                            panic!(
                                "Expected TableScan under Aggregate, got {:?}",
                                agg.children[0]
                            );
                        }
                    } else {
                        panic!("Expected Aggregate, got {:?}", filter.children[0]);
                    }
                } else {
                    panic!("Expected Filter, got {:?}", join.children[1]);
                }
            } else {
                panic!("Expected Join, got {:?}", union_op.children[0]);
            }

            // Check right child (Filter)
            if let CoreData::Logical(LogicalOp(Materialized(filter))) = &union_op.children[1].0 {
                assert_eq!(filter.tag, "Filter");
                assert!(matches!(&filter.data[0].0, CoreData::Literal(Bool(false))));

                // Check Filter's child (Project) - this is where the pattern match fails
                if let CoreData::Logical(LogicalOp(Materialized(project))) = &filter.children[0].0 {
                    assert_eq!(project.tag, "Project");
                    // This project has a string value, not the int 42 we tried to match
                    assert!(
                        matches!(&project.data[0].0, CoreData::Literal(String(s)) if s == "column")
                    );

                    // Check Project's child (TableScan)
                    if let CoreData::Logical(LogicalOp(Materialized(scan))) = &project.children[0].0
                    {
                        assert_eq!(scan.tag, "TableScan");
                        assert!(
                            matches!(&scan.data[0].0, CoreData::Literal(String(s)) if s == "products")
                        );
                    } else {
                        panic!(
                            "Expected TableScan under Project, got {:?}",
                            project.children[0]
                        );
                    }
                } else {
                    panic!("Expected Project, got {:?}", filter.children[0]);
                }
            } else {
                panic!("Expected Filter, got {:?}", union_op.children[1]);
            }
        } else {
            panic!("Expected Union, got {:?}", result_value);
        }

        // Now let's try a pattern that will match the whole structure
        // by making the right filter's project accept any data type

        // Pattern for Project that accepts any data
        let project_wildcard_pattern = Pattern::Operator(Operator {
            tag: "Project".to_string(),
            data: vec![Bind("project_data".to_string(), Box::new(Wildcard))],
            children: vec![Bind(
                "project_input".to_string(),
                Box::new(table_scan_pattern.clone()),
            )],
        });

        // Pattern for Filter with flexible Project child
        let filter_flexible_pattern = Pattern::Operator(Operator {
            tag: "Filter".to_string(),
            data: vec![Bind("filter2_pred".to_string(), Box::new(Wildcard))],
            children: vec![Bind(
                "filter2_child".to_string(),
                Box::new(project_wildcard_pattern),
            )],
        });

        // Root Union pattern with flexible right child
        let flexible_union_pattern = Pattern::Operator(Operator {
            tag: "Union".to_string(),
            data: vec![],
            children: vec![
                Bind("union_left".to_string(), Box::new(join_pattern)),
                Bind("union_right".to_string(), Box::new(filter_flexible_pattern)),
            ],
        });

        // Match the pattern against the root group reference with the flexible pattern
        let flexible_results = collect_stream_values(match_pattern(
            root_group_ref.clone(),
            flexible_union_pattern,
            engine,
        ));

        // Should get one result
        assert_eq!(flexible_results.len(), 1);
        let (_, flexible_context_opt) = &flexible_results[0];

        // This time the context should be Some because the pattern matches
        assert!(flexible_context_opt.is_some());
        let context = flexible_context_opt.as_ref().unwrap();

        // Check some of the bound variables
        let join_key = context.lookup("join_key").unwrap();
        assert!(matches!(&join_key.0, CoreData::Literal(String(s)) if s == "user_id"));

        let column_id = context.lookup("column_id").unwrap();
        assert!(matches!(&column_id.0, CoreData::Literal(Int64(42))));

        let filter_pred = context.lookup("filter_pred").unwrap();
        assert!(matches!(&filter_pred.0, CoreData::Literal(Bool(true))));

        let filter2_pred = context.lookup("filter2_pred").unwrap();
        assert!(matches!(&filter2_pred.0, CoreData::Literal(Bool(false))));

        // This one is from the wildcard pattern on the right side
        let project_data = context.lookup("project_data").unwrap();
        assert!(matches!(&project_data.0, CoreData::Literal(String(s)) if s == "column"));

        // Check some table names
        let tables = [
            ("users", context.lookup("table_name").unwrap()),
            ("orders", context.lookup("agg_input").unwrap()),
            ("products", context.lookup("project_input").unwrap()),
        ];

        for (expected, bound) in tables {
            if let CoreData::Logical(LogicalOp(Materialized(scan))) = &bound.0 {
                assert_eq!(scan.tag, "TableScan");
                assert!(matches!(&scan.data[0].0, CoreData::Literal(String(s)) if s == expected));
            }
        }
    }
}
