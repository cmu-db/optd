//! This module provides pattern matching functionality for expressions.
//!
//! Pattern matching is a powerful feature that allows for destructuring and examining
//! values against patterns. This implementation supports matching against literals,
//! wildcards, bindings, structs, arrays, and operators, with special handling for operators.

use super::{Engine, Evaluate, Expander};
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
    E: Expander,
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
    E: Expander,
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
            let expanded_values = engine.expander.expand_winning_expr(physical_goal);
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
    E: Expander,
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
    E: Expander,
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
    E: Expander,
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
    E: Expander,
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
    E: Expander,
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
