//! This module provides pattern matching functionality for expressions.
//!
//! Pattern matching is a powerful feature that allows for destructuring and examining
//! values against patterns. This implementation supports matching against literals,
//! wildcards, bindings, structs, arrays, and operators, with special handling for operators.

use super::{Engine, Evaluate, Expander};
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
use std::pin::Pin;
use Materializable::*;
use Pattern::*;

/// Type representing a match result: a value and an optional context
/// None means the match failed, Some(context) means it succeeded
type MatchResult = (Value, Option<Context>);

/// Type alias for a stream of result-wrapped match results
type MatchResultStream = Pin<Box<dyn Stream<Item = Result<MatchResult, Error>> + Send>>;

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
            let context_opt = (pattern_lit == *value_lit).then(|| engine.context);
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

            match_array_decomposition(*head_pattern, *tail_pattern, arr, engine)
        }

        // Struct pattern: match name and recursively match fields
        (Struct(pat_name, field_patterns), CoreData::Struct(val_name, field_values)) => {
            if pat_name != *val_name || field_patterns.len() != field_values.len() {
                return propagate_success((value, None));
            }

            match_struct_pattern(value.clone(), &field_patterns, field_values, engine)
        }

        // Unmaterialized operators
        (Operator(op_pattern), CoreData::Logical(LogicalOp(UnMaterialized(group_id)))) => {
            let expanded_values = engine.expander.expand_all_exprs(*group_id);
            match_against_expanded_values(&op_pattern, expanded_values, engine)
        }
        (Operator(op_pattern), CoreData::Physical(PhysicalOp(UnMaterialized(physical_goal)))) => {
            let expanded_values = engine.expander.expand_winning_expr(&physical_goal);
            match_against_expanded_values(&op_pattern, expanded_values, engine)
        }

        // Materialized operators
        (Operator(op_pattern), CoreData::Logical(LogicalOp(Materialized(operator)))) => {
            match_operator_pattern(value.clone(), &op_pattern, operator, engine)
        }
        (Operator(op_pattern), CoreData::Physical(PhysicalOp(Materialized(operator)))) => {
            match_operator_pattern(value.clone(), &op_pattern, operator, engine)
        }

        // No match for other combinations
        _ => propagate_success((value, None)),
    }
}

/// Helper function to match array decomposition patterns
fn match_array_decomposition<E>(
    head_pattern: Pattern,
    tail_pattern: Pattern,
    arr: &[Value],
    engine: Engine<E>,
) -> MatchResultStream
where
    E: Expander,
{
    // Split array into head and tail
    let head = arr[0].clone();
    let tail_elements = arr[1..].to_vec();
    let tail = Value(CoreData::Array(tail_elements.clone()));

    // Try to match head against head pattern
    match_pattern(head, head_pattern, engine.clone())
        .flat_map(move |head_result| {
            stream_from_result(
                head_result,
                capture!(
                    [tail_pattern, engine, tail, tail_elements],
                    move |(expanded_head, head_ctx_opt)| {
                        match head_ctx_opt {
                            Some(head_ctx) => {
                                // Head pattern matched, try matching tail
                                let engine_with_ctx = engine.with_context(head_ctx);
                                match_pattern(tail, tail_pattern, engine_with_ctx)
                                    .map(move |tail_result| {
                                        tail_result.map(|(expanded_tail, tail_ctx_opt)| {
                                            // Extract expanded tail elements or panic
                                            let expanded_tail_elements = match &expanded_tail.0 {
                                                CoreData::Array(elements) => elements,
                                                _ => panic!("Expected Array in tail result"),
                                            };

                                            // Create new array with expanded components
                                            let new_array = Value(CoreData::Array(
                                                std::iter::once(expanded_head.clone())
                                                    .chain(expanded_tail_elements.iter().cloned())
                                                    .collect(),
                                            ));
                                            (new_array, tail_ctx_opt)
                                        })
                                    })
                                    .boxed()
                            }
                            None => {
                                // Head pattern didn't match, return array with expanded head + original tail
                                let new_array = Value(CoreData::Array(
                                    std::iter::once(expanded_head)
                                        .chain(tail_elements.iter().cloned())
                                        .collect(),
                                ));
                                propagate_success((new_array, None))
                            }
                        }
                    }
                ),
            )
        })
        .boxed()
}

/// Helper function to match struct patterns
fn match_struct_pattern<E>(
    original_value: Value,
    field_patterns: &[Pattern],
    field_values: &[Value],
    engine: Engine<E>,
) -> MatchResultStream
where
    E: Expander,
{
    // Create pairs of field patterns and values
    let pattern_value_pairs: Vec<_> = field_patterns
        .iter()
        .cloned()
        .zip(field_values.iter().cloned())
        .collect();

    // Start with a stream containing the original context
    let initial_stream = propagate_success((original_value.clone(), Some(engine.context.clone())));

    // For each field pair, extend the context if match succeeds
    pattern_value_pairs
        .into_iter()
        .fold(initial_stream, |acc_stream, (pattern, value)| {
            acc_stream
                .flat_map(capture!([engine], move |acc_result| {
                    stream_from_result(
                        acc_result,
                        capture!([engine, pattern, value], move |(original, ctx_opt)| {
                            match ctx_opt {
                                Some(ctx) => {
                                    // If we have a context, try matching the next field
                                    let engine_with_ctx = engine.clone().with_context(ctx);
                                    match_pattern(value.clone(), pattern.clone(), engine_with_ctx)
                                        .map(move |field_result| {
                                            field_result.map(|(_, field_ctx_opt)| {
                                                // Keep original value but with new context result
                                                (original.clone(), field_ctx_opt)
                                            })
                                        })
                                        .boxed()
                                }
                                None => {
                                    // If previous match failed, no need to try further fields
                                    propagate_success((original, None))
                                }
                            }
                        }),
                    )
                }))
                .boxed()
        })
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

    // Create pairs for data and children
    let data_pairs: Vec<_> = op_pattern
        .data
        .iter()
        .cloned()
        .zip(op.data.iter().cloned())
        .collect();

    let children_pairs: Vec<_> = op_pattern
        .children
        .iter()
        .cloned()
        .zip(op.children.iter().cloned())
        .collect();

    // Start with a stream containing the original context
    let initial_stream = propagate_success((original_value.clone(), Some(engine.context.clone())));

    // First match all data components
    let after_data_stream =
        data_pairs
            .into_iter()
            .fold(initial_stream, |acc_stream, (pattern, value)| {
                acc_stream
                    .flat_map(capture!([engine], move |acc_result| {
                        stream_from_result(
                            acc_result,
                            capture!([engine, pattern, value], move |(original, ctx_opt)| {
                                match ctx_opt {
                                    Some(ctx) => {
                                        // If we have a context, try matching the next component
                                        let engine_with_ctx = engine.clone().with_context(ctx);
                                        match_pattern(
                                            value.clone(),
                                            pattern.clone(),
                                            engine_with_ctx,
                                        )
                                        .map(move |comp_result| {
                                            comp_result.map(|(_, comp_ctx_opt)| {
                                                // Keep original value but with new context result
                                                (original.clone(), comp_ctx_opt)
                                            })
                                        })
                                        .boxed()
                                    }
                                    None => {
                                        // If previous match failed, no need to try further components
                                        propagate_success((original, None))
                                    }
                                }
                            }),
                        )
                    }))
                    .boxed()
            });

    // Then match all children components
    children_pairs
        .into_iter()
        .fold(after_data_stream, |acc_stream, (pattern, value)| {
            acc_stream
                .flat_map(capture!([engine], move |acc_result| {
                    stream_from_result(
                        acc_result,
                        capture!([engine, pattern, value], move |(original, ctx_opt)| {
                            match ctx_opt {
                                Some(ctx) => {
                                    // If we have a context, try matching the next child
                                    let engine_with_ctx = engine.clone().with_context(ctx);
                                    match_pattern(value.clone(), pattern.clone(), engine_with_ctx)
                                        .map(move |child_result| {
                                            child_result.map(|(_, child_ctx_opt)| {
                                                // Keep original value but with new context result
                                                (original.clone(), child_ctx_opt)
                                            })
                                        })
                                        .boxed()
                                }
                                None => {
                                    // If previous match failed, no need to try further children
                                    propagate_success((original, None))
                                }
                            }
                        }),
                    )
                }))
                .boxed()
        })
}

/// Helper function to match a pattern against a stream of expanded values
fn match_against_expanded_values<E, S>(
    op_pattern: &Operator<Pattern>,
    expanded_values: S,
    engine: Engine<E>,
) -> MatchResultStream
where
    E: Expander,
    S: Stream<Item = Result<Value, Error>> + Send + 'static,
{
    let op_pattern = op_pattern.clone();

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
