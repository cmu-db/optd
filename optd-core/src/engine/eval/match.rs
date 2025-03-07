use super::{Engine, Evaluate, Expander};
use crate::{capture, engine::utils::streams::ValueStream};
use futures::{stream, Stream, StreamExt};
use optd_dsl::analyzer::{
    context::Context,
    hir::{
        CoreData, Goal, GroupId, Literal, LogicalOp, MatchArm, Materializable, Operator, Pattern,
        PhysicalOp, Value,
    },
};
use std::pin::Pin;
use Literal::*;
use Materializable::*;
use Pattern::*;

/// Type representing a match result: a value and an optional context
/// None means the match failed, Some(context) means it succeeded
type MatchResult = (Value, Option<Context>);

/// Type alias for a stream of match results
type MatchResultStream = Pin<Box<dyn Stream<Item = MatchResult> + Send>>;

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
        panic!("Pattern match exhausted: no pattern matched the value");
    }

    // Start by attempting to match against the first arm
    let first_arm = match_arms[0].clone();
    let remaining_arms = match_arms[1..].to_vec();

    // Match the value against the arm's pattern
    match_pattern(value.clone(), first_arm.pattern.clone(), engine.clone())
        .flat_map(move |(value, context_opt)| {
            match context_opt {
                // If we got a match, evaluate the arm's expression with the context
                Some(context) => {
                    let engine_with_ctx = engine.clone().with_context(context);
                    first_arm.expr.clone().evaluate(engine_with_ctx)
                }
                // If no match, try the next arm with this value
                None => {
                    if remaining_arms.is_empty() {
                        // No more arms to try - this is a runtime error
                        panic!("Pattern match exhausted: no pattern matched the value");
                    }
                    try_match_arms(value, remaining_arms.clone(), engine.clone())
                }
            }
        })
        .boxed()
}

/// Attempts to match a value against a pattern.
///
/// Returns a stream of (Value, Option<Context>) pairs:
/// - For successful matches: (Value, Some(Context))
/// - For failed matches: (Value, None)
fn match_pattern<E>(value: Value, pattern: Pattern, engine: Engine<E>) -> MatchResultStream
where
    E: Expander,
{
    match (pattern, &value.0) {
        // Wildcard pattern matches anything
        (Wildcard, _) => stream::once(async move { (value, Some(engine.context.clone())) }).boxed(),

        // Binding pattern: bind the value to the identifier and continue matching
        (Bind(ident, inner_pattern), _) => {
            match_pattern(value.clone(), *inner_pattern, engine.clone())
                .map(move |(matched_value, ctx_opt)| {
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
                .boxed()
        }

        // Literal pattern: match if literals are equal
        (Literal(pattern_lit), CoreData::Literal(value_lit)) => {
            match_literals(value.clone(), &pattern_lit, value_lit, engine.context)
        }

        // Empty array pattern
        (EmptyArray, CoreData::Array(arr)) if arr.is_empty() => {
            stream::once(async move { (value, Some(engine.context.clone())) }).boxed()
        }

        // List decomposition pattern: match first element and rest of the array
        (ArrayDecomp(head_pattern, tail_pattern), CoreData::Array(arr)) => {
            match_array_decomposition(value.clone(), &head_pattern, &tail_pattern, arr, engine)
        }

        // Struct pattern: match name and recursively match fields
        (Struct(pat_name, field_patterns), CoreData::Struct(val_name, field_values)) => {
            if pat_name != *val_name || field_patterns.len() != field_values.len() {
                return stream::once(async move { (value, None) }).boxed();
            }

            match_struct_pattern(value.clone(), &field_patterns, field_values, engine)
        }

        // Unmaterialized operators
        (Operator(op_pattern), CoreData::Logical(LogicalOp(UnMaterialized(group_id)))) => {
            match_unmaterialized_group(&op_pattern, *group_id, engine)
        }
        (Operator(op_pattern), CoreData::Physical(PhysicalOp(UnMaterialized(physical_goal)))) => {
            match_unmaterialized_physical(&op_pattern, &physical_goal.clone(), engine)
        }

        // Materialized operators
        (Operator(op_pattern), CoreData::Logical(LogicalOp(Materialized(operator)))) => {
            match_operator_pattern(value.clone(), &op_pattern, operator, engine)
        }
        (Operator(op_pattern), CoreData::Physical(PhysicalOp(Materialized(operator)))) => {
            match_operator_pattern(value.clone(), &op_pattern, operator, engine)
        }

        // No match for other combinations
        _ => stream::once(async move { (value, None) }).boxed(),
    }
}

/// Helper function to match literals
fn match_literals(
    value: Value,
    pattern_lit: &Literal,
    value_lit: &Literal,
    context: Context,
) -> MatchResultStream {
    let result = match (pattern_lit, value_lit) {
        (Int64(p), Int64(v)) if p == v => Some(context),
        (Float64(p), Float64(v)) if p == v => Some(context),
        (String(p), String(v)) if p == v => Some(context),
        (Bool(p), Bool(v)) if p == v => Some(context),
        (Unit, Unit) => Some(context),
        _ => None,
    };

    stream::once(async move { (value, result) }).boxed()
}

/// Helper function to match array decomposition patterns
fn match_array_decomposition<E>(
    original_value: Value,
    head_pattern: &Pattern,
    tail_pattern: &Pattern,
    arr: &[Value],
    engine: Engine<E>,
) -> MatchResultStream
where
    E: Expander,
{
    if arr.is_empty() {
        return stream::once(async move { (original_value, None) }).boxed();
    }

    // Split array into head and tail
    let head = arr[0].clone();
    let tail = Value(CoreData::Array(arr[1..].to_vec()));
    let head_pattern = (*head_pattern).clone();
    let tail_pattern = (*tail_pattern).clone();

    // Match head against head pattern
    match_pattern(head, head_pattern, engine.clone())
        .flat_map(capture!([original_value], move |(_, head_ctx_opt)| {
            match head_ctx_opt {
                Some(head_ctx) => {
                    // Head matched, now try to match tail
                    let engine_with_head_ctx = engine.clone().with_context(head_ctx);
                    match_pattern(tail.clone(), tail_pattern.clone(), engine_with_head_ctx)
                        .map(capture!([original_value], move |(_, tail_ctx_opt)| {
                            // Return original value with tail match result
                            (original_value.clone(), tail_ctx_opt)
                        }))
                        .boxed()
                }
                None => {
                    // Head didn't match, so array decomposition fails
                    stream::once({
                        let value = original_value.clone();
                        async move { (value, None) }
                    })
                    .boxed()
                }
            }
        }))
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
    let initial_stream = stream::once(capture!([engine], async move {
        (original_value.clone(), Some(engine.context))
    }))
    .boxed();

    // For each field pair, extend the context if match succeeds
    pattern_value_pairs
        .into_iter()
        .fold(initial_stream, |acc_stream, (pattern, value)| {
            acc_stream
                .flat_map(capture!([engine], move |(original, ctx_opt)| {
                    match ctx_opt {
                        Some(ctx) => {
                            // If we have a context, try matching the next field
                            let engine_with_ctx = engine.clone().with_context(ctx);
                            match_pattern(value.clone(), pattern.clone(), engine_with_ctx)
                                .map(move |(original, field_ctx_opt)| {
                                    // Keep original value but with new context result
                                    (original, field_ctx_opt)
                                })
                                .boxed()
                        }
                        None => {
                            // If previous match failed, no need to try further fields
                            stream::once(async move { (original, None) }).boxed()
                        }
                    }
                }))
                .boxed()
        })
}

/// Helper function to match unmaterialized logical groups
fn match_unmaterialized_group<E>(
    op_pattern: &Operator<Pattern>,
    group_id: GroupId,
    engine: Engine<E>,
) -> MatchResultStream
where
    E: Expander,
{
    let expanded_values = engine.expander.expand_all_exprs(group_id);
    match_against_expanded_values(op_pattern, expanded_values, engine)
}

/// Helper function to match unmaterialized physical goals
fn match_unmaterialized_physical<E>(
    op_pattern: &Operator<Pattern>,
    physical_goal: &Goal,
    engine: Engine<E>,
) -> MatchResultStream
where
    E: Expander,
{
    let expanded_values = engine.expander.expand_winning_expr(physical_goal);
    match_against_expanded_values(op_pattern, expanded_values, engine)
}

/// Helper function to match a pattern against a stream of expanded values
fn match_against_expanded_values<E>(
    op_pattern: &Operator<Pattern>,
    expanded_values: ValueStream,
    engine: Engine<E>,
) -> MatchResultStream
where
    E: Expander,
{
    let op_pattern = op_pattern.clone();

    expanded_values
        .filter_map(move |expanded_value_result| {
            let op_pattern_clone = op_pattern.clone();
            let engine_clone = engine.clone();

            async move {
                match expanded_value_result {
                    Ok(expanded_value) => Some((expanded_value, op_pattern_clone, engine_clone)),
                    Err(_) => None,
                }
            }
        })
        .flat_map(move |(expanded_value, op_pattern_clone, engine_clone)| {
            match_pattern(expanded_value, Operator(op_pattern_clone), engine_clone).map(
                move |(value, ctx_opt)| {
                    // Return original value with the context from matching the expanded value
                    (value, ctx_opt)
                },
            )
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
        return stream::once(async move { (original_value, None) }).boxed();
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
    let initial_stream = stream::once(capture!([engine], async move {
        (original_value.clone(), Some(engine.context.clone()))
    }))
    .boxed();

    // First match all data components
    let after_data_stream =
        data_pairs
            .into_iter()
            .fold(initial_stream, |acc_stream, (pattern, value)| {
                acc_stream
                    .flat_map(capture!([engine], move |(original, ctx_opt)| {
                        match ctx_opt {
                            Some(ctx) => {
                                // If we have a context, try matching the next component
                                let engine_with_ctx = engine.clone().with_context(ctx);
                                match_pattern(value.clone(), pattern.clone(), engine_with_ctx)
                                    .map(move |(_, comp_ctx_opt)| {
                                        // Keep original value but with new context result
                                        (original.clone(), comp_ctx_opt)
                                    })
                                    .boxed()
                            }
                            None => {
                                // If previous match failed, no need to try further components
                                stream::once(async move { (original, None) }).boxed()
                            }
                        }
                    }))
                    .boxed()
            });

    // Then match all children components
    children_pairs
        .into_iter()
        .fold(after_data_stream, |acc_stream, (pattern, value)| {
            acc_stream
                .flat_map(capture!([engine], move |(original, ctx_opt)| {
                    match ctx_opt {
                        Some(ctx) => {
                            // If we have a context, try matching the next child
                            let engine_with_ctx = engine.clone().with_context(ctx);
                            match_pattern(value.clone(), pattern.clone(), engine_with_ctx)
                                .map(move |(_, child_ctx_opt)| {
                                    // Keep original value but with new context result
                                    (original.clone(), child_ctx_opt)
                                })
                                .boxed()
                        }
                        None => {
                            // If previous match failed, no need to try further children
                            stream::once(async move { (original, None) }).boxed()
                        }
                    }
                }))
                .boxed()
        })
}
