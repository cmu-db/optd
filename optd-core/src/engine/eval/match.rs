//! This module provides pattern matching functionality for expressions.
//!
//! Pattern matching is a powerful feature that allows for destructuring and examining
//! values against patterns. This implementation supports matching against literals,
//! wildcards, bindings, structs, and operators, with special handling for operator groups.
//!
//! The pattern matching process is asynchronous to support operations like group expansion
//! that may require database access or other I/O operations.

use super::{Engine, Evaluate, Expander};
use crate::{capture, engine::utils::streams::ValueStream};
use async_recursion::async_recursion;
use futures::{future, stream, StreamExt};
use optd_dsl::analyzer::{
    context::Context,
    hir::{
        CoreData, Literal, LogicalOp, MatchArm, Materializable, Operator, Pattern, PhysicalOp,
        ScalarOp, Value,
    },
};
use Literal::*;
use Materializable::*;
use Pattern::*;

/// Tries to match a value against a sequence of match arms.
///
/// Attempts to match the value against each arm's pattern in order.
/// Returns the result of evaluating the first matching arm's expression.
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

    // Take the first arm
    let arm = &match_arms[0];
    let expr = arm.expr.clone();
    let remaining_arms = match_arms[1..].to_vec();

    let pattern_match_future = match_pattern(value.clone(), arm.pattern.clone(), engine.clone());

    stream::once(pattern_match_future)
        .flat_map(move |matched_contexts| {
            if !matched_contexts.is_empty() {
                // If we have matches, evaluate the arm's expression with all matched contexts
                stream::iter(matched_contexts)
                    .flat_map({
                        capture!([expr, engine], move |new_context| {
                            expr.clone()
                                .evaluate(engine.clone().with_context(new_context))
                        })
                    })
                    .boxed()
            } else {
                // If no matches, try the remaining arms
                try_match_arms(value.clone(), remaining_arms.clone(), engine.clone())
            }
        })
        .boxed()
}

/// Attempts to match a value against a pattern.
///
/// If the match succeeds, returns a vector of updated contexts with any bound variables.
/// If the match fails, returns an empty vector.
#[async_recursion]
async fn match_pattern<E>(value: Value, pattern: Pattern, engine: Engine<E>) -> Vec<Context>
where
    E: Expander,
{
    match (&pattern, &value.0) {
        // Wildcard pattern matches anything
        (Wildcard, _) => vec![engine.context.clone()],

        // Binding pattern: bind the value to the identifier and continue matching
        (Bind(ident, inner_pattern), _) => {
            let mut new_ctx = engine.context.clone();
            new_ctx.bind(ident.clone(), value.clone());
            match_pattern(
                value,
                (**inner_pattern).clone(),
                engine.with_context(new_ctx),
            )
            .await
        }

        // Literal pattern: match if literals are equal
        (Literal(pattern_lit), CoreData::Literal(value_lit)) => {
            match_literals(pattern_lit, value_lit, engine.context.clone())
        }

        (EmptyArray, CoreData::Array(arr)) if arr.is_empty() => vec![engine.context.clone()],

        // List decomposition pattern: match first element and rest of the array
        (ArrayDecomp(head_pattern, tail_pattern), CoreData::Array(arr)) => {
            match_array_decomposition(head_pattern, tail_pattern, arr, engine).await
        }

        // Struct pattern: match name and recursively match fields
        (Struct(pat_name, field_patterns), CoreData::Struct(val_name, field_values)) => {
            if pat_name != val_name || field_patterns.len() != field_values.len() {
                return vec![];
            }

            match_pattern_pairs(field_patterns.iter().zip(field_values.iter()), engine).await
        }

        // Unmaterialized operators
        (Operator(op_pattern), CoreData::Logical(LogicalOp(UnMaterialized(group_id)))) => {
            let expanded_values = engine.expander.expand_logical_group(*group_id).await;
            match_against_expanded_values(op_pattern, expanded_values, engine).await
        }
        (Operator(op_pattern), CoreData::Scalar(ScalarOp(UnMaterialized(group_id)))) => {
            let expanded_values = engine.expander.expand_scalar_group(*group_id).await;
            match_against_expanded_values(op_pattern, expanded_values, engine).await
        }

        (Operator(op_pattern), CoreData::Physical(PhysicalOp(UnMaterialized(physical_goal)))) => {
            let expanded_value = engine.expander.expand_physical_goal(physical_goal).await;
            match_against_expanded_values(op_pattern, vec![expanded_value], engine).await
        }

        // Materialized operators
        (Operator(op_pattern), CoreData::Logical(LogicalOp(Materialized(operator)))) => {
            match_operator_pattern(op_pattern, operator, engine).await
        }
        (Operator(op_pattern), CoreData::Scalar(ScalarOp(Materialized(operator)))) => {
            match_operator_pattern(op_pattern, operator, engine).await
        }
        (Operator(op_pattern), CoreData::Physical(PhysicalOp(Materialized(operator)))) => {
            match_operator_pattern(op_pattern, operator, engine).await
        }

        // No match for other combinations
        _ => vec![],
    }
}

/// Helper function to match a pattern against a collection of expanded values
async fn match_against_expanded_values<E>(
    op_pattern: &Operator<Pattern>,
    expanded_values: Vec<Value>,
    engine: Engine<E>,
) -> Vec<Context>
where
    E: Expander,
{
    // Create futures for matching against each expanded value
    let context_futures = expanded_values
        .into_iter()
        .map(|expanded_value| {
            match_pattern(expanded_value, Operator(op_pattern.clone()), engine.clone())
        })
        .collect::<Vec<_>>();

    // Await all futures and flatten the results
    future::join_all(context_futures)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Helper function to match literals
fn match_literals(pattern_lit: &Literal, value_lit: &Literal, context: Context) -> Vec<Context> {
    match (pattern_lit, value_lit) {
        (Int64(p), Int64(v)) if p == v => vec![context],
        (Float64(p), Float64(v)) if p == v => vec![context],
        (String(p), String(v)) if p == v => vec![context],
        (Bool(p), Bool(v)) if p == v => vec![context],
        (Unit, Unit) => vec![context],
        _ => vec![],
    }
}

/// Helper function to match array decomposition patterns
async fn match_array_decomposition<E>(
    head_pattern: &Pattern,
    tail_pattern: &Pattern,
    arr: &[Value],
    engine: Engine<E>,
) -> Vec<Context>
where
    E: Expander,
{
    if arr.is_empty() {
        return vec![];
    }

    // Split array into head and tail
    let head = arr[0].clone();
    let tail = Value(CoreData::Array(arr[1..].to_vec()));

    // Match head against head pattern
    let head_contexts = match_pattern(head, (*head_pattern).clone(), engine.clone()).await;
    if head_contexts.is_empty() {
        return vec![];
    }

    // For each successful head match, try to match tail
    let mut result_contexts = Vec::new();
    for head_ctx in head_contexts {
        let engine_with_head_ctx = engine.clone().with_context(head_ctx);
        let tail_contexts =
            match_pattern(tail.clone(), (*tail_pattern).clone(), engine_with_head_ctx).await;
        result_contexts.extend(tail_contexts);
    }

    result_contexts
}

/// Matches a pattern against a materialized operator.
///
/// This is the core operator matching function that matches a pattern against
/// a specific operator's components.
async fn match_operator_pattern<E>(
    op_pattern: &Operator<Pattern>,
    op: &Operator<Value>,
    engine: Engine<E>,
) -> Vec<Context>
where
    E: Expander,
{
    // Match tag and kind
    if op_pattern.tag != op.tag || op_pattern.kind != op.kind {
        return vec![];
    }

    // Match component counts
    if op_pattern.operator_data.len() != op.operator_data.len()
        || op_pattern.relational_children.len() != op.relational_children.len()
        || op_pattern.scalar_children.len() != op.scalar_children.len()
    {
        return vec![];
    }

    // Step 1: Match operator data
    let ctx_after_data = match_pattern_pairs(
        op_pattern.operator_data.iter().zip(op.operator_data.iter()),
        engine.clone(),
    )
    .await;

    if ctx_after_data.is_empty() {
        return vec![];
    }

    // Step 2: Match relational children for each context from operator data
    let contexts_after_rel = match_operator_children(
        &op_pattern.relational_children,
        &op.relational_children,
        ctx_after_data,
        engine.clone(),
    )
    .await;

    if contexts_after_rel.is_empty() {
        return vec![];
    }

    // Step 3: Match scalar children for each context from relational children
    match_operator_children(
        &op_pattern.scalar_children,
        &op.scalar_children,
        contexts_after_rel,
        engine,
    )
    .await
}

/// Helper function to match operator children components
async fn match_operator_children<E>(
    pattern_children: &[Pattern],
    op_children: &[Value],
    contexts: Vec<Context>,
    engine: Engine<E>,
) -> Vec<Context>
where
    E: Expander,
{
    let context_futures = contexts
        .into_iter()
        .map(|ctx| {
            let engine_with_ctx = engine.clone().with_context(ctx);
            match_pattern_pairs(
                pattern_children.iter().zip(op_children.iter()),
                engine_with_ctx,
            )
        })
        .collect::<Vec<_>>();

    // Await all futures and collect results
    future::join_all(context_futures)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Helper function to match a series of pattern-value pairs.
///
/// This function takes pattern-value pairs and tries to match them sequentially,
/// collecting all possible matching contexts.
async fn match_pattern_pairs<'a, I, E>(pairs: I, engine: Engine<E>) -> Vec<Context>
where
    I: Iterator<Item = (&'a Pattern, &'a Value)>,
    E: Expander,
{
    // Start with a set containing just the initial context
    let mut current_contexts = vec![engine.context.clone()];

    // For each pattern-value pair
    for (pattern, value) in pairs {
        // For each context, match the pattern-value pair and collect all resulting contexts
        let next_context_futures = current_contexts
            .into_iter()
            .map(|ctx| {
                let engine_with_ctx = engine.clone().with_context(ctx);
                match_pattern(value.clone(), pattern.clone(), engine_with_ctx)
            })
            .collect::<Vec<_>>();

        // Await all futures and collect all contexts
        let next_contexts = future::join_all(next_context_futures)
            .await
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        // If no contexts matched, the entire pattern fails to match
        if next_contexts.is_empty() {
            return vec![];
        }

        // Continue with the next pattern-value pair
        current_contexts = next_contexts;
    }

    current_contexts
}
