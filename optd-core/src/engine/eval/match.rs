//! This module provides pattern matching functionality for expressions.
//!
//! Pattern matching is a powerful feature that allows for destructuring and examining
//! values against patterns. This implementation supports matching against literals,
//! wildcards, bindings, structs, and operators, with special handling for operator groups.
//!
//! The pattern matching process is asynchronous to support operations like group expansion
//! that may require database access or other I/O operations.

use super::Evaluate;
use crate::engine::utils::streams::ValueStream;
use async_recursion::async_recursion;
use futures::{future, stream, StreamExt};
use optd_dsl::analyzer::{
    context::Context,
    hir::{
        CoreData, Literal, LogicalOp, MatchArm, Materializable, Operator, Pattern, PhysicalGoal,
        PhysicalOp, ScalarOp, Value,
    },
};
use Literal::*;
use Materializable::*;
use Pattern::*;

/// Tries to match a value against a sequence of match arms.
///
/// Attempts to match the value against each arm's pattern in order.
/// Returns the result of evaluating the first matching arm's expression.
pub(super) fn try_match_arms(
    value: Value,
    match_arms: Vec<MatchArm>,
    context: Context,
) -> ValueStream {
    if match_arms.is_empty() {
        // No arms matched - this is a runtime error
        panic!("Pattern match exhausted: no pattern matched the value");
    }

    // Take the first arm
    let arm = match_arms[0].clone();
    let remaining_arms = match_arms[1..].to_vec();

    let pattern_match_future = match_pattern(value.clone(), arm.pattern, context.clone());

    stream::once(pattern_match_future)
        .flat_map(move |matched_contexts| {
            if !matched_contexts.is_empty() {
                // If we have matches, evaluate the arm's expression with all matched contexts
                stream::iter(matched_contexts)
                    .flat_map({
                        let expr = arm.expr.clone();
                        move |new_context| expr.clone().evaluate(new_context)
                    })
                    .boxed()
            } else {
                // If no matches, try the remaining arms
                try_match_arms(value.clone(), remaining_arms.clone(), context.clone())
            }
        })
        .boxed()
}

/// Attempts to match a value against a pattern.
///
/// If the match succeeds, returns a vector of updated contexts with any bound variables.
/// If the match fails, returns an empty vector.
#[async_recursion]
async fn match_pattern(value: Value, pattern: Pattern, context: Context) -> Vec<Context> {
    match (&pattern, &value.0) {
        // Wildcard pattern matches anything
        (Wildcard, _) => vec![context],

        // Binding pattern: bind the value to the identifier and continue matching
        (Bind(ident, inner_pattern), _) => {
            let mut new_ctx = context;
            new_ctx.bind(ident.clone(), value.clone());
            match_pattern(value, (**inner_pattern).clone(), new_ctx).await
        }

        // Literal pattern: match if literals are equal
        (Literal(pattern_lit), CoreData::Literal(value_lit)) => {
            match_literals(pattern_lit, value_lit, context)
        }

        (EmptyArray, CoreData::Array(arr)) if arr.is_empty() => vec![context],

        // List decomposition pattern: match first element and rest of the array
        (ArrayDecomp(head_pattern, tail_pattern), CoreData::Array(arr)) => {
            match_array_decomposition(head_pattern, tail_pattern, arr, context).await
        }

        // Struct pattern: match name and recursively match fields
        (Struct(pat_name, field_patterns), CoreData::Struct(val_name, field_values)) => {
            if pat_name != val_name || field_patterns.len() != field_values.len() {
                return vec![];
            }

            match_pattern_pairs(field_patterns.iter().zip(field_values.iter()), context).await
        }

        // All operator pattern matching
        (Operator(op_pattern), CoreData::Logical(LogicalOp(UnMaterialized(group_id)))) => {
            match_unmaterialized_logical_operator(op_pattern, group_id.0, context).await
        }
        (Operator(op_pattern), CoreData::Logical(LogicalOp(Materialized(operator)))) => {
            match_operator_pattern(op_pattern, operator, context).await
        }
        (Operator(op_pattern), CoreData::Scalar(ScalarOp(UnMaterialized(group_id)))) => {
            match_unmaterialized_scalar_operator(op_pattern, group_id.0, context).await
        }
        (Operator(op_pattern), CoreData::Scalar(ScalarOp(Materialized(operator)))) => {
            match_operator_pattern(op_pattern, operator, context).await
        }
        (Operator(op_pattern), CoreData::Physical(PhysicalOp(UnMaterialized(physical_goal)))) => {
            match_unmaterialized_physical_operator(op_pattern, physical_goal, context).await
        }
        (Operator(op_pattern), CoreData::Physical(PhysicalOp(Materialized(operator)))) => {
            match_operator_pattern(op_pattern, operator, context).await
        }

        // No match for other combinations
        _ => vec![],
    }
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
async fn match_array_decomposition(
    head_pattern: &Pattern,
    tail_pattern: &Pattern,
    arr: &[Value],
    context: Context,
) -> Vec<Context> {
    if arr.is_empty() {
        return vec![];
    }

    // Split array into head and tail
    let head = arr[0].clone();
    let tail = Value(CoreData::Array(arr[1..].to_vec()));

    // Match head against head pattern
    let head_contexts = match_pattern(head, (*head_pattern).clone(), context).await;
    if head_contexts.is_empty() {
        return vec![];
    }

    // For each successful head match, try to match tail
    let mut result_contexts = Vec::new();
    for head_ctx in head_contexts {
        let tail_contexts = match_pattern(tail.clone(), (*tail_pattern).clone(), head_ctx).await;
        result_contexts.extend(tail_contexts);
    }

    result_contexts
}

/// Expands an unmaterialized logical operator and matches against the pattern
async fn match_unmaterialized_logical_operator(
    op_pattern: &Operator<Pattern>,
    group_id: i64,
    context: Context,
) -> Vec<Context> {
    let expanded_values = expand_logical_group(group_id).await;

    // Match against each expanded value
    let context_futures = expanded_values
        .into_iter()
        .map(|expanded_value| {
            match_pattern(
                expanded_value,
                Operator(op_pattern.clone()),
                context.clone(),
            )
        })
        .collect::<Vec<_>>();

    // Await all futures and flatten the results
    future::join_all(context_futures)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Expands an unmaterialized scalar operator and matches against the pattern
async fn match_unmaterialized_scalar_operator(
    op_pattern: &Operator<Pattern>,
    group_id: i64,
    context: Context,
) -> Vec<Context> {
    let expanded_values = expand_scalar_group(group_id).await;

    // Match against each expanded value
    let context_futures = expanded_values
        .into_iter()
        .map(|expanded_value| {
            match_pattern(
                expanded_value,
                Operator(op_pattern.clone()),
                context.clone(),
            )
        })
        .collect::<Vec<_>>();

    // Await all futures and flatten the results
    future::join_all(context_futures)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Expands an unmaterialized physical operator and matches against the pattern
async fn match_unmaterialized_physical_operator(
    op_pattern: &Operator<Pattern>,
    physical_goal: &PhysicalGoal,
    context: Context,
) -> Vec<Context> {
    // Physical operators have a single optimal implementation
    let expanded_value = expand_physical_goal(physical_goal).await;

    // Match against the single expanded value
    match_pattern(expanded_value, Operator(op_pattern.clone()), context).await
}

/// Matches a pattern against a materialized operator.
///
/// This is the core operator matching function that matches a pattern against
/// a specific operator's components.
async fn match_operator_pattern(
    op_pattern: &Operator<Pattern>,
    op: &Operator<Value>,
    context: Context,
) -> Vec<Context> {
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
        context,
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
    )
    .await
}

/// Helper function to match operator children components
async fn match_operator_children(
    pattern_children: &[Pattern],
    op_children: &[Value],
    contexts: Vec<Context>,
) -> Vec<Context> {
    let context_futures = contexts
        .into_iter()
        .map(|ctx| match_pattern_pairs(pattern_children.iter().zip(op_children.iter()), ctx))
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
async fn match_pattern_pairs<'a, I>(pairs: I, initial_context: Context) -> Vec<Context>
where
    I: Iterator<Item = (&'a Pattern, &'a Value)>,
{
    // Start with a set containing just the initial context
    let mut current_contexts = vec![initial_context];

    // For each pattern-value pair
    for (pattern, value) in pairs {
        // For each context, match the pattern-value pair and collect all resulting contexts
        let next_context_futures = current_contexts
            .into_iter()
            .map(|ctx| match_pattern(value.clone(), pattern.clone(), ctx))
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

/// Asynchronously expands a logical operator group.
///
/// This function retrieves all logical operators that belong to the specified group.
async fn expand_logical_group(_group_id: i64) -> Vec<Value> {
    todo!("Implement logical group expansion")
}

/// Asynchronously expands a scalar operator group.
///
/// This function retrieves all scalar operators that belong to the specified group.
async fn expand_scalar_group(_group_id: i64) -> Vec<Value> {
    todo!("Implement scalar group expansion")
}

/// Asynchronously expands a physical goal to its optimal implementation.
///
/// This function retrieves the single best physical operator for the given goal.
async fn expand_physical_goal(_physical_goal: &PhysicalGoal) -> Value {
    todo!("Implement physical goal expansion")
}
