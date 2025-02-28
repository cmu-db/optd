//! This module provides pattern matching functionality for expressions.
//!
//! Pattern matching is a powerful feature that allows for destructuring and examining
//! values against patterns. This implementation supports matching against literals,
//! wildcards, bindings, structs, and operators, with special handling for operator groups.
//!
//! The pattern matching process is asynchronous to support operations like group expansion
//! that may require database access or other I/O operations.

use crate::{
    analyzer::hir::{
        CoreData, Literal, MatchArm, Materializable, Operator, OperatorKind, Pattern, Value,
    },
    engine::utils::streams::ValueStream,
    utils::context::Context,
};
use async_recursion::async_recursion;
use futures::{future, stream, StreamExt};

use Literal::*;
use Pattern::*;

/// Tries to match a value against a sequence of match arms.
///
/// Attempts to match the value against each arm's pattern in order.
/// Returns the result of evaluating the first matching arm's expression.
///
/// # Parameters
/// * `value` - The value to match
/// * `match_arms` - The list of pattern-expression pairs to try
/// * `context` - The evaluation context
///
/// # Returns
/// A stream of evaluation results from the first matching arm
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
///
/// # Parameters
/// * `value` - The value to match
/// * `pattern` - The pattern to match against
/// * `context` - The current evaluation context
///
/// # Returns
/// A future resolving to a vector of updated contexts with bound variables if the match succeeds
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
        (Literal(pattern_lit), CoreData::Literal(value_lit)) => match (pattern_lit, value_lit) {
            (Int64(p), Int64(v)) if p == v => vec![context],
            (Float64(p), Float64(v)) if p == v => vec![context],
            (String(p), String(v)) if p == v => vec![context],
            (Bool(p), Bool(v)) if p == v => vec![context],
            (Unit, Unit) => vec![context],
            _ => vec![],
        },

        // Empty list pattern: match if value is an empty array
        (EmptyArray, CoreData::Array(arr)) if arr.is_empty() => vec![context],

        // List decomposition pattern: match first element and rest of the array
        (ArrayDecomp(head_pattern, tail_pattern), CoreData::Array(arr)) => {
            if arr.is_empty() {
                return vec![];
            }

            // Split array into head and tail
            let head = arr[0].clone();
            let tail = Value(CoreData::Array(arr[1..].to_vec()));

            // Match head against head pattern
            let head_contexts = match_pattern(head, (**head_pattern).clone(), context).await;
            if head_contexts.is_empty() {
                return vec![];
            }

            // For each successful head match, try to match tail
            let mut result_contexts = Vec::new();
            for head_ctx in head_contexts {
                let tail_contexts = match_pattern(tail.clone(), (**tail_pattern).clone(), head_ctx).await;
                result_contexts.extend(tail_contexts);
            }

            result_contexts
        }

        // Struct pattern: match name and recursively match fields
        (Struct(pat_name, field_patterns), CoreData::Struct(val_name, field_values)) => {
            if pat_name != val_name || field_patterns.len() != field_values.len() {
                return vec![];
            }

            match_pattern_pairs(field_patterns.iter().zip(field_values.iter()), context).await
        }

        // Operator pattern: match tag, kind, and components
        (Operator(op_pattern), CoreData::Operator(op_value)) => {
            match op_value {
                Materializable::Data(op) => {
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

                    // Match operator data
                    let ctx_after_data = match_pattern_pairs(
                        op_pattern.operator_data.iter().zip(op.operator_data.iter()),
                        context,
                    )
                    .await;

                    if ctx_after_data.is_empty() {
                        return vec![];
                    }

                    // Match relational children for each context from operator data
                    let contexts_after_rel = ctx_after_data
                        .into_iter()
                        .map(|ctx| {
                            match_pattern_pairs(
                                op_pattern
                                    .relational_children
                                    .iter()
                                    .zip(op.relational_children.iter()),
                                ctx,
                            )
                        })
                        .collect::<Vec<_>>();

                    // Await all futures and collect results
                    let contexts_after_rel = future::join_all(contexts_after_rel)
                        .await
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>();

                    if contexts_after_rel.is_empty() {
                        return vec![];
                    }

                    // Match scalar children for each context from relational children
                    let final_contexts_futures = contexts_after_rel
                        .into_iter()
                        .map(|ctx| {
                            match_pattern_pairs(
                                op_pattern
                                    .scalar_children
                                    .iter()
                                    .zip(op.scalar_children.iter()),
                                ctx,
                            )
                        })
                        .collect::<Vec<_>>();

                    // Await all futures and collect results
                    future::join_all(final_contexts_futures)
                        .await
                        .into_iter()
                        .flatten()
                        .collect()
                }

                Materializable::Group(group_id, kind) => {
                    // Expand the group asynchronously
                    let expanded_ops = expand_group(*group_id, *kind).await;

                    // Match against each expanded operator
                    let context_futures = expanded_ops
                        .into_iter()
                        .map(|expanded_op| {
                            let expanded_value =
                                Value(CoreData::Operator(Materializable::Data(expanded_op)));
                            match_pattern(expanded_value, pattern.clone(), context.clone())
                        })
                        .collect::<Vec<_>>();

                    // Await all futures and flatten the results
                    future::join_all(context_futures)
                        .await
                        .into_iter()
                        .flatten()
                        .collect()
                }
            }
        }

        // No match for other combinations
        _ => vec![],
    }
}

/// Helper function to match a series of pattern-value pairs.
///
/// This function takes pattern-value pairs and tries to match them sequentially,
/// collecting all possible matching contexts. For each pair, it tries to match
/// the value against the pattern for every context from the previous step.
///
/// # Parameters
/// * `pairs` - Iterator of (pattern, value) pairs to match
/// * `initial_context` - Starting context for the matching process
///
/// # Returns
/// A future resolving to a vector of contexts where all pairs matched successfully
async fn match_pattern_pairs<'a, I>(pairs: I, initial_context: Context) -> Vec<Context>
where
    I: Iterator<Item = (&'a Pattern, &'a Value)>,
{
    // Convert pairs to a Vec since we need to use it multiple times
    let pairs_vec: Vec<_> = pairs.collect();

    // If no pairs, just return the original context
    if pairs_vec.is_empty() {
        return vec![initial_context];
    }

    // Start with a set containing just the initial context
    let mut current_contexts = vec![initial_context];

    // For each pattern-value pair
    for (pattern, value) in pairs_vec {
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

/// Asynchronously expands a group of operators.
///
/// This function retrieves all operators that belong to the specified group.
/// In a real implementation, this would likely query a database or cache.
///
/// # Parameters
/// * `group_id` - The ID of the group to expand
/// * `kind` - The kind of operators in the group
///
/// # Returns
/// A future resolving to a vector of operators in the group
async fn expand_group(_group_id: i64, _kind: OperatorKind) -> Vec<Operator<Value>> {
    todo!()
}
