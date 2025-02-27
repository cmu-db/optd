use crate::{
    analyzer::hir::{
        CoreData, Expr, Literal, MatchArm, Materializable, Operator, OperatorKind, Pattern, Value,
    },
    capture,
    engine::utils::streams::{stream_from_result, ValueStream},
    utils::context::Context,
};
use futures::StreamExt;

use Literal::*;
use Pattern::*;

// TODO: 1. Make match return a stream, mock expand_group.
// 2. Arcs on exprs and contexts, 3. caching of (smart)stream?

/// Evaluates a pattern match expression.
///
/// First evaluates the expression to match, then tries each match arm in order
/// until a pattern matches.
///
/// # Parameters
/// * `expr` - The expression to match against patterns
/// * `match_arms` - The list of pattern-expression pairs to try
/// * `context` - The evaluation context
///
/// # Returns
/// A stream of all possible evaluation results
pub(super) fn evaluate_pattern_match(
    expr: Expr,
    match_arms: Vec<MatchArm>,
    context: Context,
) -> ValueStream {
    // First evaluate the expression
    expr.evaluate(context.clone())
        .flat_map(move |expr_result| {
            stream_from_result(
                expr_result,
                capture!([context, match_arms], move |value| {
                    // Try each match arm in sequence
                    try_match_arms(value, match_arms, context)
                }),
            )
        })
        .boxed()
}

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
fn try_match_arms(value: Value, match_arms: Vec<MatchArm>, context: Context) -> ValueStream {
    if match_arms.is_empty() {
        // No arms matched - this is a runtime error
        panic!("Pattern match exhausted: no pattern matched the value");
    }

    // Take the first arm
    let arm = match_arms[0].clone();
    let remaining_arms = match_arms[1..].to_vec();

    // Try to match the pattern - now handling multiple contexts
    let matched_contexts = match_pattern(&value, &arm.pattern, context.clone());

    if !matched_contexts.is_empty() {
        // If we have any matches, evaluate the arm's expression with all the matched contexts
        // We'll use a streaming approach to handle all contexts
        futures::stream::iter(matched_contexts)
            .flat_map(move |new_context| arm.expr.clone().evaluate(new_context))
            .boxed()
    } else {
        // If no matches, try the remaining arms
        try_match_arms(value, remaining_arms, context)
    }
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
/// A vector of updated contexts with bound variables if the match succeeds
fn match_pattern(value: &Value, pattern: &Pattern, context: Context) -> Vec<Context> {
    match (pattern, &value.0) {
        // Wildcard pattern matches anything
        (Wildcard, _) => vec![context],

        // Binding pattern: bind the value to the identifier and continue matching
        (Bind(ident, inner_pattern), _) => {
            let mut new_ctx = context.clone();
            new_ctx.bind(ident.clone(), value.clone());
            match_pattern(value, inner_pattern, new_ctx)
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

        // Struct pattern: match name and recursively match fields
        (Struct(pat_name, field_patterns), CoreData::Struct(val_name, field_values)) => {
            if pat_name != val_name || field_patterns.len() != field_values.len() {
                return vec![];
            }

            match_components(context, field_patterns.iter().zip(field_values.iter()))
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
                    let ctx_after_data = match_components(
                        context,
                        op_pattern.operator_data.iter().zip(op.operator_data.iter()),
                    );

                    if ctx_after_data.is_empty() {
                        return vec![];
                    }

                    // For each context after matching operator data
                    let mut contexts_after_rel = Vec::new();
                    for ctx in ctx_after_data {
                        // Match relational children
                        let rel_contexts = match_components(
                            ctx,
                            op_pattern
                                .relational_children
                                .iter()
                                .zip(op.relational_children.iter()),
                        );
                        contexts_after_rel.extend(rel_contexts);
                    }

                    if contexts_after_rel.is_empty() {
                        return vec![];
                    }

                    // For each context after matching relational children
                    let mut final_contexts = Vec::new();
                    for ctx in contexts_after_rel {
                        // Match scalar children
                        let scalar_contexts = match_components(
                            ctx,
                            op_pattern
                                .scalar_children
                                .iter()
                                .zip(op.scalar_children.iter()),
                        );
                        final_contexts.extend(scalar_contexts);
                    }

                    final_contexts
                }

                Materializable::Group(group_id, kind) => {
                    // Expand the group and match against each expanded operator
                    expand_group(*group_id, *kind)
                        .into_iter()
                        .flat_map(|expanded_op| {
                            // Create a value for the expanded operator
                            let expanded_value =
                                Value(CoreData::Operator(Materializable::Data(expanded_op)));

                            // Match the pattern against the expanded operator
                            match_pattern(&expanded_value, pattern, context.clone())
                        })
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
/// Attempts to match each pattern-value pair in sequence, threading the contexts through.
/// Returns an empty vector if any pair fails to match.
///
/// # Parameters
/// * `context` - Current context to match against
/// * `pairs` - Iterator of (pattern, value) pairs to match
///
/// # Returns
/// A vector of updated contexts if all pairs match
fn match_components<'a, I>(context: Context, pairs: I) -> Vec<Context>
where
    I: Iterator<Item = (&'a Pattern, &'a Value)>,
{
    let mut current_contexts = vec![context];

    for (pattern, value) in pairs {
        // For each pattern-value pair, try matching with all current contexts
        let mut next_contexts = Vec::new();

        for ctx in current_contexts {
            // Match the current pattern-value pair with the current context
            let matched_contexts = match_pattern(value, pattern, ctx);
            next_contexts.extend(matched_contexts);
        }

        // If no contexts matched, the entire pattern fails to match
        if next_contexts.is_empty() {
            return vec![];
        }

        // Continue with the next pattern-value pair
        current_contexts = next_contexts;
    }

    current_contexts
}

/// Mock implementation of expand_group.
///
/// In a real implementation, this would query a database or cache to retrieve
/// all operators that belong to the specified group.
///
/// # Parameters
/// * `group_id` - The ID of the group to expand
/// * `kind` - The kind of operators in the group
///
/// # Returns
/// A vector of operators in the group
fn expand_group(group_id: i64, kind: OperatorKind) -> Vec<Operator<Value>> {
    todo!()
}
