use super::{Engine, Evaluate, Generator};
use crate::capture;
use crate::engine::generator::Continuation;
use optd_dsl::analyzer::{
    context::Context,
    hir::{
        CoreData, Expr, LogicalOp, MatchArm, Materializable, Operator, Pattern, PhysicalOp, Value,
    },
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use Materializable::*;
use Pattern::*;

/// Type representing a match result: a value and an optional context
/// None means the match failed, Some(context) means it succeeded
type MatchResult = (Value, Option<Context>);

/// Specialized continuation type for match results
type MatchContinuation =
    Arc<dyn Fn(MatchResult) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;

/// Specialized continuation type for sequence match results
/// Takes a vector of all match results for the sequence
type MatchSequenceContinuation = Arc<
    dyn Fn(Vec<MatchResult>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
>;

/// Evaluates a pattern match expression.
///
/// First evaluates the expression to match, then tries each match arm in order
/// until a pattern matches, passing results to the continuation.
///
/// # Parameters
/// * `expr` - The expression to match against patterns
/// * `match_arms` - The list of pattern-expression pairs to try
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) async fn evaluate_pattern_match<G>(
    expr: Arc<Expr>,
    match_arms: Vec<MatchArm>,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
{
    if match_arms.is_empty() {
        panic!("Pattern match exhausted: no patterns provided");
    }

    // First evaluate the expression to match
    expr.evaluate(
        engine.clone(),
        Arc::new(move |value| {
            Box::pin(capture!([match_arms, engine, k], async move {
                // Try to match against each arm in order
                try_match_arms(value, match_arms, engine, k).await;
            }))
        }),
    )
    .await;
}

/// Tries to match a value against a sequence of match arms.
///
/// Attempts to match the value against each arm's pattern in order.
/// Evaluates the expression of the first matching arm, or panics if no arm matches.
///
/// # Parameters
/// * `value` - The value to match against patterns
/// * `match_arms` - The list of pattern-expression pairs to try
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
fn try_match_arms<G>(
    value: Value,
    match_arms: Vec<MatchArm>,
    engine: Engine<G>,
    k: Continuation,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    G: Generator,
{
    Box::pin(async move {
        if match_arms.is_empty() {
            panic!("Pattern match exhausted: no matching pattern found");
        }

        // Take the first arm and the remaining arms
        let first_arm = match_arms[0].clone();
        let remaining_arms = match_arms[1..].to_vec();

        // Try to match the value against the arm's pattern
        match_pattern(
            value,
            first_arm.pattern.clone(),
            engine.context.clone(),
            engine.generator.clone(),
            Arc::new(move |(matched_value, context_opt)| {
                Box::pin(capture!(
                    [first_arm, remaining_arms, engine, k],
                    async move {
                        match context_opt {
                            // If we got a match, evaluate the arm's expression with the context
                            Some(context) => {
                                let engine_with_ctx = engine.with_context(context);
                                first_arm.expr.evaluate(engine_with_ctx, k).await;
                            }
                            // If no match, try the next arm with the matching value and remaining arms
                            None => {
                                try_match_arms(matched_value, remaining_arms, engine, k).await;
                            }
                        }
                    }
                ))
            }),
        )
        .await;
    })
}

/// Attempts to match a value against a pattern.
///
/// # Parameters
/// * `value` - The value to match against the pattern
/// * `pattern` - The pattern to match
/// * `ctx` - The current context to extend with bindings
/// * `gen` - The generator to resolve references
/// * `k` - The continuation to receive the match result
fn match_pattern<G>(
    value: Value,
    pattern: Pattern,
    ctx: Context,
    gen: G,
    k: MatchContinuation,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    G: Generator,
{
    Box::pin(async move {
        match (pattern, &value.0) {
            // Simple patterns
            (Wildcard, _) => {
                k((value, Some(ctx))).await;
            }
            (Literal(pattern_lit), CoreData::Literal(value_lit)) => {
                let context_opt = (pattern_lit == *value_lit).then_some(ctx);
                k((value, context_opt)).await;
            }
            (EmptyArray, CoreData::Array(arr)) if arr.is_empty() => {
                k((value, Some(ctx))).await;
            }
            // Complex patterns
            (Bind(ident, inner_pattern), _) => {
                match_bind_pattern(value.clone(), ident, *inner_pattern, ctx, gen, k).await;
            }
            (ArrayDecomp(head_pat, tail_pat), CoreData::Array(arr)) => {
                if arr.is_empty() {
                    k((value, None)).await;
                    return;
                }

                match_array_pattern(*head_pat, *tail_pat, arr, ctx, gen, k).await;
            }
            (Struct(pat_name, pat_fields), CoreData::Struct(val_name, val_fields)) => {
                if pat_name != *val_name || pat_fields.len() != val_fields.len() {
                    k((value, None)).await;
                    return;
                }

                match_struct_pattern(pat_name, pat_fields, val_fields, ctx, gen, k).await;
            }
            (Operator(op_pattern), CoreData::Logical(LogicalOp(Materialized(operator)))) => {
                if op_pattern.tag != operator.tag
                    || op_pattern.data.len() != operator.data.len()
                    || op_pattern.children.len() != operator.children.len()
                {
                    k((value, None)).await;
                    return;
                }

                match_materialized_operator(true, op_pattern, operator.clone(), ctx, gen, k).await;
            }
            (Operator(op_pattern), CoreData::Physical(PhysicalOp(Materialized(operator)))) => {
                if op_pattern.tag != operator.tag
                    || op_pattern.data.len() != operator.data.len()
                    || op_pattern.children.len() != operator.children.len()
                {
                    k((value, None)).await;
                    return;
                }

                match_materialized_operator(false, op_pattern, operator.clone(), ctx, gen, k).await;
            }
            // Unmaterialized operators
            (Operator(op_pattern), CoreData::Logical(LogicalOp(UnMaterialized(group_id)))) => {
                gen.clone()
                    .yield_group(
                        *group_id,
                        Arc::new(move |expanded_value| {
                            Box::pin(capture!([op_pattern, ctx, gen, k], async move {
                                match_pattern(expanded_value, Operator(op_pattern), ctx, gen, k)
                                    .await;
                            }))
                        }),
                    )
                    .await;
            }
            (Operator(op_pattern), CoreData::Physical(PhysicalOp(UnMaterialized(goal)))) => {
                gen.clone()
                    .yield_goal(
                        goal,
                        Arc::new(move |expanded_value| {
                            Box::pin(capture!([op_pattern, ctx, gen, k], async move {
                                match_pattern(expanded_value, Operator(op_pattern), ctx, gen, k)
                                    .await;
                            }))
                        }),
                    )
                    .await;
            }
            // No match for other combinations
            _ => {
                k((value, None)).await;
            }
        }
    })
}

/// Matches a binding pattern
async fn match_bind_pattern<G>(
    value: Value,
    ident: String,
    inner_pattern: Pattern,
    ctx: Context,
    generator: G,
    k: MatchContinuation,
) where
    G: Generator,
{
    // First check if the inner pattern matches without binding
    match_pattern(
        value,
        inner_pattern,
        ctx.clone(),
        generator,
        Arc::new(move |(matched_value, ctx_opt)| {
            Box::pin(capture!([ident, k], async move {
                // Only bind if the inner pattern matched
                if let Some(mut ctx) = ctx_opt {
                    // Create a new context with the binding
                    ctx.bind(ident.clone(), matched_value.clone());
                    k((matched_value, Some(ctx))).await;
                } else {
                    // Inner pattern didn't match, propagate failure
                    k((matched_value, None)).await;
                }
            }))
        }),
    )
    .await;
}

/// Matches an array decomposition pattern
async fn match_array_pattern<G>(
    head_pattern: Pattern,
    tail_pattern: Pattern,
    arr: &[Value],
    ctx: Context,
    generator: G,
    k: MatchContinuation,
) where
    G: Generator,
{
    // Split array into head and tail
    let head = arr[0].clone();
    let tail_elements = arr[1..].to_vec();
    let tail = Value(CoreData::Array(tail_elements));

    // Create components to match sequentially
    let patterns = vec![head_pattern, tail_pattern];
    let values = vec![head, tail];

    // Use match_components to process all components
    match_components(
        patterns,
        values,
        ctx.clone(),
        generator,
        Arc::new(move |results| {
            Box::pin(capture!([ctx, k], async move {
                // Check if all parts matched successfully
                let all_matched = results.iter().all(|(_, ctx_opt)| ctx_opt.is_some());

                // All matched or not, get the matched values
                let head_value = results[0].0.clone();
                let tail_value = results[1].0.clone();

                // Extract tail elements
                let tail_elements = match &tail_value.0 {
                    CoreData::Array(elements) => elements.clone(),
                    _ => panic!("Expected Array in tail result"),
                };

                // Create new array with matched head + tail elements
                let new_array = Value(CoreData::Array(
                    std::iter::once(head_value).chain(tail_elements).collect(),
                ));

                if all_matched {
                    // Combine contexts by folding over the results, starting with the base context
                    let combined_ctx = results
                        .iter()
                        .filter_map(|(_, ctx_opt)| ctx_opt.clone())
                        .fold(ctx, |mut acc_ctx, ctx| {
                            acc_ctx.merge(ctx);
                            acc_ctx
                        });

                    // Return the new array with the combined context
                    k((new_array, Some(combined_ctx))).await;
                } else {
                    // Return the new array but with None context since match failed
                    k((new_array, None)).await;
                }
            }))
        }),
    )
    .await;
}

/// Matches a struct pattern
async fn match_struct_pattern<G>(
    pat_name: String,
    field_patterns: Vec<Pattern>,
    field_values: &[Value],
    ctx: Context,
    generator: G,
    k: MatchContinuation,
) where
    G: Generator,
{
    // Match fields sequentially
    match_components(
        field_patterns,
        field_values.to_vec(),
        ctx.clone(),
        generator,
        Arc::new(move |results| {
            Box::pin(capture!([ctx, pat_name, k], async move {
                // Check if all fields matched successfully
                let all_matched = results.iter().all(|(_, ctx_opt)| ctx_opt.is_some());

                // Reconstruct struct with matched field values
                let matched_values = results.iter().map(|(v, _)| v.clone()).collect();
                let new_struct = Value(CoreData::Struct(pat_name, matched_values));

                if all_matched {
                    // Combine contexts by folding over the results, starting with the base context
                    let combined_ctx = results
                        .iter()
                        .filter_map(|(_, ctx_opt)| ctx_opt.clone())
                        .fold(ctx, |mut acc_ctx, ctx| {
                            acc_ctx.merge(ctx);
                            acc_ctx
                        });

                    // Return the new struct with the combined context
                    k((new_struct, Some(combined_ctx))).await;
                } else {
                    // Return the new struct but with None context since match failed
                    k((new_struct, None)).await;
                }
            }))
        }),
    )
    .await;
}

/// Matches a materialized operator
async fn match_materialized_operator<G>(
    is_logical: bool,
    op_pattern: Operator<Pattern>,
    operator: Operator<Value>,
    ctx: Context,
    gen: G,
    k: MatchContinuation,
) where
    G: Generator,
{
    // Create all patterns and values to match
    let data_patterns = op_pattern.data;
    let children_patterns = op_pattern.children;
    let all_patterns: Vec<Pattern> = data_patterns
        .into_iter()
        .chain(children_patterns.into_iter())
        .collect();

    let data_values = operator.data.clone();
    let children_values = operator.children.clone();
    let all_values: Vec<Value> = data_values
        .into_iter()
        .chain(children_values.into_iter())
        .collect();

    // Match all components sequentially
    match_components(
        all_patterns,
        all_values,
        ctx.clone(),
        gen,
        Arc::new(move |results| {
            Box::pin(capture!([ctx, operator, k], async move {
                // Check if all components matched successfully
                let all_matched = results.iter().all(|(_, ctx_opt)| ctx_opt.is_some());

                // Split results back into data and children
                let data_len = operator.data.len();
                let matched_values: Vec<_> = results.iter().map(|(v, _)| v.clone()).collect();
                let matched_data = matched_values[..data_len].to_vec();
                let matched_children = matched_values[data_len..].to_vec();

                // Create new operator with matched components
                let new_op = Operator {
                    tag: operator.tag.clone(),
                    data: matched_data,
                    children: matched_children,
                };

                // Create appropriate value type based on original_value
                let new_value = if is_logical {
                    Value(CoreData::Logical(LogicalOp(Materialized(new_op))))
                } else {
                    Value(CoreData::Physical(PhysicalOp(Materialized(new_op))))
                };

                if all_matched {
                    // Combine contexts by folding over the results, starting with the base context
                    let combined_ctx = results
                        .iter()
                        .filter_map(|(_, ctx_opt)| ctx_opt.clone())
                        .fold(ctx, |mut acc_ctx, ctx| {
                            acc_ctx.merge(ctx);
                            acc_ctx
                        });

                    // Return the new operator with the combined context
                    k((new_value, Some(combined_ctx))).await;
                } else {
                    // Return the new operator but with None context since match failed
                    k((new_value, None)).await;
                }
            }))
        }),
    )
    .await;
}

/// Helper function to match multiple components (struct fields, array parts, or operator components)
/// against their corresponding patterns sequentially.
///
/// # Parameters
/// * `patterns` - The patterns to match against
/// * `values` - The values to match
/// * `ctx` - The current context to extend with bindings
/// * `generator` - The generator to resolve references
/// * `k` - The continuation to receive the vector of match results
fn match_components<G>(
    patterns: Vec<Pattern>,
    values: Vec<Value>,
    ctx: Context,
    generator: G,
    k: MatchSequenceContinuation,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    G: Generator,
{
    // Start the sequential matching process with an empty results vector
    match_components_sequentially(patterns, values, 0, ctx, Vec::new(), generator, k)
}

/// Internal helper function to match components sequentially
///
/// This function will process all components regardless of match success,
/// collecting results for each component to allow proper reconstruction of arrays/structs.
fn match_components_sequentially<G>(
    patterns: Vec<Pattern>,
    values: Vec<Value>,
    index: usize,
    ctx: Context,
    results: Vec<MatchResult>,
    generator: G,
    k: MatchSequenceContinuation,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    G: Generator,
{
    Box::pin(async move {
        // Base case: all components matched
        if index >= patterns.len() {
            k(results).await;
            return;
        }

        // Match current component
        match_pattern(
            values[index].clone(),
            patterns[index].clone(),
            ctx.clone(),
            generator.clone(),
            Arc::new(move |(matched_value, ctx_opt)| {
                Box::pin(capture!(
                    [patterns, values, index, results, ctx, generator, k],
                    async move {
                        // Add this result to the results vector, keeping the context as is
                        // (Some if matched, None if didn't match)
                        let mut updated_results = results;
                        updated_results.push((matched_value, ctx_opt));

                        // Continue to the next component regardless of match outcome
                        match_components_sequentially(
                            patterns,
                            values,
                            index + 1,
                            ctx,
                            updated_results,
                            generator,
                            k,
                        )
                        .await;
                    }
                ))
            }),
        )
        .await
    })
}
