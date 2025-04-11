use crate::{
    analyzer::{
        context::Context,
        hir::{
            CoreData, Expr, LogicalOp, MatchArm, Materializable, Operator, Pattern, PatternKind,
            PhysicalOp, Value,
        },
    },
    engine::{Continuation, EngineResponse},
};
use crate::{capture, engine::Engine};
use Materializable::*;
use PatternKind::*;
use futures::future::BoxFuture;
use std::sync::Arc;

/// A type representing a match result, which is a value and an optional context.
///
/// None means the match failed, Some(context) means it succeeded.
type MatchResult = (Value, Option<Context>);

impl<O: Clone + Send + 'static> Engine<O> {
    /// Evaluates a pattern match expression.
    ///
    /// First evaluates the expression to match, then tries each match arm in order until a pattern
    /// matches, passing results to the continuation.
    ///
    /// # Parameters
    ///
    /// * `expr` - The expression to match against patterns.
    /// * `match_arms` - The list of pattern-expression pairs to try.
    /// * `k` - The continuation to receive evaluation results.
    pub(crate) async fn evaluate_pattern_match(
        self,
        expr: Arc<Expr>,
        match_arms: Vec<MatchArm>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        let engine = self.clone();

        if match_arms.is_empty() {
            panic!("Pattern match exhausted: no patterns provided");
        }

        // First evaluate the expression to match.
        self.evaluate(
            expr,
            Arc::new(move |value| {
                Box::pin(capture!([match_arms, engine, k], async move {
                    // Try to match against each arm in order.
                    engine.try_match_arms(value, match_arms, k).await
                }))
            }),
        )
        .await
    }

    /// Tries to match a value against a sequence of match arms.
    ///
    /// Attempts to match the value against each arm's pattern in order.
    /// Evaluates the expression of the first matching arm, or panics if no arm matches.
    ///
    /// # Parameters
    /// * `value` - The value to match against patterns.
    /// * `match_arms` - The list of pattern-expression pairs to try.
    /// * `k` - The continuation to receive evaluation results.
    fn try_match_arms(
        self,
        value: Value,
        match_arms: Vec<MatchArm>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> BoxFuture<'static, EngineResponse<O>> {
        Box::pin(async move {
            let engine = self.clone();

            if match_arms.is_empty() {
                panic!("Pattern match exhausted: no matching pattern found");
            }

            // Take the first arm and the remaining arms.
            let first_arm = match_arms[0].clone();
            let remaining_arms = match_arms[1..].to_vec();

            // Try to match the value against the arm's pattern.
            match_pattern(
                value,
                first_arm.pattern.clone(),
                self.context,
                Arc::new(move |(matched_value, context_opt)| {
                    Box::pin(capture!(
                        [first_arm, remaining_arms, engine, k],
                        async move {
                            match context_opt {
                                // If we got a match, evaluate the arm's expression with the context.
                                Some(context) => {
                                    let engine_with_ctx = engine.with_new_context(context);
                                    engine_with_ctx.evaluate(first_arm.expr, k).await
                                }
                                // If no match, try the next arm with the matching value.
                                None => {
                                    engine
                                        .try_match_arms(matched_value, remaining_arms, k)
                                        .await
                                }
                            }
                        }
                    ))
                }),
            )
            .await
        })
    }
}

/// Attempts to match a value against a pattern.
///
/// # Parameters
///
/// * `value` - The value to match against the pattern.
/// * `pattern` - The pattern to match.
/// * `ctx` - The current context to extend with bindings.
/// * `k` - The continuation to receive the match result.
fn match_pattern<O>(
    value: Value,
    pattern: Pattern,
    ctx: Context,
    k: Continuation<MatchResult, EngineResponse<O>>,
) -> BoxFuture<'static, EngineResponse<O>>
where
    O: Clone + Send + 'static,
{
    Box::pin(async move {
        match (pattern.kind, &value.data) {
            // Simple patterns.
            (Wildcard, _) => k((value, Some(ctx))).await,
            (Literal(pattern_lit), CoreData::Literal(value_lit)) => {
                let context_opt = (pattern_lit == *value_lit).then_some(ctx);
                k((value, context_opt)).await
            }
            (EmptyArray, CoreData::Array(arr)) if arr.is_empty() => k((value, Some(ctx))).await,

            // Complex patterns.
            (Bind(ident, inner_pattern), _) => {
                match_bind_pattern(value.clone(), ident, *inner_pattern, ctx, k).await
            }
            (ArrayDecomp(head_pat, tail_pat), CoreData::Array(arr)) => {
                if arr.is_empty() {
                    return k((value, None)).await;
                }

                match_array_pattern(*head_pat, *tail_pat, arr, ctx, k).await
            }
            (Struct(pat_name, pat_fields), CoreData::Struct(val_name, val_fields)) => {
                if pat_name != *val_name || pat_fields.len() != val_fields.len() {
                    return k((value, None)).await;
                }

                match_struct_pattern(pat_name, pat_fields, val_fields, ctx, k).await
            }

            // Materialized logical operators
            (Operator(op_pattern), CoreData::Logical(Materialized(log_op))) => {
                if op_pattern.tag != log_op.operator.tag
                    || op_pattern.data.len() != log_op.operator.data.len()
                    || op_pattern.children.len() != log_op.operator.children.len()
                {
                    return k((value, None)).await;
                }

                match_materialized_operator(true, op_pattern, log_op.operator.clone(), ctx, k).await
            }

            // Materialized physical operators
            (Operator(op_pattern), CoreData::Physical(Materialized(phys_op))) => {
                if op_pattern.tag != phys_op.operator.tag
                    || op_pattern.data.len() != phys_op.operator.data.len()
                    || op_pattern.children.len() != phys_op.operator.children.len()
                {
                    return k((value, None)).await;
                }

                match_materialized_operator(false, op_pattern, phys_op.operator.clone(), ctx, k)
                    .await
            }

            // Unmaterialized logical operators
            (Operator(op_pattern), CoreData::Logical(UnMaterialized(group_id))) => {
                // Yield the group id back to the caller and provide a callback to match expanded value against the pattern.
                EngineResponse::YieldGroup(
                    *group_id,
                    Arc::new(move |expanded_value| {
                        Box::pin(capture!([op_pattern, ctx, k], async move {
                            match_pattern(
                                expanded_value,
                                Pattern::new(Operator(op_pattern)),
                                ctx,
                                k,
                            )
                            .await
                        }))
                    }),
                )
            }

            // Unmaterialized physical operators
            (Operator(op_pattern), CoreData::Physical(UnMaterialized(goal))) => {
                // Yield the goal back to the caller and provide a callback to match expanded value against the pattern.
                EngineResponse::YieldGoal(
                    goal.clone(),
                    Arc::new(move |expanded_value| {
                        Box::pin(capture!([op_pattern, ctx, k], async move {
                            match_pattern(
                                expanded_value,
                                Pattern::new(Operator(op_pattern)),
                                ctx,
                                k,
                            )
                            .await
                        }))
                    }),
                )
            }

            // No match for other combinations.
            _ => k((value, None)).await,
        }
    })
}

/// Matches a binding pattern.
async fn match_bind_pattern<O>(
    value: Value,
    ident: String,
    inner_pattern: Pattern,
    ctx: Context,
    k: Continuation<MatchResult, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Clone + Send + 'static,
{
    // First check if the inner pattern matches without binding.
    match_pattern(
        value,
        inner_pattern,
        ctx.clone(),
        Arc::new(move |(matched_value, ctx_opt)| {
            Box::pin(capture!([ident, k], async move {
                // Only bind if the inner pattern matched.
                if let Some(mut ctx) = ctx_opt {
                    // Create a new context with the binding.
                    ctx.bind(ident.clone(), matched_value.clone());
                    k((matched_value, Some(ctx))).await
                } else {
                    // Inner pattern didn't match, propagate failure.
                    k((matched_value, None)).await
                }
            }))
        }),
    )
    .await
}

/// Matches an array decomposition pattern.
async fn match_array_pattern<O>(
    head_pattern: Pattern,
    tail_pattern: Pattern,
    arr: &[Value],
    ctx: Context,
    k: Continuation<MatchResult, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Clone + Send + 'static,
{
    // Split array into head and tail.
    let head = arr[0].clone();
    let tail_elements = arr[1..].to_vec();
    let tail = Value::new(CoreData::Array(tail_elements));

    // Create components to match sequentially.
    let patterns = vec![head_pattern, tail_pattern];
    let values = vec![head, tail];

    // Use match_components to process all components.
    match_components(
        patterns,
        values,
        ctx.clone(),
        Arc::new(move |results| {
            Box::pin(capture!([ctx, k], async move {
                // Check if all parts matched successfully.
                let all_matched = results.iter().all(|(_, ctx_opt)| ctx_opt.is_some());

                // All matched or not, get the matched values.
                let head_value = results[0].0.clone();
                let tail_value = results[1].0.clone();

                // Extract tail elements.
                let tail_elements = match &tail_value.data {
                    CoreData::Array(elements) => elements.clone(),
                    _ => panic!("Expected Array in tail result"),
                };

                // Create new array with matched head + tail elements.
                let new_array = Value::new(CoreData::Array(
                    std::iter::once(head_value).chain(tail_elements).collect(),
                ));

                if all_matched {
                    // Combine contexts by folding over the results, starting with the base context.
                    let combined_ctx = results
                        .iter()
                        .filter_map(|(_, ctx_opt)| ctx_opt.clone())
                        .fold(ctx, |mut acc_ctx, ctx| {
                            acc_ctx.merge(ctx);
                            acc_ctx
                        });

                    // Return the new array with the combined context.
                    k((new_array, Some(combined_ctx))).await
                } else {
                    // Return the new array but with None context since match failed.
                    k((new_array, None)).await
                }
            }))
        }),
    )
    .await
}

/// Matches a struct pattern.
async fn match_struct_pattern<O>(
    pat_name: String,
    field_patterns: Vec<Pattern>,
    field_values: &[Value],
    ctx: Context,
    k: Continuation<MatchResult, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Clone + Send + 'static,
{
    // Match fields sequentially.
    match_components(
        field_patterns,
        field_values.to_vec(),
        ctx.clone(),
        Arc::new(move |results| {
            Box::pin(capture!([ctx, pat_name, k], async move {
                // Check if all fields matched successfully.
                let all_matched = results.iter().all(|(_, ctx_opt)| ctx_opt.is_some());

                // Reconstruct struct with matched field values.
                let matched_values = results.iter().map(|(v, _)| v.clone()).collect();
                let new_struct = Value::new(CoreData::Struct(pat_name, matched_values));

                if all_matched {
                    // Combine contexts by folding over the results, starting with the base context.
                    let combined_ctx = results
                        .iter()
                        .filter_map(|(_, ctx_opt)| ctx_opt.clone())
                        .fold(ctx, |mut acc_ctx, ctx| {
                            acc_ctx.merge(ctx);
                            acc_ctx
                        });

                    // Return the new struct with the combined context.
                    k((new_struct, Some(combined_ctx))).await
                } else {
                    // Return the new struct but with None context since match failed.
                    k((new_struct, None)).await
                }
            }))
        }),
    )
    .await
}

/// Matches a materialized operator.
async fn match_materialized_operator<O>(
    is_logical: bool,
    op_pattern: Operator<Pattern>,
    operator: Operator<Value>,
    ctx: Context,
    k: Continuation<MatchResult, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Clone + Send + 'static,
{
    // Create all patterns and values to match.
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

    // Match all components sequentially.
    match_components(
        all_patterns,
        all_values,
        ctx.clone(),
        Arc::new(move |results| {
            Box::pin(capture!([ctx, operator, is_logical, k], async move {
                // Check if all components matched successfully.
                let all_matched = results.iter().all(|(_, ctx_opt)| ctx_opt.is_some());

                // Split results back into data and children.
                let data_len = operator.data.len();
                let matched_values: Vec<_> = results.iter().map(|(v, _)| v.clone()).collect();
                let matched_data = matched_values[..data_len].to_vec();
                let matched_children = matched_values[data_len..].to_vec();

                // Create new operator with matched components.
                let new_op = Operator {
                    tag: operator.tag.clone(),
                    data: matched_data,
                    children: matched_children,
                };

                // Create appropriate value type based on original_value.
                let new_value = if is_logical {
                    let log_op = LogicalOp::logical(new_op);
                    Value::new(CoreData::Logical(Materialized(log_op)))
                } else {
                    let phys_op = PhysicalOp::physical(new_op);
                    Value::new(CoreData::Physical(Materialized(phys_op)))
                };

                if all_matched {
                    // Combine contexts by folding over the results, starting with the base context.
                    let combined_ctx = results
                        .iter()
                        .filter_map(|(_, ctx_opt)| ctx_opt.clone())
                        .fold(ctx, |mut acc_ctx, ctx| {
                            acc_ctx.merge(ctx);
                            acc_ctx
                        });

                    // Return the new operator with the combined context.
                    k((new_value, Some(combined_ctx))).await
                } else {
                    // Return the new operator but with None context since match failed.
                    k((new_value, None)).await
                }
            }))
        }),
    )
    .await
}

/// Helper function to match multiple components (struct fields, array parts, or operator components)
/// against their corresponding patterns sequentially.
///
/// # Parameters
///
/// * `patterns` - The patterns to match against.
/// * `values` - The values to match.
/// * `ctx` - The current context to extend with bindings.
/// * `k` - The continuation to receive the vector of match results.
async fn match_components<O>(
    patterns: Vec<Pattern>,
    values: Vec<Value>,
    ctx: Context,
    k: Continuation<Vec<MatchResult>, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Clone + Send + 'static,
{
    // Start the sequential matching process with an empty results vector.
    match_components_sequentially(patterns, values, 0, ctx, Vec::new(), k).await
}

/// Internal helper function to match components sequentially.
///
/// This function will process all components regardless of match success, collecting results for
/// each component to allow proper reconstruction of arrays/structs.
fn match_components_sequentially<O>(
    patterns: Vec<Pattern>,
    values: Vec<Value>,
    index: usize,
    ctx: Context,
    results: Vec<MatchResult>,
    k: Continuation<Vec<MatchResult>, EngineResponse<O>>,
) -> BoxFuture<'static, EngineResponse<O>>
where
    O: Clone + Send + 'static,
{
    Box::pin(async move {
        // Base case: all components matched.
        if index >= patterns.len() {
            return k(results).await;
        }

        // Match current component.
        match_pattern(
            values[index].clone(),
            patterns[index].clone(),
            ctx.clone(),
            Arc::new(move |(matched_value, ctx_opt)| {
                Box::pin(capture!(
                    [patterns, values, index, results, ctx, k],
                    async move {
                        // Add this result to the results vector, keeping the context as is.
                        // (Some if matched, None if didn't match)
                        let mut updated_results = results;
                        updated_results.push((matched_value, ctx_opt));

                        // Continue to the next component regardless of match outcome.
                        match_components_sequentially(
                            patterns,
                            values,
                            index + 1,
                            ctx,
                            updated_results,
                            k,
                        )
                        .await
                    }
                ))
            }),
        )
        .await
    })
}

#[cfg(test)]
mod tests {
    use crate::analyzer::hir::UdfKind;
    use crate::engine::Engine;
    use crate::utils::tests::{
        array_decomp_pattern, array_val, bind_pattern, create_logical_operator,
        create_physical_operator, evaluate_and_collect, lit_val, literal_pattern, match_arm,
        operator_pattern, ref_expr, struct_pattern, struct_val,
    };
    use crate::{
        analyzer::{
            context::Context,
            hir::{
                BinOp, CoreData, Expr, ExprKind, FunKind, Goal, GroupId, Literal, LogicalOp,
                Materializable, Operator, Value,
            },
        },
        utils::tests::{TestHarness, int, lit_expr, pattern_match_expr, string, wildcard_pattern},
    };
    use ExprKind::*;
    use Materializable::*;
    use std::sync::Arc;

    /// Test simple pattern matching with literals
    #[tokio::test]
    async fn test_simple_literal_patterns() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a match expression: match 42 { 42 => "matched", _ => "not matched" }
        let match_expr = pattern_match_expr(
            lit_expr(int(42)),
            vec![
                match_arm(literal_pattern(int(42)), lit_expr(string("matched"))),
                match_arm(wildcard_pattern(), lit_expr(string("not matched"))),
            ],
        );

        // Evaluate the expression
        let results = evaluate_and_collect(match_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::String("matched".to_string()));
            }
            _ => panic!("Expected string literal"),
        }
    }

    /// Test binding patterns with nested patterns
    #[tokio::test]
    async fn test_bind_pattern_with_nesting() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a match expression:
        // match 42 {
        //   x @ 42 => let y = x + 10 in y,
        //   _ => 0
        // }
        let match_expr = pattern_match_expr(
            lit_expr(int(42)),
            vec![
                match_arm(
                    bind_pattern("x", literal_pattern(int(42))),
                    Arc::new(Expr::new(Let(
                        "y".to_string(),
                        Arc::new(Expr::new(Binary(
                            ref_expr("x"),
                            BinOp::Add,
                            lit_expr(int(10)),
                        ))),
                        ref_expr("y"),
                    ))),
                ),
                match_arm(wildcard_pattern(), lit_expr(int(0))),
            ],
        );

        // Evaluate the expression
        let results = evaluate_and_collect(match_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::Int64(52));
            }
            _ => panic!("Expected integer literal"),
        }
    }

    /// Test array decomposition patterns
    #[tokio::test]
    async fn test_array_decomposition() {
        let harness = TestHarness::new();

        // Create an array value [1, 2, 3, 4, 5]
        let array_expr = Arc::new(Expr::new(CoreVal(array_val(vec![
            lit_val(int(1)),
            lit_val(int(2)),
            lit_val(int(3)),
            lit_val(int(4)),
            lit_val(int(5)),
        ]))));

        // Create a match expression:
        // match [1, 2, 3, 4, 5] {
        //   [head @ 1, ...tail] => head + tail.length,
        //   _ => 0
        // }
        //
        // This should bind head to 1, tail to [2, 3, 4, 5]
        // And return 1 + 4 = 5
        let match_expr = pattern_match_expr(
            array_expr,
            vec![
                match_arm(
                    array_decomp_pattern(
                        bind_pattern("head", literal_pattern(int(1))),
                        bind_pattern("tail", wildcard_pattern()),
                    ),
                    Arc::new(Expr::new(Binary(
                        ref_expr("head"),
                        BinOp::Add,
                        Arc::new(Expr::new(Call(ref_expr("length"), vec![ref_expr("tail")]))),
                    ))),
                ),
                match_arm(wildcard_pattern(), lit_expr(int(0))),
            ],
        );

        // Add a length function to the context
        let mut ctx = Context::default();
        ctx.bind(
            "length".to_string(),
            Value::new(CoreData::Function(FunKind::Udf(UdfKind::Linked(
                |args| match &args[0].data {
                    CoreData::Array(elements) => {
                        Value::new(CoreData::Literal(int(elements.len() as i64)))
                    }
                    _ => panic!("Expected array"),
                },
            )))),
        );

        let engine = Engine::new(ctx);

        // Evaluate the expression
        let results = evaluate_and_collect(match_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::Int64(5));
            }
            _ => panic!("Expected integer literal"),
        }
    }

    /// Test struct patterns
    #[tokio::test]
    async fn test_struct_patterns() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a struct value Point { x: 10, y: 20 }
        let struct_expr = Arc::new(Expr::new(CoreVal(struct_val(
            "Point",
            vec![lit_val(int(10)), lit_val(int(20))],
        ))));

        // Create a match expression:
        // match Point { x: 10, y: 20 } {
        //   Point { x @ 10, y } => x + y,
        //   _ => 0
        // }
        let match_expr = pattern_match_expr(
            struct_expr,
            vec![
                match_arm(
                    struct_pattern(
                        "Point",
                        vec![
                            bind_pattern("x", literal_pattern(int(10))),
                            bind_pattern("y", wildcard_pattern()),
                        ],
                    ),
                    Arc::new(Expr::new(Binary(ref_expr("x"), BinOp::Add, ref_expr("y")))),
                ),
                match_arm(wildcard_pattern(), lit_expr(int(0))),
            ],
        );

        // Evaluate the expression
        let results = evaluate_and_collect(match_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::Int64(30));
            }
            _ => panic!("Expected integer literal"),
        }
    }

    /// Test complex operator pattern matching
    #[tokio::test]
    async fn test_complex_operator_patterns() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a logical operator: LogicalJoin { joinType: "inner", condition: "x = y" } [TableScan("orders"), TableScan("lineitem")]
        let op = Operator {
            tag: "LogicalJoin".to_string(),
            data: vec![lit_val(string("inner")), lit_val(string("x = y"))],
            children: vec![
                create_logical_operator("TableScan", vec![lit_val(string("orders"))], vec![]),
                create_logical_operator("TableScan", vec![lit_val(string("lineitem"))], vec![]),
            ],
        };

        let logical_op_value = Value::new(CoreData::Logical(Materialized(LogicalOp::logical(op))));
        let logical_op_expr = Arc::new(Expr::new(CoreVal(logical_op_value.clone())));

        // Create a match expression:
        // match LogicalJoin { joinType: "inner", condition } [TableScan(left), TableScan(right)] {
        //   LogicalJoin { joinType @ "inner", condition } [
        //     TableScan(left @ "orders"),
        //     TableScan(right)
        //   ] => {
        //     "Found inner join between " + left + " and " + right + " with condition: " + condition
        //   },
        //   _ => "No match"
        // }
        let match_expr = pattern_match_expr(
            logical_op_expr,
            vec![
                match_arm(
                    operator_pattern(
                        "LogicalJoin",
                        vec![
                            bind_pattern("joinType", literal_pattern(string("inner"))),
                            bind_pattern("condition", wildcard_pattern()),
                        ],
                        vec![
                            operator_pattern(
                                "TableScan",
                                vec![bind_pattern("left", literal_pattern(string("orders")))],
                                vec![],
                            ),
                            operator_pattern(
                                "TableScan",
                                vec![bind_pattern("right", wildcard_pattern())],
                                vec![],
                            ),
                        ],
                    ),
                    // Create a complex expression to concatenate strings
                    Arc::new(Expr::new(Binary(
                        Arc::new(Expr::new(Binary(
                            Arc::new(Expr::new(Binary(
                                Arc::new(Expr::new(Binary(
                                    lit_expr(string("Found inner join between ")),
                                    BinOp::Concat,
                                    ref_expr("left"),
                                ))),
                                BinOp::Concat,
                                lit_expr(string(" and ")),
                            ))),
                            BinOp::Concat,
                            ref_expr("right"),
                        ))),
                        BinOp::Concat,
                        Arc::new(Expr::new(Binary(
                            lit_expr(string(" with condition: ")),
                            BinOp::Concat,
                            ref_expr("condition"),
                        ))),
                    ))),
                ),
                match_arm(wildcard_pattern(), lit_expr(string("No match"))),
            ],
        );

        // Evaluate the expression
        let results = evaluate_and_collect(match_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(
                    lit,
                    &Literal::String(
                        "Found inner join between orders and lineitem with condition: x = y"
                            .to_string()
                    )
                );
            }
            _ => panic!("Expected string literal"),
        }
    }

    /// Test pattern matching with unmaterialized operators (requires group resolution)
    #[tokio::test]
    async fn test_unmaterialized_logical_operator_patterns() {
        let harness = TestHarness::new();
        let test_group_id = GroupId(1);

        // Register a logical operator in the test harness.
        // This is what will be returned when the UnMaterialized group is expanded
        let materialized_join = create_logical_operator(
            "LogicalJoin",
            vec![lit_val(string("inner")), lit_val(string("x = y"))],
            vec![
                create_logical_operator("TableScan", vec![lit_val(string("orders"))], vec![]),
                create_logical_operator("TableScan", vec![lit_val(string("lineitem"))], vec![]),
            ],
        );

        harness.register_group(test_group_id, materialized_join);

        // Create an unmaterialized logical operator
        let unmaterialized_logical_op =
            Value::new(CoreData::Logical(UnMaterialized(test_group_id)));

        let unmaterialized_expr = Arc::new(Expr::new(CoreVal(unmaterialized_logical_op)));

        // Create a match expression:
        // match UnMaterializedLogicalOp(group_id) {
        //   LogicalJoin { joinType @ "inner", condition } [
        //     TableScan(left @ "orders"),
        //     TableScan(right)
        //   ] => {
        //     "Found inner join between " + left + " and " + right + " with condition: " + condition
        //   },
        //   _ => "No match"
        // }
        let match_expr = pattern_match_expr(
            unmaterialized_expr,
            vec![
                match_arm(
                    operator_pattern(
                        "LogicalJoin",
                        vec![
                            bind_pattern("joinType", literal_pattern(string("inner"))),
                            bind_pattern("condition", wildcard_pattern()),
                        ],
                        vec![
                            operator_pattern(
                                "TableScan",
                                vec![bind_pattern("left", literal_pattern(string("orders")))],
                                vec![],
                            ),
                            operator_pattern(
                                "TableScan",
                                vec![bind_pattern("right", wildcard_pattern())],
                                vec![],
                            ),
                        ],
                    ),
                    // Create a complex expression to concatenate strings
                    Arc::new(Expr::new(Binary(
                        Arc::new(Expr::new(Binary(
                            Arc::new(Expr::new(Binary(
                                Arc::new(Expr::new(Binary(
                                    lit_expr(string("Resolved group: found inner join between ")),
                                    BinOp::Concat,
                                    ref_expr("left"),
                                ))),
                                BinOp::Concat,
                                lit_expr(string(" and ")),
                            ))),
                            BinOp::Concat,
                            ref_expr("right"),
                        ))),
                        BinOp::Concat,
                        Arc::new(Expr::new(Binary(
                            lit_expr(string(" with condition: ")),
                            BinOp::Concat,
                            ref_expr("condition"),
                        ))),
                    ))),
                ),
                match_arm(wildcard_pattern(), lit_expr(string("No match"))),
            ],
        );

        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Evaluate the expression
        let results = evaluate_and_collect(match_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(
                    lit,
                    &Literal::String(
                        "Resolved group: found inner join between orders and lineitem with condition: x = y".to_string()
                    )
                );
            }
            _ => panic!("Expected string literal"),
        }
    }

    /// Test pattern matching with unmaterialized physical operators (requires goal resolution)
    #[tokio::test]
    async fn test_unmaterialized_physical_operator_patterns() {
        let harness = TestHarness::new();

        // Create a physical goal
        let test_group_id = GroupId(2);
        let properties = Box::new(Value::new(CoreData::Literal(string("sorted"))));
        let test_goal = Goal {
            group_id: test_group_id,
            properties,
        };

        // Register a physical operator to be returned when the goal is expanded
        let materialized_hash_join = create_physical_operator(
            "HashJoin",
            vec![lit_val(string("hash")), lit_val(string("id = id"))],
            vec![
                create_physical_operator("IndexScan", vec![lit_val(string("customers"))], vec![]),
                create_physical_operator("ParallelScan", vec![lit_val(string("orders"))], vec![]),
            ],
        );

        harness.register_goal(&test_goal, materialized_hash_join);

        // Create an unmaterialized physical operator with the goal
        let unmaterialized_physical_op = Value::new(CoreData::Physical(UnMaterialized(test_goal)));

        let unmaterialized_expr = Arc::new(Expr::new(CoreVal(unmaterialized_physical_op)));

        // Create a match expression to match against the expanded physical operator
        let match_expr = pattern_match_expr(
            unmaterialized_expr,
            vec![
                match_arm(
                    operator_pattern(
                        "HashJoin",
                        vec![
                            bind_pattern("join_method", literal_pattern(string("hash"))),
                            bind_pattern("join_condition", wildcard_pattern()),
                        ],
                        vec![
                            operator_pattern(
                                "IndexScan",
                                vec![bind_pattern("left_table", wildcard_pattern())],
                                vec![],
                            ),
                            operator_pattern(
                                "ParallelScan",
                                vec![bind_pattern("right_table", wildcard_pattern())],
                                vec![],
                            ),
                        ],
                    ),
                    // Create formatted result string with binding values
                    Arc::new(Expr::new(Let(
                        "to_string".to_string(),
                        Arc::new(Expr::new(CoreVal(Value::new(CoreData::Function(
                            FunKind::Udf(UdfKind::Linked(|args| match &args[0].data {
                                CoreData::Literal(lit) => {
                                    Value::new(CoreData::Literal(string(&format!("{:?}", lit))))
                                }
                                _ => Value::new(CoreData::Literal(string("<non-literal>"))),
                            })),
                        ))))),
                        Arc::new(Expr::new(Binary(
                            lit_expr(string("Physical plan: ")),
                            BinOp::Concat,
                            Arc::new(Expr::new(Binary(
                                Arc::new(Expr::new(Call(
                                    ref_expr("to_string"),
                                    vec![ref_expr("join_method")],
                                ))),
                                BinOp::Concat,
                                Arc::new(Expr::new(Binary(
                                    lit_expr(string(" join between ")),
                                    BinOp::Concat,
                                    Arc::new(Expr::new(Binary(
                                        ref_expr("left_table"),
                                        BinOp::Concat,
                                        Arc::new(Expr::new(Binary(
                                            lit_expr(string(" and ")),
                                            BinOp::Concat,
                                            Arc::new(Expr::new(Binary(
                                                ref_expr("right_table"),
                                                BinOp::Concat,
                                                Arc::new(Expr::new(Binary(
                                                    lit_expr(string(" with condition: ")),
                                                    BinOp::Concat,
                                                    ref_expr("join_condition"),
                                                ))),
                                            ))),
                                        ))),
                                    ))),
                                ))),
                            ))),
                        ))),
                    ))),
                ),
                match_arm(wildcard_pattern(), lit_expr(string("No match"))),
            ],
        );

        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Evaluate the expression
        let results = evaluate_and_collect(match_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                match lit {
                    Literal::String(s) => {
                        // Check that the string contains all our expected components
                        assert!(s.contains("Physical plan:"));
                        assert!(s.contains("hash"));
                        assert!(s.contains("join between"));
                        assert!(s.contains("customers"));
                        assert!(s.contains("orders"));
                        assert!(s.contains("with condition:"));
                        assert!(s.contains("id = id"));
                    }
                    _ => panic!("Expected string result"),
                }
            }
            _ => panic!("Expected string literal"),
        }
    }

    /// Test multiple pattern matching arms with fallthrough
    #[tokio::test]
    async fn test_multiple_match_arms_with_fallthrough() {
        let harness = TestHarness::new();
        let mut ctx = Context::default();

        // Add to_string function to convert numbers to strings
        ctx.bind(
            "to_string".to_string(),
            Value::new(CoreData::Function(FunKind::Udf(UdfKind::Linked(
                |args| match &args[0].data {
                    CoreData::Literal(Literal::Int64(i)) => {
                        Value::new(CoreData::Literal(string(&i.to_string())))
                    }
                    _ => panic!("Expected integer literal"),
                },
            )))),
        );

        let engine = Engine::new(ctx);

        // Create a struct value Point { x: 30, y: 40 }
        let struct_expr = Arc::new(Expr::new(CoreVal(struct_val(
            "Point",
            vec![lit_val(int(30)), lit_val(int(40))],
        ))));

        // Create a match expression with multiple arms and fallthrough:
        // match Point { x: 30, y: 40 } {
        //   Point { x @ 10, y } => "First arm: " + to_string(x) + ", " + to_string(y),
        //   Point { x @ 20, y } => "Second arm: " + to_string(x) + ", " + to_string(y),
        //   Point { x, y } => "Fallthrough arm: " + to_string(x) + ", " + to_string(y),
        //   _ => "Default"
        // }
        let match_expr = pattern_match_expr(
            struct_expr,
            vec![
                // First arm - won't match
                match_arm(
                    struct_pattern(
                        "Point",
                        vec![
                            bind_pattern("x", literal_pattern(int(10))),
                            bind_pattern("y", wildcard_pattern()),
                        ],
                    ),
                    Arc::new(Expr::new(Binary(
                        lit_expr(string("First arm: ")),
                        BinOp::Concat,
                        Arc::new(Expr::new(Binary(
                            Arc::new(Expr::new(Call(ref_expr("to_string"), vec![ref_expr("x")]))),
                            BinOp::Concat,
                            Arc::new(Expr::new(Binary(
                                lit_expr(string(", ")),
                                BinOp::Concat,
                                Arc::new(Expr::new(Call(
                                    ref_expr("to_string"),
                                    vec![ref_expr("y")],
                                ))),
                            ))),
                        ))),
                    ))),
                ),
                // Second arm - won't match
                match_arm(
                    struct_pattern(
                        "Point",
                        vec![
                            bind_pattern("x", literal_pattern(int(20))),
                            bind_pattern("y", wildcard_pattern()),
                        ],
                    ),
                    Arc::new(Expr::new(Binary(
                        lit_expr(string("Second arm: ")),
                        BinOp::Concat,
                        Arc::new(Expr::new(Binary(
                            Arc::new(Expr::new(Call(ref_expr("to_string"), vec![ref_expr("x")]))),
                            BinOp::Concat,
                            Arc::new(Expr::new(Binary(
                                lit_expr(string(", ")),
                                BinOp::Concat,
                                Arc::new(Expr::new(Call(
                                    ref_expr("to_string"),
                                    vec![ref_expr("y")],
                                ))),
                            ))),
                        ))),
                    ))),
                ),
                // Fallthrough arm - should match
                match_arm(
                    struct_pattern(
                        "Point",
                        vec![
                            bind_pattern("x", wildcard_pattern()),
                            bind_pattern("y", wildcard_pattern()),
                        ],
                    ),
                    Arc::new(Expr::new(Binary(
                        lit_expr(string("Fallthrough arm: ")),
                        BinOp::Concat,
                        Arc::new(Expr::new(Binary(
                            Arc::new(Expr::new(Call(ref_expr("to_string"), vec![ref_expr("x")]))),
                            BinOp::Concat,
                            Arc::new(Expr::new(Binary(
                                lit_expr(string(", ")),
                                BinOp::Concat,
                                Arc::new(Expr::new(Call(
                                    ref_expr("to_string"),
                                    vec![ref_expr("y")],
                                ))),
                            ))),
                        ))),
                    ))),
                ),
                // Default arm - won't reach
                match_arm(wildcard_pattern(), lit_expr(string("Default"))),
            ],
        );

        // Evaluate the expression
        let results = evaluate_and_collect(match_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::String("Fallthrough arm: 30, 40".to_string()));
            }
            _ => panic!("Expected string literal"),
        }
    }

    /// Test pattern matching with deeply nested bind patterns
    #[tokio::test]
    async fn test_deeply_nested_bind_patterns() {
        let harness = TestHarness::new();
        let mut ctx = Context::default();

        // Add to_string function to convert complex values to strings
        ctx.bind(
            "to_string".to_string(),
            Value::new(CoreData::Function(FunKind::Udf(UdfKind::Linked(
                |args| match &args[0].data {
                    CoreData::Literal(lit) => {
                        Value::new(CoreData::Literal(string(&format!("{:?}", lit))))
                    }
                    CoreData::Array(_) => Value::new(CoreData::Literal(string("<array>"))),
                    _ => Value::new(CoreData::Literal(string("<unknown>"))),
                },
            )))),
        );

        let engine = Engine::new(ctx);

        // Create a deeply nested operator:
        // Project [col1, col2] (
        //   Filter (predicate) (
        //     Join (joinType, condition) (
        //       TableScan (table1),
        //       TableScan (table2)
        //     )
        //   )
        // )
        let nested_op = create_logical_operator(
            "Project",
            vec![array_val(vec![
                lit_val(string("col1")),
                lit_val(string("col2")),
            ])],
            vec![create_logical_operator(
                "Filter",
                vec![lit_val(string("age > 30"))],
                vec![create_logical_operator(
                    "Join",
                    vec![lit_val(string("inner")), lit_val(string("t1.id = t2.id"))],
                    vec![
                        create_logical_operator(
                            "TableScan",
                            vec![lit_val(string("employees"))],
                            vec![],
                        ),
                        create_logical_operator(
                            "TableScan",
                            vec![lit_val(string("departments"))],
                            vec![],
                        ),
                    ],
                )],
            )],
        );

        let nested_op_expr = Arc::new(Expr::new(CoreVal(nested_op)));

        // Create a deeply nested pattern match that extracts values at various depths:
        let match_expr = pattern_match_expr(
            nested_op_expr,
            vec![
                match_arm(
                    operator_pattern(
                        "Project",
                        vec![bind_pattern("cols", wildcard_pattern())],
                        vec![operator_pattern(
                            "Filter",
                            vec![bind_pattern("pred", wildcard_pattern())],
                            vec![operator_pattern(
                                "Join",
                                vec![
                                    bind_pattern("jtype", wildcard_pattern()),
                                    bind_pattern("jcond", wildcard_pattern()),
                                ],
                                vec![
                                    operator_pattern(
                                        "TableScan",
                                        vec![bind_pattern("t1", wildcard_pattern())],
                                        vec![],
                                    ),
                                    operator_pattern(
                                        "TableScan",
                                        vec![bind_pattern("t2", wildcard_pattern())],
                                        vec![],
                                    ),
                                ],
                            )],
                        )],
                    ),
                    // Build a SQL-like string with to_string calls
                    Arc::new(Expr::new(Binary(
                        lit_expr(string("Query: SELECT ")),
                        BinOp::Concat,
                        Arc::new(Expr::new(Binary(
                            Arc::new(Expr::new(Call(
                                ref_expr("to_string"),
                                vec![ref_expr("cols")],
                            ))),
                            BinOp::Concat,
                            Arc::new(Expr::new(Binary(
                                lit_expr(string(" FROM ")),
                                BinOp::Concat,
                                Arc::new(Expr::new(Binary(
                                    Arc::new(Expr::new(Call(
                                        ref_expr("to_string"),
                                        vec![ref_expr("t1")],
                                    ))),
                                    BinOp::Concat,
                                    Arc::new(Expr::new(Binary(
                                        lit_expr(string(" JOIN ")),
                                        BinOp::Concat,
                                        Arc::new(Expr::new(Binary(
                                            Arc::new(Expr::new(Call(
                                                ref_expr("to_string"),
                                                vec![ref_expr("t2")],
                                            ))),
                                            BinOp::Concat,
                                            Arc::new(Expr::new(Binary(
                                                lit_expr(string(" ON ")),
                                                BinOp::Concat,
                                                Arc::new(Expr::new(Binary(
                                                    ref_expr("jcond"),
                                                    BinOp::Concat,
                                                    Arc::new(Expr::new(Binary(
                                                        lit_expr(string(" WHERE ")),
                                                        BinOp::Concat,
                                                        ref_expr("pred"),
                                                    ))),
                                                ))),
                                            ))),
                                        ))),
                                    ))),
                                ))),
                            ))),
                        ))),
                    ))),
                ),
                match_arm(wildcard_pattern(), lit_expr(string("No match"))),
            ],
        );

        // Evaluate the expression
        let results = evaluate_and_collect(match_expr, engine, harness).await;

        // Check result (this is a complex test of correct binding propagation through deeply nested patterns)
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                assert!(matches!(lit, Literal::String(_)));
                let result_str = match lit {
                    Literal::String(s) => s,
                    _ => unreachable!(),
                };

                // Check for the presence of key components
                assert!(result_str.contains("SELECT"));
                assert!(result_str.contains("FROM"));
                assert!(result_str.contains("employees"));
                assert!(result_str.contains("departments"));
                assert!(result_str.contains("JOIN"));
                assert!(result_str.contains("t1.id = t2.id"));
                assert!(result_str.contains("WHERE"));
                assert!(result_str.contains("age > 30"));
            }
            _ => panic!("Expected string literal"),
        }
    }

    /// Test combinatorial explosion with multiple expansion paths
    #[tokio::test]
    async fn test_combinatorial_explosion() {
        let harness = TestHarness::new();
        let group_id_1 = GroupId(1);
        let group_id_2 = GroupId(2);

        // Register multiple alternatives for each group to create a combinatorial explosion
        // For group 1, register 3 different possible values
        harness.register_group(
            group_id_1,
            create_logical_operator("TableScan", vec![lit_val(string("employees"))], vec![]),
        );
        harness.register_group(
            group_id_1,
            create_logical_operator("TableScan", vec![lit_val(string("departments"))], vec![]),
        );
        harness.register_group(
            group_id_1,
            create_logical_operator("TableScan", vec![lit_val(string("customers"))], vec![]),
        );
        harness.register_group(
            group_id_1,
            create_logical_operator("Filter", vec![lit_val(string("age > 30"))], vec![]),
        );

        // For group 2, register 2 different possible values
        harness.register_group(
            group_id_2,
            create_logical_operator("Filter", vec![lit_val(string("age > 30"))], vec![]),
        );
        harness.register_group(
            group_id_2,
            create_logical_operator("Filter", vec![lit_val(string("salary > 50000"))], vec![]),
        );

        // Create a join with two unmaterialized children
        let join_op = create_logical_operator(
            "Join",
            vec![
                lit_val(string("inner")),
                lit_val(string("left.id = right.id")),
            ],
            vec![
                Value::new(CoreData::Logical(UnMaterialized(group_id_1))),
                Value::new(CoreData::Logical(UnMaterialized(group_id_2))),
            ],
        );

        let join_expr = Arc::new(Expr::new(CoreVal(join_op)));

        // Create a pattern match that will match any join with a table scan as first child
        // and extract information from it
        let match_expr = pattern_match_expr(
            join_expr,
            vec![
                match_arm(
                    operator_pattern(
                        "Join",
                        vec![
                            bind_pattern("join_type", wildcard_pattern()),
                            bind_pattern("join_condition", wildcard_pattern()),
                        ],
                        vec![
                            operator_pattern(
                                "TableScan",
                                vec![bind_pattern("table_name", wildcard_pattern())],
                                vec![],
                            ),
                            operator_pattern(
                                "Filter",
                                vec![bind_pattern("filter_condition", wildcard_pattern())],
                                vec![],
                            ),
                        ],
                    ),
                    Arc::new(Expr::new(Let(
                        "result".to_string(),
                        Arc::new(Expr::new(Binary(
                            ref_expr("table_name"),
                            BinOp::Concat,
                            Arc::new(Expr::new(Binary(
                                lit_expr(string(" filtered by ")),
                                BinOp::Concat,
                                ref_expr("filter_condition"),
                            ))),
                        ))),
                        ref_expr("result"),
                    ))),
                ),
                match_arm(wildcard_pattern(), lit_expr(string("No match"))),
            ],
        );

        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Evaluate the expression
        let results = evaluate_and_collect(match_expr, engine, harness).await;

        // We should get 4 × 2 = 8 different results from the combinatorial explosion
        assert_eq!(results.len(), 8);

        // Check that we got all the expected combinations
        let expected_combinations = [
            ("employees filtered by age > 30", 1),
            ("employees filtered by salary > 50000", 1),
            ("departments filtered by age > 30", 1),
            ("departments filtered by salary > 50000", 1),
            ("customers filtered by age > 30", 1),
            ("customers filtered by salary > 50000", 1),
            ("No match", 2),
        ];

        for result in &results {
            match &result.data {
                CoreData::Literal(Literal::String(s)) => {
                    assert!(
                        expected_combinations
                            .map(|(expected, _)| expected)
                            .contains(&s.as_str())
                    );
                }
                _ => panic!("Expected string literal"),
            }
        }

        // Ensure we got each combination exactly once (no duplicates)
        let mut result_strings = Vec::new();
        for result in &results {
            if let CoreData::Literal(Literal::String(s)) = &result.data {
                result_strings.push(s.clone());
            }
        }

        for (expected, occ) in expected_combinations {
            assert_eq!(
                result_strings
                    .iter()
                    .filter(|s| s == &&expected.to_string())
                    .count(),
                occ,
                "Expected combination '{}' should appear exactly once",
                expected
            );
        }
    }
}
