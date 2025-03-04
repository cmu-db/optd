//! This module provides implementation of expression evaluation, handling different
//! expression types and evaluation strategies in a non-blocking, streaming manner.

use super::{
    binary::eval_binary_op,
    core::evaluate_core_expr,
    r#match::{expand_top_level, try_match_arms},
    unary::eval_unary_op,
    Expander,
};
use crate::{
    capture,
    engine::{
        utils::streams::{
            evaluate_all_combinations, propagate_success, stream_from_result, ValueStream,
        },
        Engine, Evaluate,
    },
};
use futures::{stream, StreamExt};
use optd_dsl::analyzer::hir::{BinOp, CoreData, Expr, FunKind, Literal, MatchArm, UnaryOp, Value};
use std::sync::Arc;
use CoreData::*;
use Expr::*;
use FunKind::*;

impl Evaluate for Arc<Expr> {
    /// Evaluates an expression to a stream of possible values.
    ///
    /// This function takes a reference to the expression, dispatching to specialized
    /// handlers for each expression type.
    ///
    /// # Parameters
    /// * `self` - Reference to the expression to evaluate
    /// * `engine` - The evaluation engine with an expander implementation
    ///
    /// # Returns
    /// A stream of all possible evaluation results
    fn evaluate<E>(self, engine: Engine<E>) -> ValueStream
    where
        E: Expander,
    {
        match &*self {
            PatternMatch(expr, match_arms) => {
                evaluate_pattern_match(expr.clone(), match_arms.clone(), engine)
            }
            IfThenElse(cond, then_expr, else_expr) => {
                evaluate_if_then_else(cond.clone(), then_expr.clone(), else_expr.clone(), engine)
            }
            Let(ident, assignee, after) => {
                evaluate_let_binding(ident.clone(), assignee.clone(), after.clone(), engine)
            }
            Binary(left, op, right) => {
                evaluate_binary_expr(left.clone(), op.clone(), right.clone(), engine)
            }
            Unary(op, expr) => evaluate_unary_expr(op.clone(), expr.clone(), engine),
            Call(fun, args) => evaluate_function_call(fun.clone(), args.clone(), engine),
            Ref(ident) => evaluate_reference(ident.clone(), engine),
            CoreExpr(expr) => evaluate_core_expr(expr.clone(), engine),
            CoreVal(val) => propagate_success(val.clone()).boxed(),
        }
    }
}

/// Evaluates a pattern match expression.
///
/// First evaluates the expression to match, then tries each match arm in order
/// until a pattern matches.
///
/// # Parameters
/// * `expr` - The expression to match against patterns
/// * `match_arms` - The list of pattern-expression pairs to try
/// * `engine` - The evaluation engine
///
/// # Returns
/// A stream of all possible evaluation results
fn evaluate_pattern_match<E>(
    expr: Arc<Expr>,
    match_arms: Vec<MatchArm>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    // First evaluate the expression
    expr.evaluate(engine.clone())
        .flat_map(move |expr_result| {
            stream_from_result(
                expr_result,
                capture!([engine, match_arms], move |value| {
                    let expansion_future = expand_top_level(value, engine.clone());
                    stream::once(expansion_future)
                        .flat_map(move |expanded_values| {
                            // For each expanded value, try all match arms
                            stream::iter(expanded_values).flat_map(capture!(
                                [match_arms, engine],
                                move |expanded_value| {
                                    try_match_arms(
                                        expanded_value,
                                        match_arms.clone(),
                                        engine.clone(),
                                    )
                                }
                            ))
                        })
                        .boxed()
                }),
            )
        })
        .boxed()
}
/// Evaluates an if-then-else expression.
///
/// First evaluates the condition, then either the 'then' branch if the condition is true,
/// or the 'else' branch if the condition is false.
fn evaluate_if_then_else<E>(
    cond: Arc<Expr>,
    then_expr: Arc<Expr>,
    else_expr: Arc<Expr>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    cond.evaluate(engine.clone())
        .flat_map(move |cond_result| {
            stream_from_result(
                cond_result,
                capture!([engine, then_expr, else_expr], move |value| {
                    match value.0 {
                        // If condition is a boolean, evaluate the appropriate branch
                        Literal(Literal::Bool(b)) => {
                            if b {
                                then_expr.evaluate(engine)
                            } else {
                                else_expr.evaluate(engine)
                            }
                        }
                        // Condition must be a boolean
                        _ => panic!("Expected boolean in condition"),
                    }
                }),
            )
        })
        .boxed()
}

/// Evaluates a let binding expression.
///
/// Binds the result of evaluating the assignee to the identifier in the context,
/// then evaluates the 'after' expression in the updated context.
fn evaluate_let_binding<E>(
    ident: String,
    assignee: Arc<Expr>,
    after: Arc<Expr>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    assignee
        .evaluate(engine.clone())
        .flat_map(move |expr_result| {
            stream_from_result(
                expr_result,
                capture!([engine, after, ident], move |value| {
                    // Create updated context with the new binding
                    let mut new_ctx = engine.context.clone();
                    new_ctx.bind(ident, value);
                    after.evaluate(engine.with_context(new_ctx))
                }),
            )
        })
        .boxed()
}

/// Evaluates a binary expression.
///
/// Evaluates both operands in all possible combinations, then applies the binary operation.
fn evaluate_binary_expr<E>(
    left: Arc<Expr>,
    op: BinOp,
    right: Arc<Expr>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    let exprs = vec![left, right];
    evaluate_all_combinations(exprs.into_iter(), engine)
        .map(move |combo_result| {
            combo_result.map(|mut values| {
                let right_val = values.pop().expect("Right operand not found");
                let left_val = values.pop().expect("Left operand not found");
                eval_binary_op(left_val, &op, right_val)
            })
        })
        .boxed()
}

/// Evaluates a unary expression.
///
/// Evaluates the operand, then applies the unary operation.
fn evaluate_unary_expr<E>(op: UnaryOp, expr: Arc<Expr>, engine: Engine<E>) -> ValueStream
where
    E: Expander,
{
    expr.evaluate(engine)
        .map(move |expr_result| expr_result.map(|value| eval_unary_op(&op, value)))
        .boxed()
}

/// Evaluates a function call expression.
///
/// First evaluates the function expression, then the arguments,
/// and finally applies the function to the arguments.
fn evaluate_function_call<E>(fun: Arc<Expr>, args: Vec<Arc<Expr>>, engine: Engine<E>) -> ValueStream
where
    E: Expander,
{
    let fun_stream = fun.evaluate(engine.clone());

    fun_stream
        .flat_map(move |fun_result| {
            stream_from_result(
                fun_result,
                capture!([engine, args], move |fun_value| {
                    match fun_value.0 {
                        // Handle closure (user-defined function)
                        Function(Closure(params, body)) => {
                            evaluate_closure_call(params, body, args.clone(), engine)
                        }
                        // Handle Rust UDF (built-in function)
                        Function(RustUDF(udf)) => evaluate_rust_udf_call(udf, args, engine),
                        // Value must be a function
                        _ => panic!("Expected function value"),
                    }
                }),
            )
        })
        .boxed()
}

/// Evaluates a call to a closure (user-defined function).
///
/// Evaluates the arguments, binds them to the parameters in a new context,
/// then evaluates the function body in that context.
fn evaluate_closure_call<E>(
    params: Vec<String>,
    body: Arc<Expr>,
    args: Vec<Arc<Expr>>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    evaluate_all_combinations(args.into_iter(), engine.clone())
        .flat_map(move |args_result| {
            stream_from_result(
                args_result,
                capture!([engine, params, body], move |args| {
                    // Create a new context with parameters bound to arguments
                    let mut new_ctx = engine.context.clone();
                    new_ctx.push_scope();
                    params.iter().zip(args).for_each(|(p, a)| {
                        new_ctx.bind(p.clone(), a);
                    });
                    body.evaluate(engine.with_context(new_ctx))
                }),
            )
        })
        .boxed()
}

/// Evaluates a call to a Rust UDF (built-in function).
///
/// Evaluates the arguments, then calls the Rust function with those arguments.
fn evaluate_rust_udf_call<E>(
    udf: fn(Vec<Value>) -> Value,
    args: Vec<Arc<Expr>>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    evaluate_all_combinations(args.into_iter(), engine)
        .map(move |args_result| args_result.map(udf))
        .boxed()
}

/// Evaluates a reference to a variable.
///
/// Looks up the variable in the context and returns its value.
fn evaluate_reference<E>(ident: String, engine: Engine<E>) -> ValueStream
where
    E: Expander,
{
    propagate_success(
        engine
            .context
            .lookup(&ident)
            .expect("Variable not found")
            .clone(),
    )
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        utils::tests::{arc, bool_val, collect_stream_values, int_val, string_val, MockExpander},
        Context, Engine,
    };
    use optd_dsl::analyzer::hir::{
        BinOp, CoreData, GroupId, Literal, LogicalOp, MatchArm, Materializable, Operator,
        OperatorKind, Pattern, ScalarOp, Value,
    };
    use Literal::*;
    use Materializable::*;
    use Pattern::*;

    // Test pattern matching with expansion at the top level
    #[test]
    fn test_pattern_match_with_expansion() {
        // Create a MockExpander that provides multiple implementations for a logical group
        let expander = MockExpander::new(
            |group_id| {
                if group_id == GroupId(1) {
                    // Return two different filter operators
                    let filter_op = Operator {
                        tag: "Filter".to_string(),
                        kind: OperatorKind::Logical,
                        operator_data: vec![bool_val(true)],
                        scalar_children: vec![],
                        relational_children: vec![],
                    };

                    let project_op = Operator {
                        tag: "Project".to_string(),
                        kind: OperatorKind::Logical,
                        operator_data: vec![int_val(42)],
                        scalar_children: vec![],
                        relational_children: vec![],
                    };

                    vec![
                        Value(Logical(LogicalOp(Materialized(filter_op)))),
                        Value(Logical(LogicalOp(Materialized(project_op)))),
                    ]
                } else {
                    vec![]
                }
            },
            |_| vec![],
            |_| panic!("Physical expansion not expected"),
        );

        let engine = Engine::new(Context::default(), expander);

        // Create a logical group reference value
        let logical_group_ref = Value(Logical(LogicalOp(UnMaterialized(GroupId(1)))));

        // Create a pattern match expression that matches against different operators
        let pattern_match_expr = arc(PatternMatch(
            arc(CoreVal(logical_group_ref)),
            vec![
                // First arm: match Filter operator
                MatchArm {
                    pattern: Pattern::Operator(Operator {
                        tag: "Filter".to_string(),
                        kind: OperatorKind::Logical,
                        operator_data: vec![Pattern::Literal(Bool(true))],
                        scalar_children: vec![],
                        relational_children: vec![],
                    }),
                    expr: arc(CoreVal(string_val("is_filter"))),
                },
                // Second arm: match Project operator
                MatchArm {
                    pattern: Pattern::Operator(Operator {
                        tag: "Project".to_string(),
                        kind: OperatorKind::Logical,
                        operator_data: vec![Bind("column".to_string(), Box::new(Wildcard))],
                        scalar_children: vec![],
                        relational_children: vec![],
                    }),
                    expr: arc(Ref("column".to_string())),
                },
                // Fallback arm
                MatchArm {
                    pattern: Wildcard,
                    expr: arc(CoreVal(string_val("unknown"))),
                },
            ],
        ));

        // Evaluate the pattern match expression
        let values = collect_stream_values(pattern_match_expr.evaluate(engine));

        // Should get one result for each expansion (one for Filter, one for Project)
        assert_eq!(values.len(), 2);

        // Check results
        let result_strings: Vec<std::string::String> = values
            .iter()
            .map(|v| {
                if let CoreData::Literal(String(s)) = &v.0 {
                    s.clone()
                } else if let CoreData::Literal(Int64(i)) = &v.0 {
                    i.to_string()
                } else {
                    format!("{:?}", v)
                }
            })
            .collect();

        // Results should include "is_filter" (from first arm) and "42" (from second arm)
        assert!(result_strings.contains(&"is_filter".to_string()));
        assert!(result_strings.contains(&"42".to_string()));
    }

    // Test pattern matching for list length calculation with expansion
    #[test]
    fn test_list_length_with_expansion() {
        // Create a MockExpander that provides multiple list implementations
        let expander = MockExpander::new(
            |_| vec![],
            |group_id| {
                if group_id == GroupId(4) {
                    // Return two different lists
                    vec![
                        Value(CoreData::Array(vec![int_val(1), int_val(2), int_val(3)])),
                        Value(CoreData::Array(vec![int_val(10), int_val(20)])),
                    ]
                } else {
                    vec![]
                }
            },
            |_| panic!("Physical expansion not expected"),
        );

        // Create a scalar group reference to the lists
        let scalar_group_ref = Value(Scalar(ScalarOp(UnMaterialized(GroupId(4)))));

        // Create recursive length calculation function
        // length([]) = 0
        // length([x .. xs]) = 1 + length(xs)
        let length_fn = {
            // Create the pattern matching expression for length calculation
            let length_expr = arc(PatternMatch(
                arc(Ref("list".to_string())),
                vec![
                    // Base case: empty list
                    MatchArm {
                        pattern: EmptyArray,
                        expr: arc(CoreVal(int_val(0))),
                    },
                    // Recursive case: head :: tail
                    MatchArm {
                        pattern: ArrayDecomp(
                            Box::new(Wildcard),                                     // head (ignored)
                            Box::new(Bind("rest".to_string(), Box::new(Wildcard))), // tail
                        ),
                        expr: arc(Binary(
                            arc(CoreVal(int_val(1))),
                            BinOp::Add,
                            arc(Call(
                                arc(Ref("length".to_string())),
                                vec![arc(Ref("rest".to_string()))],
                            )),
                        )),
                    },
                ],
            ));

            // Create a closure for the length function
            Value(Function(Closure(vec!["list".to_string()], length_expr)))
        };

        // Create a context with the length function bound
        let mut ctx = Context::default();
        ctx.bind("length".to_string(), length_fn);
        let engine_with_fn = Engine::new(ctx, expander);

        // Call the length function on the scalar group reference
        let call_expr = arc(Call(
            arc(Ref("length".to_string())),
            vec![arc(CoreVal(scalar_group_ref))],
        ));

        // Evaluate the call
        let values = collect_stream_values(call_expr.evaluate(engine_with_fn));

        // Should get two results (one for each expansion of the list)
        assert_eq!(values.len(), 2);

        // Results should include length 3 (for first list) and length 2 (for second list)
        let result_ints: Vec<i64> = values
            .iter()
            .map(|v| {
                if let CoreData::Literal(Int64(i)) = &v.0 {
                    *i
                } else {
                    panic!("Expected integer result, got {:?}", v)
                }
            })
            .collect();

        assert!(result_ints.contains(&3)); // Length of [1, 2, 3]
        assert!(result_ints.contains(&2)); // Length of [10, 20]
    }

    // Test multiple nested pattern matches with expansion
    #[test]
    fn test_nested_pattern_matches_with_expansion() {
        // Create a MockExpander that provides multiple implementations for different groups
        let expander = MockExpander::new(
            |group_id| {
                if group_id == GroupId(5) {
                    // Return two different filter operators
                    let filter_true = Operator {
                        tag: "Filter".to_string(),
                        kind: OperatorKind::Logical,
                        operator_data: vec![bool_val(true)],
                        scalar_children: vec![],
                        relational_children: vec![],
                    };

                    let filter_false = Operator {
                        tag: "Filter".to_string(),
                        kind: OperatorKind::Logical,
                        operator_data: vec![bool_val(false)],
                        scalar_children: vec![],
                        relational_children: vec![],
                    };

                    vec![
                        Value(Logical(LogicalOp(Materialized(filter_true)))),
                        Value(Logical(LogicalOp(Materialized(filter_false)))),
                    ]
                } else {
                    vec![]
                }
            },
            |group_id| {
                if group_id == GroupId(6) {
                    // Return two different lists
                    vec![
                        Value(CoreData::Array(vec![int_val(1), int_val(2)])),
                        Value(CoreData::Array(vec![int_val(3), int_val(4), int_val(5)])),
                    ]
                } else {
                    vec![]
                }
            },
            |_| panic!("Physical expansion not expected"),
        );

        let engine = Engine::new(Context::default(), expander);

        // Create references to groups
        let logical_group_ref = Value(Logical(LogicalOp(UnMaterialized(GroupId(5)))));
        let scalar_group_ref = Value(Scalar(ScalarOp(UnMaterialized(GroupId(6)))));

        // Create a nested pattern match:
        // match <logical_group5> {
        //   Filter(true) => match <scalar_group6> {
        //     [x, y] => x + y
        //     [x, y, z] => x + y + z
        //     _ => 0
        //   }
        //   Filter(false) => match <scalar_group6> {
        //     [x, y] => x * y
        //     [x, y, z] => x * y * z
        //     _ => 0
        //   }
        //   _ => 0
        // }
        let pattern_match_expr = arc(PatternMatch(
            arc(CoreVal(logical_group_ref)),
            vec![
                // First arm: Filter(true)
                MatchArm {
                    pattern: Pattern::Operator(Operator {
                        tag: "Filter".to_string(),
                        kind: OperatorKind::Logical,
                        operator_data: vec![Pattern::Literal(Bool(true))],
                        scalar_children: vec![],
                        relational_children: vec![],
                    }),
                    // Nested pattern match for addition
                    expr: arc(PatternMatch(
                        arc(CoreVal(scalar_group_ref.clone())),
                        vec![
                            // Pattern for list with exactly 2 elements
                            MatchArm {
                                pattern: Pattern::Struct(
                                    "Array".to_string(),
                                    vec![
                                        Bind("x".to_string(), Box::new(Wildcard)),
                                        Bind("y".to_string(), Box::new(Wildcard)),
                                    ],
                                ),
                                expr: arc(Binary(
                                    arc(Ref("x".to_string())),
                                    BinOp::Add,
                                    arc(Ref("y".to_string())),
                                )),
                            },
                            // Pattern for list with exactly 3 elements
                            MatchArm {
                                pattern: Pattern::Struct(
                                    "Array".to_string(),
                                    vec![
                                        Bind("x".to_string(), Box::new(Wildcard)),
                                        Bind("y".to_string(), Box::new(Wildcard)),
                                        Bind("z".to_string(), Box::new(Wildcard)),
                                    ],
                                ),
                                expr: arc(Binary(
                                    arc(Binary(
                                        arc(Ref("x".to_string())),
                                        BinOp::Add,
                                        arc(Ref("y".to_string())),
                                    )),
                                    BinOp::Add,
                                    arc(Ref("z".to_string())),
                                )),
                            },
                            // Fallback
                            MatchArm {
                                pattern: Wildcard,
                                expr: arc(CoreVal(int_val(0))),
                            },
                        ],
                    )),
                },
                // Second arm: Filter(false)
                MatchArm {
                    pattern: Pattern::Operator(Operator {
                        tag: "Filter".to_string(),
                        kind: OperatorKind::Logical,
                        operator_data: vec![Pattern::Literal(Bool(false))],
                        scalar_children: vec![],
                        relational_children: vec![],
                    }),
                    // Nested pattern match for multiplication
                    expr: arc(PatternMatch(
                        arc(CoreVal(scalar_group_ref)),
                        vec![
                            // Pattern for list with exactly 2 elements
                            MatchArm {
                                pattern: Pattern::Struct(
                                    "Array".to_string(),
                                    vec![
                                        Bind("x".to_string(), Box::new(Wildcard)),
                                        Bind("y".to_string(), Box::new(Wildcard)),
                                    ],
                                ),
                                expr: arc(Binary(
                                    arc(Ref("x".to_string())),
                                    BinOp::Mul,
                                    arc(Ref("y".to_string())),
                                )),
                            },
                            // Pattern for list with exactly 3 elements
                            MatchArm {
                                pattern: Pattern::Struct(
                                    "Array".to_string(),
                                    vec![
                                        Bind("x".to_string(), Box::new(Wildcard)),
                                        Bind("y".to_string(), Box::new(Wildcard)),
                                        Bind("z".to_string(), Box::new(Wildcard)),
                                    ],
                                ),
                                expr: arc(Binary(
                                    arc(Binary(
                                        arc(Ref("x".to_string())),
                                        BinOp::Mul,
                                        arc(Ref("y".to_string())),
                                    )),
                                    BinOp::Mul,
                                    arc(Ref("z".to_string())),
                                )),
                            },
                            // Fallback
                            MatchArm {
                                pattern: Wildcard,
                                expr: arc(CoreVal(int_val(0))),
                            },
                        ],
                    )),
                },
                // Fallback
                MatchArm {
                    pattern: Wildcard,
                    expr: arc(CoreVal(int_val(0))),
                },
            ],
        ));

        // Evaluate the nested pattern matches
        let values = collect_stream_values(pattern_match_expr.evaluate(engine));

        // Should get 4 results: 2 Filter values × 2 Array options = 4 combinations
        assert_eq!(values.len(), 4);

        // Expected results:
        // Filter(true) + [1, 2] = 1 + 2 = 3
        // Filter(true) + [3, 4, 5] = 3 + 4 + 5 = 12
        // Filter(false) + [1, 2] = 1 * 2 = 2
        // Filter(false) + [3, 4, 5] = 3 * 4 * 5 = 60
        let result_ints: Vec<i64> = values
            .iter()
            .map(|v| {
                if let CoreData::Literal(Int64(i)) = &v.0 {
                    *i
                } else {
                    panic!("Expected integer result, got {:?}", v)
                }
            })
            .collect();

        println!("Results: {:?}", result_ints);
        assert!(result_ints.contains(&3)); // 1 + 2
        assert!(result_ints.contains(&12)); // 3 + 4 + 5
        assert!(result_ints.contains(&2)); // 1 * 2
        assert!(result_ints.contains(&60)); // 3 * 4 * 5
    }
}
