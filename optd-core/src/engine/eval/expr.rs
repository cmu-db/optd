//! This module provides implementation of expression evaluation, handling different
//! expression types and evaluation strategies in a non-blocking, streaming manner.

use super::{
    binary::eval_binary_op, core::evaluate_core_expr, r#match::try_match_arms,
    unary::eval_unary_op, Expander,
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
use futures::StreamExt;
use optd_dsl::analyzer::hir::{
    BinOp, CoreData, Expr, FunKind, Identifier, Literal, MatchArm, UnaryOp, Value,
};
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
                    try_match_arms(value, match_arms.clone(), engine.clone())
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
                            evaluate_closure_call(params, body, args, engine)
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
    params: Vec<Identifier>,
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
            .unwrap_or_else(|| panic!("Variable not found: {}", ident))
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
        BinOp, CoreData, GroupId, Literal, LogicalOp, MatchArm, Materializable, Operator, Pattern,
        Value,
    };
    use Literal::*;
    use Materializable::*;
    use Pattern::*;

    #[test]
    fn test_pattern_matching_with_different_operator_types() {
        // Create a MockExpander that provides different operator types within a group
        let expander = MockExpander::new(
            |group_id| {
                if group_id == GroupId(10) {
                    // Group 10 expands to multiple operator types
                    vec![
                        // Filter operator
                        Value(Logical(LogicalOp(Materialized(Operator {
                            tag: "Filter".to_string(),
                            data: vec![bool_val(true)],
                            children: vec![string_val("customers")],
                        })))),
                        // Join operator
                        Value(Logical(LogicalOp(Materialized(Operator {
                            tag: "Join".to_string(),
                            data: vec![string_val("customer_id")],
                            children: vec![string_val("customers"), string_val("orders")],
                        })))),
                        // Project operator
                        Value(Logical(LogicalOp(Materialized(Operator {
                            tag: "Project".to_string(),
                            data: vec![int_val(42)],
                            children: vec![string_val("customers")],
                        })))),
                    ]
                } else {
                    vec![]
                }
            },
            |_| panic!("Physical expansion not expected"),
            |_| panic!("Properties expansion not expected"),
        );

        let engine = Engine::new(Context::default(), expander);

        // Create a logical group reference
        let logical_group_ref = Value(Logical(LogicalOp(UnMaterialized(GroupId(10)))));

        // Create a pattern match expression with arms for each operator type
        let pattern_match_expr = arc(PatternMatch(
            arc(CoreVal(logical_group_ref)),
            vec![
                // First arm: match Filter operator
                MatchArm {
                    pattern: Pattern::Operator(Operator {
                        tag: "Filter".to_string(),
                        data: vec![Pattern::Literal(Bool(true))],
                        children: vec![Bind("filter_table".to_string(), Box::new(Wildcard))],
                    }),
                    expr: arc(Binary(
                        arc(CoreVal(string_val("Filter on: "))),
                        BinOp::Concat,
                        arc(Ref("filter_table".to_string())),
                    )),
                },
                // Second arm: match Join operator with binding
                MatchArm {
                    pattern: Pattern::Operator(Operator {
                        tag: "Join".to_string(),
                        data: vec![Bind("join_key".to_string(), Box::new(Wildcard))],
                        children: vec![
                            Bind("left_table".to_string(), Box::new(Wildcard)),
                            Bind("right_table".to_string(), Box::new(Wildcard)),
                        ],
                    }),
                    expr: arc(CoreVal(string_val("join_matched"))),
                },
                // Third arm: match Project operator
                MatchArm {
                    pattern: Pattern::Operator(Operator {
                        tag: "Project".to_string(),
                        data: vec![Pattern::Bind("column".to_string(), Box::new(Wildcard))],
                        children: vec![Wildcard],
                    }),
                    expr: arc(Ref("column".to_string())), // Return the bound column value
                },
                // Fallback arm
                MatchArm {
                    pattern: Wildcard,
                    expr: arc(CoreVal(string_val("wildcard_matched"))),
                },
            ],
        ));

        // Evaluate the pattern match expression
        let values = collect_stream_values(pattern_match_expr.evaluate(engine));

        // Should get three results - one for each operator type
        assert_eq!(values.len(), 3);

        // Collect the results
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

        // Check that each operator matched correctly
        assert!(result_strings.contains(&"Filter on: customers".to_string()));
        assert!(result_strings.contains(&"join_matched".to_string()));
        assert!(result_strings.contains(&"42".to_string())); // From binding the column value
    }

    #[test]
    fn test_recursive_list_length_function() {
        // Create a mockExpander that doesn't expand anything
        let expander = MockExpander::new(
            |_| vec![],
            |_| panic!("Physical expansion not expected"),
            |_| panic!("Properties expansion not expected"),
        );

        // Create recursive length calculation function
        // length([]) = 0
        // length([x :: xs]) = 1 + length(xs)
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
                            Wildcard.into(),                                  // head (ignored)
                            Bind("rest".to_string(), Wildcard.into()).into(), // tail
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
        let engine = Engine::new(ctx, expander);

        // Create test data: lists of different lengths
        let empty_list = Value(Array(vec![]));
        let singleton_list = Value(Array(vec![int_val(5)]));
        let pair_list = Value(Array(vec![int_val(10), int_val(20)]));
        let triple_list = Value(Array(vec![int_val(1), int_val(2), int_val(3)]));

        // Helper function to call the length function on a list
        let get_length = |list: Value| {
            let call_expr = arc(Call(
                arc(Ref("length".to_string())),
                vec![arc(CoreVal(list))],
            ));
            collect_stream_values(call_expr.evaluate(engine.clone()))
        };

        // Test each list
        let empty_result = get_length(empty_list);
        let singleton_result = get_length(singleton_list);
        let pair_result = get_length(pair_list);
        let triple_result = get_length(triple_list);

        // Check results
        assert_eq!(empty_result.len(), 1);
        assert!(matches!(&empty_result[0].0, CoreData::Literal(Int64(0))));

        assert_eq!(singleton_result.len(), 1);
        assert!(matches!(
            &singleton_result[0].0,
            CoreData::Literal(Int64(1))
        ));

        assert_eq!(pair_result.len(), 1);
        assert!(matches!(&pair_result[0].0, CoreData::Literal(Int64(2))));

        assert_eq!(triple_result.len(), 1);
        assert!(matches!(&triple_result[0].0, CoreData::Literal(Int64(3))));
    }

    #[test]
    fn test_pattern_with_group_references() {
        // Create a MockExpander that provides operators with group references as children
        let expander = MockExpander::new(
            |group_id| {
                match group_id {
                    GroupId(15) => {
                        // Main join operator with group references as children
                        let join_op = Operator {
                            tag: "Join".to_string(),
                            data: vec![string_val("customer_id")],
                            children: vec![
                                // Left child is a reference to group 16
                                Value(Logical(LogicalOp(UnMaterialized(GroupId(16))))),
                                // Right child is a reference to group 17
                                Value(Logical(LogicalOp(UnMaterialized(GroupId(17))))),
                            ],
                        };

                        vec![Value(Logical(LogicalOp(Materialized(join_op))))]
                    }
                    GroupId(16) => {
                        // Group 16 expands to a Filter operator
                        let filter_op = Operator {
                            tag: "Filter".to_string(),
                            data: vec![bool_val(true)],
                            children: vec![string_val("customers")],
                        };

                        vec![Value(Logical(LogicalOp(Materialized(filter_op))))]
                    }
                    GroupId(17) => {
                        // Group 17 expands to a Project operator
                        let project_op = Operator {
                            tag: "Project".to_string(),
                            data: vec![int_val(100)],
                            children: vec![string_val("orders")],
                        };

                        vec![Value(Logical(LogicalOp(Materialized(project_op))))]
                    }
                    _ => vec![],
                }
            },
            |_| panic!("Physical expansion not expected"),
            |_| panic!("Properties expansion not expected"),
        );

        let engine = Engine::new(Context::default(), expander);

        // Create a logical group reference to the main join operator
        let logical_group_ref = Value(Logical(LogicalOp(UnMaterialized(GroupId(15)))));

        // Create a pattern match that extracts components with bindings
        let pattern_match_expr = arc(PatternMatch(
            arc(CoreVal(logical_group_ref.clone())),
            vec![
                // Match the join structure with specific operator patterns for children
                MatchArm {
                    pattern: Pattern::Operator(Operator {
                        tag: "Join".to_string(),
                        data: vec![Bind("join_key".to_string(), Wildcard.into())],
                        children: vec![
                            // Left child - filter operator pattern (will cause group 16 to expand)
                            Pattern::Bind(
                                "left_child".to_string(),
                                Pattern::Operator(Operator {
                                    tag: "Filter".to_string(),
                                    data: vec![Bind("predicate".to_string(), Wildcard.into())],
                                    children: vec![Wildcard.into()],
                                })
                                .into(),
                            ),
                            // Right child - project operator pattern (will cause group 17 to expand)
                            Pattern::Bind(
                                "right_child".to_string(),
                                Pattern::Operator(Operator {
                                    tag: "Project".to_string(),
                                    data: vec![Bind("column".to_string(), Wildcard.into())],
                                    children: vec![Wildcard.into()],
                                })
                                .into(),
                            ),
                        ],
                    }),
                    // Create a structured result with the join key and the child references
                    expr: arc(CoreExpr(CoreData::Struct(
                        "JoinPlan".to_string(),
                        vec![
                            arc(Ref("join_key".to_string())),
                            arc(Ref("left_child".to_string())),
                            arc(Ref("right_child".to_string())),
                        ],
                    ))),
                },
                // Fallback arm
                MatchArm {
                    pattern: Wildcard,
                    expr: arc(CoreVal(string_val("no_match"))),
                },
            ],
        ));

        // Evaluate the pattern match expression
        let values = collect_stream_values(pattern_match_expr.evaluate(engine.clone()));
        // Should get one result with a structured value
        assert_eq!(values.len(), 1);

        // Verify the structure of the result
        if let CoreData::Struct(name, fields) = &values[0].0 {
            assert_eq!(name, "JoinPlan");
            assert_eq!(fields.len(), 3);

            // Check that the join key was correctly captured
            assert!(matches!(&fields[0].0, CoreData::Literal(String(s)) if s == "customer_id"));

            // Check that the bound values now contain the expanded operators, not the unexpanded references
            // Left child should be a Filter operator (expanded from group 16)
            if let CoreData::Logical(LogicalOp(Materialized(left_op))) = &fields[1].0 {
                assert_eq!(left_op.tag, "Filter");
                assert!(matches!(&left_op.data[0].0, CoreData::Literal(Bool(true))));
            } else {
                panic!("Expected expanded Filter operator, got {:?}", fields[1]);
            }

            // Right child should be a Project operator (expanded from group 17)
            if let CoreData::Logical(LogicalOp(Materialized(right_op))) = &fields[2].0 {
                assert_eq!(right_op.tag, "Project");
                assert!(matches!(&right_op.data[0].0, CoreData::Literal(Int64(100))));
            } else {
                panic!("Expected expanded Project operator, got {:?}", fields[2]);
            }
        } else {
            panic!("Expected Struct result, got {:?}", values[0]);
        }

        // Now let's create a second pattern match that first expands the child groups
        let expand_children_expr = arc(PatternMatch(
            arc(CoreVal(logical_group_ref)),
            vec![
                // Match the join and then expand its children
                MatchArm {
                    pattern: Pattern::Operator(Operator {
                        tag: "Join".to_string(),
                        data: vec![Bind("join_key".to_string(), Wildcard.into())],
                        children: vec![
                            // Patterns to match the expanded child groups
                            Pattern::Operator(Operator {
                                tag: "Filter".to_string(),
                                data: vec![Bind("predicate".to_string(), Wildcard.into())],
                                children: vec![Bind("left_table".to_string(), Wildcard.into())],
                            }),
                            Pattern::Operator(Operator {
                                tag: "Project".to_string(),
                                data: vec![Bind("column".to_string(), Wildcard.into())],
                                children: vec![Bind("right_table".to_string(), Wildcard.into())],
                            }),
                        ],
                    }),
                    // Create a structured result with all bound values
                    expr: arc(CoreExpr(CoreData::Struct(
                        "ExpandedJoinPlan".to_string(),
                        vec![
                            arc(Ref("join_key".to_string())),
                            arc(Ref("predicate".to_string())),
                            arc(Ref("left_table".to_string())),
                            arc(Ref("column".to_string())),
                            arc(Ref("right_table".to_string())),
                        ],
                    ))),
                },
                // Fallback arm
                MatchArm {
                    pattern: Wildcard,
                    expr: arc(CoreVal(string_val("no_match"))),
                },
            ],
        ));

        // This should also work, because the pattern will cause expansion of the groups
        let expanded_values = collect_stream_values(expand_children_expr.evaluate(engine));

        // Should get one result with the expanded values
        assert_eq!(expanded_values.len(), 1);

        // Verify the structure of the result with expanded values
        if let CoreData::Struct(name, fields) = &expanded_values[0].0 {
            assert_eq!(name, "ExpandedJoinPlan");
            assert_eq!(fields.len(), 5);

            // Check that all values were correctly captured after expansion
            assert!(matches!(&fields[0].0, CoreData::Literal(String(s)) if s == "customer_id"));
            assert!(matches!(&fields[1].0, CoreData::Literal(Bool(true))));
            assert!(matches!(&fields[2].0, CoreData::Literal(String(s)) if s == "customers"));
            assert!(matches!(&fields[3].0, CoreData::Literal(Int64(100))));
            assert!(matches!(&fields[4].0, CoreData::Literal(String(s)) if s == "orders"));
        } else {
            panic!("Expected Struct result, got {:?}", expanded_values[0]);
        }
    }
}
