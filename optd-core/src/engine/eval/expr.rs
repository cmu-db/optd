//! This module provides implementation of expression evaluation, handling different
//! expression types and evaluation strategies in a non-blocking, streaming manner.

use crate::{
    capture,
    engine::utils::streams::{
        evaluate_all_combinations, propagate_success, stream_from_result, ValueStream,
    },
};
use futures::StreamExt;
use optd_dsl::analyzer::{
    context::Context,
    hir::{BinOp, CoreData, Expr, FunKind, Literal, MatchArm, UnaryOp, Value},
};

use super::{
    binary::eval_binary_op, core::evaluate_core_expr, r#match::try_match_arms,
    unary::eval_unary_op, Evaluate,
};
use CoreData::*;
use Expr::*;
use FunKind::*;

impl Evaluate for Expr {
    /// Evaluates an expression to a stream of possible values.
    ///
    /// This function consumes the expression, dispatching to specialized
    /// handlers for each expression type.
    ///
    /// # Parameters
    /// * `self` - The expression to evaluate (consumed)
    /// * `context` - The evaluation context containing variable bindings
    ///
    /// # Returns
    /// A stream of all possible evaluation results
    fn evaluate(self, context: Context) -> ValueStream {
        match self {
            PatternMatch(expr, match_arms) => evaluate_pattern_match(*expr, match_arms, context),
            IfThenElse(cond, then_expr, else_expr) => {
                evaluate_if_then_else(*cond, *then_expr, *else_expr, context)
            }
            Let(ident, assignee, after) => evaluate_let_binding(ident, *assignee, *after, context),
            Binary(left, op, right) => evaluate_binary_expr(*left, op, *right, context),
            Unary(op, expr) => evaluate_unary_expr(op, *expr, context),
            Call(fun, args) => evaluate_function_call(*fun, args, context),
            Ref(ident) => evaluate_reference(ident, context),
            CoreExpr(expr) => evaluate_core_expr(expr, context),
            CoreVal(val) => propagate_success(val).boxed(),
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
/// * `context` - The evaluation context
///
/// # Returns
/// A stream of all possible evaluation results
fn evaluate_pattern_match(expr: Expr, match_arms: Vec<MatchArm>, context: Context) -> ValueStream {
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

/// Evaluates an if-then-else expression.
///
/// First evaluates the condition, then either the 'then' branch if the condition is true,
/// or the 'else' branch if the condition is false.
fn evaluate_if_then_else(
    cond: Expr,
    then_expr: Expr,
    else_expr: Expr,
    context: Context,
) -> ValueStream {
    cond.evaluate(context.clone())
        .flat_map(move |cond_result| {
            stream_from_result(
                cond_result,
                capture!([context, then_expr, else_expr], move |value| {
                    match value.0 {
                        // If condition is a boolean, evaluate the appropriate branch
                        Literal(Literal::Bool(b)) => {
                            if b {
                                then_expr.evaluate(context)
                            } else {
                                else_expr.evaluate(context)
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
fn evaluate_let_binding(
    ident: String,
    assignee: Expr,
    after: Expr,
    context: Context,
) -> ValueStream {
    assignee
        .evaluate(context.clone())
        .flat_map(move |expr_result| {
            stream_from_result(
                expr_result,
                capture!([context, after, ident], move |value| {
                    // Create updated context with the new binding
                    let mut new_ctx = context.clone();
                    new_ctx.bind(ident, value);
                    after.evaluate(new_ctx)
                }),
            )
        })
        .boxed()
}

/// Evaluates a binary expression.
///
/// Evaluates both operands in all possible combinations, then applies the binary operation.
fn evaluate_binary_expr(left: Expr, op: BinOp, right: Expr, context: Context) -> ValueStream {
    evaluate_all_combinations(vec![left, right].into_iter(), context)
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
fn evaluate_unary_expr(op: UnaryOp, expr: Expr, context: Context) -> ValueStream {
    expr.evaluate(context)
        .map(move |expr_result| expr_result.map(|value| eval_unary_op(&op, value)))
        .boxed()
}

/// Evaluates a function call expression.
///
/// First evaluates the function expression, then the arguments,
/// and finally applies the function to the arguments.
fn evaluate_function_call(fun: Expr, args: Vec<Expr>, context: Context) -> ValueStream {
    let fun_stream = fun.evaluate(context.clone());

    fun_stream
        .flat_map(move |fun_result| {
            stream_from_result(
                fun_result,
                capture!([context, args], move |fun_value| {
                    match fun_value.0 {
                        // Handle closure (user-defined function)
                        Function(Closure(params, body)) => {
                            evaluate_closure_call(params, body, args.clone(), context.clone())
                        }
                        // Handle Rust UDF (built-in function)
                        Function(RustUDF(udf)) => evaluate_rust_udf_call(udf, args, context),
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
fn evaluate_closure_call(
    params: Vec<String>,
    body: Box<Expr>,
    args: Vec<Expr>,
    context: Context,
) -> ValueStream {
    evaluate_all_combinations(args.into_iter(), context.clone())
        .flat_map(move |args_result| {
            stream_from_result(
                args_result,
                capture!([context, params, body], move |args| {
                    // Create a new context with parameters bound to arguments
                    let mut new_ctx = context;
                    new_ctx.push_scope();
                    params.iter().zip(args).for_each(|(p, a)| {
                        new_ctx.bind(p.clone(), a);
                    });
                    (*body).evaluate(new_ctx)
                }),
            )
        })
        .boxed()
}

/// Evaluates a call to a Rust UDF (built-in function).
///
/// Evaluates the arguments, then calls the Rust function with those arguments.
fn evaluate_rust_udf_call(
    udf: fn(Vec<Value>) -> Value,
    args: Vec<Expr>,
    context: Context,
) -> ValueStream {
    evaluate_all_combinations(args.into_iter(), context)
        .map(move |args_result| args_result.map(udf))
        .boxed()
}

/// Evaluates a reference to a variable.
///
/// Looks up the variable in the context and returns its value.
fn evaluate_reference(ident: String, context: Context) -> ValueStream {
    propagate_success(context.lookup(&ident).expect("Variable not found").clone()).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::utils::streams::ValueStream;
    use futures::executor::block_on_stream;
    use optd_dsl::analyzer::hir::{BinOp, CoreData, Literal, MatchArm, Pattern, UnaryOp, Value};
    use std::collections::HashMap;
    use BinOp::*;
    use UnaryOp::*;

    use CoreData::{Function, Struct};
    use Literal::{Bool, Int64, String};
    use Pattern::{Bind, Wildcard};

    // Helper functions to create values
    fn int_val(i: i64) -> Value {
        Value(Literal(Int64(i)))
    }

    fn string_val(s: &str) -> Value {
        Value(Literal(String(s.to_string())))
    }

    fn bool_val(b: bool) -> Value {
        Value(Literal(Bool(b)))
    }

    // Helper to collect all successful values from a stream
    fn collect_stream_values(stream: ValueStream) -> Vec<Value> {
        block_on_stream(stream).filter_map(Result::ok).collect()
    }

    // Helper to create a context with bindings
    fn create_context_with_bindings(bindings: Vec<(std::string::String, Value)>) -> Context {
        let mut ctx = Context::new(HashMap::new());
        for (name, value) in bindings {
            ctx.bind(name, value);
        }
        ctx
    }

    #[test]
    fn test_evaluate_core_val() {
        // Test evaluating a core value directly
        let expr = CoreVal(int_val(42));
        let context = Context::new(HashMap::new());

        let stream = expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(42))));
    }

    #[test]
    fn test_evaluate_let_binding() {
        // Test a basic let binding: let x = 42 in x + 10
        let expr = Let(
            "x".to_string(),
            Box::new(CoreVal(int_val(42))),
            Box::new(Binary(
                Box::new(Ref("x".to_string())),
                Add,
                Box::new(CoreVal(int_val(10))),
            )),
        );

        let context = Context::new(HashMap::new());
        let stream = expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(52)))); // 42 + 10 = 52
    }

    #[test]
    fn test_evaluate_nested_let_binding() {
        // Test nested let bindings:
        // let x = 5 in
        //   let y = x * 2 in
        //     x + y
        let expr = Let(
            "x".to_string(),
            Box::new(CoreVal(int_val(5))),
            Box::new(Let(
                "y".to_string(),
                Box::new(Binary(
                    Box::new(Ref("x".to_string())),
                    Mul,
                    Box::new(CoreVal(int_val(2))),
                )),
                Box::new(Binary(
                    Box::new(Ref("x".to_string())),
                    Add,
                    Box::new(Ref("y".to_string())),
                )),
            )),
        );

        let context = Context::new(HashMap::new());
        let stream = expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(15)))); // 5 + (5 * 2) = 15
    }

    #[test]
    fn test_evaluate_if_then_else() {
        let context = Context::new(HashMap::new());

        // Test true condition: if true then 42 else 24
        let true_expr = IfThenElse(
            Box::new(CoreVal(bool_val(true))),
            Box::new(CoreVal(int_val(42))),
            Box::new(CoreVal(int_val(24))),
        );

        let stream = true_expr.evaluate(context.clone());
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(42))));

        // Test false condition: if false then 42 else 24
        let false_expr = IfThenElse(
            Box::new(CoreVal(bool_val(false))),
            Box::new(CoreVal(int_val(42))),
            Box::new(CoreVal(int_val(24))),
        );

        let stream = false_expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(24))));
    }

    #[test]
    fn test_evaluate_if_then_else_with_computation() {
        // Test if-then-else with computed condition:
        // if 5 < 10 then "less" else "greater"
        let expr = IfThenElse(
            Box::new(Binary(
                Box::new(CoreVal(int_val(5))),
                Lt,
                Box::new(CoreVal(int_val(10))),
            )),
            Box::new(CoreVal(string_val("less"))),
            Box::new(CoreVal(string_val("greater"))),
        );

        let context = Context::new(HashMap::new());
        let stream = expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(String(s)) if s == "less"));
    }

    #[test]
    fn test_evaluate_binary_expr() {
        let context = Context::new(HashMap::new());

        // Test addition: 5 + 7
        let add_expr = Binary(
            Box::new(CoreVal(int_val(5))),
            Add,
            Box::new(CoreVal(int_val(7))),
        );

        let stream = add_expr.evaluate(context.clone());
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(12))));

        // Test complex expression: (3 * 4) + (10 / 2)
        let complex_expr = Binary(
            Box::new(Binary(
                Box::new(CoreVal(int_val(3))),
                Mul,
                Box::new(CoreVal(int_val(4))),
            )),
            Add,
            Box::new(Binary(
                Box::new(CoreVal(int_val(10))),
                Div,
                Box::new(CoreVal(int_val(2))),
            )),
        );

        let stream = complex_expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(17)))); // (3 * 4) + (10 / 2) = 12 + 5 = 17
    }

    #[test]
    fn test_evaluate_unary_expr() {
        let context = Context::new(HashMap::new());

        // Test negation: -42
        let neg_expr = Unary(Neg, Box::new(CoreVal(int_val(42))));

        let stream = neg_expr.evaluate(context.clone());
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(-42))));

        // Test logical not: !true
        let not_expr = Unary(Not, Box::new(CoreVal(bool_val(true))));

        let stream = not_expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Bool(false))));
    }

    #[test]
    fn test_evaluate_variable_reference() {
        // Create a context with a bound variable
        let context = create_context_with_bindings(vec![("x".to_string(), int_val(42))]);

        // Test referencing the variable
        let expr = Ref("x".to_string());

        let stream = expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(42))));
    }

    #[test]
    fn test_evaluate_closure_call() {
        let context = Context::new(HashMap::new());

        // Create a closure: fn(x) { x + 1 }
        let closure_body = Binary(
            Box::new(Ref("x".to_string())),
            Add,
            Box::new(CoreVal(int_val(1))),
        );

        let closure_val = Value(Function(Closure(
            vec!["x".to_string()],
            Box::new(closure_body),
        )));

        // Call the closure with argument 5
        let call_expr = Call(Box::new(CoreVal(closure_val)), vec![CoreVal(int_val(5))]);

        let stream = call_expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(6)))); // 5 + 1 = 6
    }

    #[test]
    fn test_evaluate_rust_udf_call() {
        let context = Context::new(HashMap::new());

        // Create a Rust UDF that squares a number
        fn square_udf(args: Vec<Value>) -> Value {
            if let Literal(Int64(n)) = args[0].0 {
                int_val(n * n)
            } else {
                panic!("Expected integer argument")
            }
        }

        let udf_val = Value(Function(RustUDF(square_udf)));

        // Call the UDF with argument 7
        let call_expr = Call(Box::new(CoreVal(udf_val)), vec![CoreVal(int_val(7))]);

        let stream = call_expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(49)))); // 7² = 49
    }

    #[test]
    fn test_evaluate_pattern_match() {
        let context = Context::new(HashMap::new());

        // Create a pattern match expression:
        // match 42 {
        //   x @ _ => x
        // }
        let match_expr = PatternMatch(
            Box::new(CoreVal(int_val(42))),
            vec![MatchArm {
                pattern: Bind("x".to_string(), Box::new(Wildcard)),
                expr: Ref("x".to_string()),
            }],
        );

        let stream = match_expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(42))));
    }

    #[test]
    fn test_evaluate_pattern_match_multiple_arms() {
        let context = Context::new(HashMap::new());

        // Create a pattern match expression with multiple arms:
        // match 42 {
        //   50 => "fifty"
        //   42 => "forty-two"
        //   _ => "other"
        // }
        let match_expr = PatternMatch(
            Box::new(CoreVal(int_val(42))),
            vec![
                MatchArm {
                    pattern: Pattern::Literal(Int64(50)),
                    expr: CoreVal(string_val("fifty")),
                },
                MatchArm {
                    pattern: Pattern::Literal(Int64(42)),
                    expr: CoreVal(string_val("forty-two")),
                },
                MatchArm {
                    pattern: Wildcard,
                    expr: CoreVal(string_val("other")),
                },
            ],
        );

        let stream = match_expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(String(s)) if s == "forty-two"));
    }

    #[test]
    fn test_evaluate_pattern_match_with_struct() {
        let context = Context::new(HashMap::new());

        // Create a struct value: Person("Alice", 30)
        let person = Value(Struct(
            "Person".to_string(),
            vec![string_val("Alice"), int_val(30)],
        ));

        // Create a pattern match expression:
        // match Person("Alice", 30) {
        //   Person(name, age) => name
        // }
        let match_expr = PatternMatch(
            Box::new(CoreVal(person)),
            vec![MatchArm {
                pattern: Pattern::Struct(
                    "Person".to_string(),
                    vec![
                        Bind("name".to_string(), Box::new(Wildcard)),
                        Bind("age".to_string(), Box::new(Wildcard)),
                    ],
                ),
                expr: Ref("name".to_string()),
            }],
        );

        let stream = match_expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(String(s)) if s == "Alice"));
    }

    #[test]
    fn test_evaluate_complex_expression() {
        let context = Context::new(HashMap::new());

        // Test a complex expression:
        // let x = 10 in
        //   let y = 20 in
        //     if x < y then
        //       x + y
        //     else
        //       x * y
        let expr = Let(
            "x".to_string(),
            Box::new(CoreVal(int_val(10))),
            Box::new(Let(
                "y".to_string(),
                Box::new(CoreVal(int_val(20))),
                Box::new(IfThenElse(
                    Box::new(Binary(
                        Box::new(Ref("x".to_string())),
                        Lt,
                        Box::new(Ref("y".to_string())),
                    )),
                    Box::new(Binary(
                        Box::new(Ref("x".to_string())),
                        Add,
                        Box::new(Ref("y".to_string())),
                    )),
                    Box::new(Binary(
                        Box::new(Ref("x".to_string())),
                        Mul,
                        Box::new(Ref("y".to_string())),
                    )),
                )),
            )),
        );

        let stream = expr.evaluate(context);
        let values = collect_stream_values(stream);

        assert_eq!(values.len(), 1);
        assert!(matches!(&values[0].0, Literal(Int64(30)))); // 10 + 20 = 30 (since 10 < 20)
    }

    #[test]
    fn test_recursive_list_sum() {
        let context = Context::new(HashMap::new());

        // Define a recursive sum function using pattern matching
        // sum([]) = 0
        // sum([x .. xs]) = x + sum(xs)
        let sum_function = Value(Function(Closure(
            vec!["arr".to_string()],
            Box::new(PatternMatch(
                Box::new(Ref("arr".to_string())),
                vec![
                    // Base case: empty array returns 0
                    MatchArm {
                        pattern: Pattern::EmptyArray,
                        expr: CoreVal(int_val(0)),
                    },
                    // Recursive case: add head + sum(tail)
                    MatchArm {
                        pattern: Pattern::ArrayDecomp(
                            Box::new(Bind("head".to_string(), Box::new(Wildcard))),
                            Box::new(Bind("tail".to_string(), Box::new(Wildcard))),
                        ),
                        expr: Binary(
                            Box::new(Ref("head".to_string())),
                            BinOp::Add,
                            Box::new(Call(
                                Box::new(Ref("sum".to_string())),
                                vec![Ref("tail".to_string())],
                            )),
                        ),
                    },
                ],
            )),
        )));

        // Bind the recursive function in the context
        let mut test_context = context.clone();
        test_context.bind("sum".to_string(), sum_function);

        // Test arrays
        let empty_array = Value(CoreData::Array(vec![]));
        let array_123 = Value(CoreData::Array(vec![int_val(1), int_val(2), int_val(3)]));
        let array_42 = Value(CoreData::Array(vec![int_val(42)]));

        // Test 1: Sum of empty array should be 0
        let call_empty = Call(Box::new(Ref("sum".to_string())), vec![CoreVal(empty_array)]);

        let result = collect_stream_values(call_empty.evaluate(test_context.clone()));
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0].0, Literal(Int64(n)) if *n == 0));

        // Test 2: Sum of [1, 2, 3] should be 6
        let call_123 = Call(Box::new(Ref("sum".to_string())), vec![CoreVal(array_123)]);

        let result = collect_stream_values(call_123.evaluate(test_context.clone()));
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0].0, Literal(Int64(n)) if *n == 6));

        // Test 3: Sum of [42] should be 42
        let call_42 = Call(Box::new(Ref("sum".to_string())), vec![CoreVal(array_42)]);

        let result = collect_stream_values(call_42.evaluate(test_context));
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0].0, Literal(Int64(n)) if *n == 42));
    }
}
