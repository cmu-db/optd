use optd_core::cascades::memo::Memoize;

use crate::analyzer::hir::{BinOp, CoreData, Expr, UnaryOp, Value};
use crate::analyzer::hir::{FunType, Literal};
use BinOp::*;
use CoreData::*;
use Expr::*;
use FunType::*;
use Literal::*;
use UnaryOp::*;

use super::context::Context;

impl Expr {
    fn eval_binary_op(left: Value, op: &BinOp, right: Value) -> Value {
        match (&left.0, op, &right.0) {
            // Integer operations
            (Literal(Int64(l)), Add, Literal(Int64(r))) => Value(Literal(Int64(l + r))),
            (Literal(Int64(l)), Sub, Literal(Int64(r))) => Value(Literal(Int64(l - r))),
            (Literal(Int64(l)), Mul, Literal(Int64(r))) => Value(Literal(Int64(l * r))),
            (Literal(Int64(l)), Div, Literal(Int64(r))) => Value(Literal(Int64(l / r))),
            (Literal(Int64(l)), Eq, Literal(Int64(r))) => Value(Literal(Bool(l == r))),
            (Literal(Int64(l)), Lt, Literal(Int64(r))) => Value(Literal(Bool(l < r))),

            // Float operations
            (Literal(Float64(l)), Add, Literal(Float64(r))) => Value(Literal(Float64(l + r))),
            (Literal(Float64(l)), Sub, Literal(Float64(r))) => Value(Literal(Float64(l - r))),
            (Literal(Float64(l)), Mul, Literal(Float64(r))) => Value(Literal(Float64(l * r))),
            (Literal(Float64(l)), Div, Literal(Float64(r))) => Value(Literal(Float64(l / r))),
            (Literal(Float64(l)), Lt, Literal(Float64(r))) => Value(Literal(Bool(l < r))),

            // Boolean operations
            (Literal(Bool(l)), And, Literal(Bool(r))) => Value(Literal(Bool(*l && *r))),
            (Literal(Bool(l)), Or, Literal(Bool(r))) => Value(Literal(Bool(*l || *r))),
            (Literal(Bool(l)), Eq, Literal(Bool(r))) => Value(Literal(Bool(*l == *r))),

            // String operations
            (Literal(String(l)), Eq, Literal(String(r))) => Value(Literal(Bool(l == r))),
            (Literal(String(l)), Concat, Literal(String(r))) => {
                Value(Literal(String(l.to_string() + r)))
            }

            // Range operation (creates an array)
            (Literal(Int64(l)), Range, Literal(Int64(r))) => {
                let range = (*l..=*r).map(|n| Value(Literal(Int64(n)))).collect();
                Value(Array(range))
            }

            // Array concatenation
            (Array(l), Concat, Array(r)) => {
                let mut new_array = l.clone();
                new_array.extend(r.iter().cloned());
                Value(Array(new_array))
            }

            _ => panic!("Invalid binary operation or type mismatch"),
        }
    }

    fn eval_unary_op(op: &UnaryOp, expr: Value) -> Value {
        match (op, &expr.0) {
            // Numeric negation
            (Neg, Literal(Int64(x))) => Value(Literal(Int64(-x))),
            (Neg, Literal(Float64(x))) => Value(Literal(Float64(-x))),

            // Boolean negation
            (Not, Literal(Bool(x))) => Value(Literal(Bool(!x))),

            _ => panic!("Invalid unary operation or type mismatch"),
        }
    }

    /// Evaluate an expression in the given context with memoization
    pub fn evaluate<'a, M: Memoize>(&self, context: &mut Context, memo: &M) -> Value {
        match self {
            PatternMatch(_expr, _match_arms) => todo!(),
            IfThenElse(expr, r#if, r#else) => {
                if let CoreData::Literal(Literal::Bool(b)) = expr.evaluate(context, memo).0 {
                    if b {
                        r#if.evaluate(context, memo)
                    } else {
                        r#else.evaluate(context, memo)
                    }
                } else {
                    panic!("Expected boolean value in if condition")
                }
            }
            Let(ident, assignee, after) => {
                let value = assignee.evaluate(context, memo);
                context.bind(ident.to_string(), value);
                after.evaluate(context, memo)
            }
            Binary(left, bin_op, right) => Self::eval_binary_op(
                left.evaluate(context, memo),
                bin_op,
                right.evaluate(context, memo),
            ),
            Unary(unary_op, expr) => Self::eval_unary_op(unary_op, expr.evaluate(context, memo)),
            Call(function, args) => {
                let fun = function.evaluate(context, memo);
                let evaluated_args: Vec<_> =
                    args.iter().map(|arg| arg.evaluate(context, memo)).collect();
                match fun.0 {
                    Function(Closure(params, body)) => {
                        context.push_scope();
                        for (param, arg) in params.iter().zip(evaluated_args.iter()) {
                            context.bind(param.clone(), arg.clone());
                        }
                        let result = body.evaluate(context, memo);
                        context.pop_scope();

                        result
                    }
                    Function(RustUDF(udf)) => {
                        let args = args.iter().map(|arg| arg.evaluate(context, memo)).collect();
                        udf(args)
                    }
                    _ => panic!("Expected function value"),
                }
            }
            Ref(ident) => context.lookup(ident).expect("Variable not found").clone(),
            Core(_core_data) => todo!(),
        }
    }
}
