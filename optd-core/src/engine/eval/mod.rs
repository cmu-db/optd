use super::generator::{Continuation, Generator};
use super::{Engine, UnitFuture};
use core::evaluate_core_expr;
use expr::{
    evaluate_binary_expr, evaluate_function_call, evaluate_if_then_else, evaluate_let_binding,
    evaluate_reference, evaluate_unary_expr,
};
use optd_dsl::analyzer::hir::Expr;
use r#match::evaluate_pattern_match;
use std::sync::Arc;
use Expr::*;

mod binary;
mod core;
mod expr;
mod r#match;
mod operator;
mod unary;

/// Trait for evaluating expressions using Continuation Passing Style (CPS).
pub trait Evaluate {
    /// Evaluates an expression and passes results to the provided continuation.
    ///
    /// # Parameters
    ///
    /// * `self` - The expression to evaluate
    /// * `engine` - The evaluation engine (owned)
    /// * `k` - The continuation to receive each evaluation result
    fn evaluate<G>(self, engine: Engine<G>, k: Continuation) -> UnitFuture
    where
        G: Generator;
}

impl Evaluate for Arc<Expr> {
    fn evaluate<G>(self, engine: Engine<G>, k: Continuation) -> UnitFuture
    where
        G: Generator,
    {
        Box::pin(async move {
            match &*self {
                PatternMatch(expr, match_arms) => {
                    evaluate_pattern_match(expr.clone(), match_arms.clone(), engine, k).await
                }
                IfThenElse(cond, then_expr, else_expr) => {
                    evaluate_if_then_else(
                        cond.clone(),
                        then_expr.clone(),
                        else_expr.clone(),
                        engine,
                        k,
                    )
                    .await
                }
                Let(ident, assignee, after) => {
                    evaluate_let_binding(ident.clone(), assignee.clone(), after.clone(), engine, k)
                        .await
                }
                Binary(left, op, right) => {
                    evaluate_binary_expr(left.clone(), op.clone(), right.clone(), engine, k).await
                }
                Unary(op, expr) => evaluate_unary_expr(op.clone(), expr.clone(), engine, k).await,
                Call(fun, args) => {
                    evaluate_function_call(fun.clone(), args.clone(), engine, k).await
                }
                Ref(ident) => evaluate_reference(ident.clone(), engine, k).await,
                CoreExpr(expr) => evaluate_core_expr(expr.clone(), engine, k).await,
                CoreVal(val) => k(val.clone()).await,
            }
        })
    }
}
