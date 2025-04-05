//! Expression conversion from AST to HIR
//!
//! This module contains functions for converting AST expression nodes to their
//! corresponding HIR representations.

use super::pattern::convert_match_arms;
use super::types::{convert_type, create_function_type};
use crate::analyzer::hir::{
    BinOp, CoreData, Expr, ExprKind, FunKind, Identifier, Literal, TypedSpan, UnaryOp, Value,
};
use crate::analyzer::types::Type;
use crate::parser::ast;
use crate::utils::error::CompileError;
use crate::utils::span::{Span, Spanned};
use ExprKind::*;
use std::collections::HashSet;
use std::sync::Arc;

/// Converts an AST expression to an HIR expression.
///
/// This function is the main dispatcher for expression conversion, routing
/// the conversion to specialized functions based on the expression kind.
pub(super) fn convert_expr(
    spanned_expr: &Spanned<ast::Expr>,
    generics: &HashSet<Identifier>,
) -> Result<Expr<TypedSpan>, CompileError> {
    let span = spanned_expr.span.clone();
    let mut ty = Type::Unknown;

    let kind = match &*spanned_expr.value {
        ast::Expr::Error => panic!("AST should no longer contain errors"),
        ast::Expr::Literal(lit) => convert_literal(lit, &span),
        ast::Expr::Ref(ident) => convert_ref(ident),
        ast::Expr::Binary(left, op, right) => convert_binary(left, op, right, &span, generics)?,
        ast::Expr::Unary(op, operand) => convert_unary(op, operand, generics)?,
        ast::Expr::Let(field, init, body) => convert_let(field, init, body, generics)?,
        ast::Expr::IfThenElse(condition, then_branch, else_branch) => {
            convert_if_then_else(condition, then_branch, else_branch, generics)?
        }
        ast::Expr::PatternMatch(scrutinee, arms) => {
            convert_pattern_match(scrutinee, arms, generics)?
        }
        ast::Expr::Array(elements) => convert_array(elements, generics)?,
        ast::Expr::Tuple(elements) => convert_tuple(elements, generics)?,
        ast::Expr::Map(entries) => convert_map(entries, generics)?,
        ast::Expr::Constructor(name, args) => convert_constructor(name, args, generics)?,
        ast::Expr::Closure(params, body) => {
            // We extract the potential type annotations in the closure.
            let params = params
                .iter()
                .map(|field| {
                    (
                        (*field.name).clone(),
                        convert_type(&field.ty.value, generics),
                    )
                })
                .collect::<Vec<_>>();

            ty = create_function_type(&params, &Type::Unknown);
            convert_closure(&params, body, generics)?
        }
        ast::Expr::Postfix(expr, op) => convert_postfix(expr, op, generics)?,
        ast::Expr::Fail(error_expr) => convert_fail(error_expr, generics)?,
    };

    Ok(Expr::new_with(kind, ty, span))
}

/// Converts a literal expression to an HIR expression kind.
fn convert_literal(literal: &ast::Literal, span: &Span) -> ExprKind<TypedSpan> {
    let hir_lit = match literal {
        ast::Literal::Int64(val) => Literal::Int64(*val),
        ast::Literal::String(val) => Literal::String(val.clone()),
        ast::Literal::Bool(val) => Literal::Bool(*val),
        ast::Literal::Float64(val) => Literal::Float64(val.0),
        ast::Literal::Unit => Literal::Unit,
    };

    CoreVal(Value::new_unknown(CoreData::Literal(hir_lit), span.clone()))
}

/// Converts a reference expression to an HIR expression kind.
fn convert_ref(ident: &Identifier) -> ExprKind<TypedSpan> {
    Ref(ident.clone())
}

/// Converts a binary expression to an HIR expression kind.
fn convert_binary(
    left: &Spanned<ast::Expr>,
    op: &ast::BinOp,
    right: &Spanned<ast::Expr>,
    span: &Span,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, CompileError> {
    match op {
        ast::BinOp::Add
        | ast::BinOp::Sub
        | ast::BinOp::Mul
        | ast::BinOp::Div
        | ast::BinOp::Lt
        | ast::BinOp::Eq
        | ast::BinOp::And
        | ast::BinOp::Or
        | ast::BinOp::Range
        | ast::BinOp::Concat => {
            let hir_left = convert_expr(left, generics)?;
            let hir_right = convert_expr(right, generics)?;
            let hir_op = match op {
                ast::BinOp::Add => BinOp::Add,
                ast::BinOp::Sub => BinOp::Sub,
                ast::BinOp::Mul => BinOp::Mul,
                ast::BinOp::Div => BinOp::Div,
                ast::BinOp::Lt => BinOp::Lt,
                ast::BinOp::Eq => BinOp::Eq,
                ast::BinOp::And => BinOp::And,
                ast::BinOp::Or => BinOp::Or,
                ast::BinOp::Range => BinOp::Range,
                ast::BinOp::Concat => BinOp::Concat,
                _ => unreachable!(),
            };

            Ok(Binary(hir_left.into(), hir_op, hir_right.into()))
        }

        // Desugar not equal (!= becomes !(a == b)).
        ast::BinOp::Neq => {
            let hir_left = convert_expr(left, generics)?;
            let hir_right = convert_expr(right, generics)?;

            let eq_expr = Expr::new_unknown(
                Binary(hir_left.into(), BinOp::Eq, hir_right.into()),
                span.clone(),
            );

            Ok(Unary(UnaryOp::Not, eq_expr.into()))
        }

        // Desugar greater than (> becomes b < a by swapping operands).
        ast::BinOp::Gt => {
            let hir_left = convert_expr(right, generics)?;
            let hir_right = convert_expr(left, generics)?;

            Ok(Binary(hir_left.into(), BinOp::Lt, hir_right.into()))
        }

        // Desugar greater than or equal (>= becomes !(a < b)).
        ast::BinOp::Ge => {
            let hir_left = convert_expr(left, generics)?;
            let hir_right = convert_expr(right, generics)?;

            let lt_expr = Expr::new_unknown(
                Binary(hir_left.into(), BinOp::Lt, hir_right.into()),
                span.clone(),
            );

            Ok(Unary(UnaryOp::Not, lt_expr.into()))
        }

        // Desugar less than or equal (<= becomes a < b || a == b).
        ast::BinOp::Le => {
            let hir_left = Arc::new(convert_expr(left, generics)?);
            let hir_right = Arc::new(convert_expr(right, generics)?);

            let lt_expr = Expr::new_unknown(
                Binary(hir_left.clone(), BinOp::Lt, hir_right.clone()),
                span.clone(),
            );
            let eq_expr = Expr::new_unknown(Binary(hir_left, BinOp::Eq, hir_right), span.clone());

            Ok(Binary(lt_expr.into(), BinOp::Or, eq_expr.into()))
        }
    }
}

/// Converts a unary expression to an HIR expression kind.
fn convert_unary(
    op: &ast::UnaryOp,
    operand: &Spanned<ast::Expr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, CompileError> {
    let hir_operand = convert_expr(operand, generics)?;

    let hir_op = match op {
        ast::UnaryOp::Neg => UnaryOp::Neg,
        ast::UnaryOp::Not => UnaryOp::Not,
    };

    Ok(Unary(hir_op, hir_operand.into()))
}

/// Converts a let expression to an HIR expression kind.
fn convert_let(
    field: &Spanned<ast::Field>,
    init: &Spanned<ast::Expr>,
    body: &Spanned<ast::Expr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, CompileError> {
    let hir_init = convert_expr(init, generics)?;
    let hir_body = convert_expr(body, generics)?;
    let var_name = (*field.name).clone();

    Ok(Let(var_name, hir_init.into(), hir_body.into()))
}

/// Converts a pattern match expression to an HIR expression kind.
fn convert_pattern_match(
    scrutinee: &Spanned<ast::Expr>,
    arms: &[Spanned<ast::MatchArm>],
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, CompileError> {
    let hir_scrutinee = convert_expr(scrutinee, generics)?;
    let hir_arms = convert_match_arms(arms, generics)?;

    Ok(PatternMatch(hir_scrutinee.into(), hir_arms))
}

/// Converts an if-then-else expression to an HIR expression kind.
fn convert_if_then_else(
    condition: &Spanned<ast::Expr>,
    then_branch: &Spanned<ast::Expr>,
    else_branch: &Spanned<ast::Expr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, CompileError> {
    let hir_condition = convert_expr(condition, generics)?;
    let hir_then = convert_expr(then_branch, generics)?;
    let hir_else = convert_expr(else_branch, generics)?;

    Ok(IfThenElse(
        hir_condition.into(),
        hir_then.into(),
        hir_else.into(),
    ))
}

/// Helper function to convert a list of expressions.
fn convert_expr_list(
    elements: &[Spanned<ast::Expr>],
    generics: &HashSet<Identifier>,
) -> Result<Vec<Arc<Expr<TypedSpan>>>, CompileError> {
    let mut hir_elements = Vec::with_capacity(elements.len());

    for elem in elements {
        let hir_elem = convert_expr(elem, generics)?;
        hir_elements.push(Arc::new(hir_elem));
    }

    Ok(hir_elements)
}

/// Converts an array expression to an HIR expression kind.
fn convert_array(
    elements: &[Spanned<ast::Expr>],
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, CompileError> {
    let hir_elements = convert_expr_list(elements, generics)?;
    Ok(CoreExpr(CoreData::Array(hir_elements)))
}

/// Converts a tuple expression to an HIR expression kind.
fn convert_tuple(
    elements: &[Spanned<ast::Expr>],
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, CompileError> {
    let hir_elements = convert_expr_list(elements, generics)?;
    Ok(CoreExpr(CoreData::Tuple(hir_elements)))
}

/// Converts a map expression to an HIR expression kind.
fn convert_map(
    entries: &[(Spanned<ast::Expr>, Spanned<ast::Expr>)],
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, CompileError> {
    let mut hir_entries = Vec::with_capacity(entries.len());

    for (key, value) in entries {
        let key = convert_expr(key, generics)?;
        let value = convert_expr(value, generics)?;
        hir_entries.push((Arc::new(key), Arc::new(value)));
    }

    Ok(Map(hir_entries))
}

/// Converts a constructor expression to an HIR expression kind.
fn convert_constructor(
    name: &Identifier,
    args: &[Spanned<ast::Expr>],
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, CompileError> {
    let hir_args = convert_expr_list(args, generics)?;

    Ok(CoreExpr(CoreData::Struct(name.clone(), hir_args)))
}

/// Converts a closure expression to an HIR expression kind.
fn convert_closure(
    params: &[(Identifier, Type)],
    body: &Spanned<ast::Expr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, CompileError> {
    let param_names = params.iter().map(|(name, _)| name.clone()).collect();
    let hir_body = convert_expr(body, generics)?;

    Ok(CoreExpr(CoreData::Function(FunKind::Closure(
        param_names,
        hir_body.into(),
    ))))
}

/// Converts a postfix expression to an HIR expression kind.
fn convert_postfix(
    expr: &Spanned<ast::Expr>,
    op: &ast::PostfixOp,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, CompileError> {
    let hir_expr = convert_expr(expr, generics)?;

    match op {
        ast::PostfixOp::Call(args) => {
            let hir_args = convert_expr_list(args, generics)?;
            Ok(Call(hir_expr.into(), hir_args))
        }
        ast::PostfixOp::Field(field_name) => {
            // Wait until after type inference to transform this
            // into a `Call` operation.
            Ok(FieldAccess(hir_expr.into(), (*field_name.value).clone()))
        }
        // Desugar method call (obj.method(args)) into function call (method(obj, args)).
        ast::PostfixOp::Method(method_name, args) => {
            let all_args = std::iter::once(hir_expr.into())
                .chain(convert_expr_list(args, generics)?)
                .collect();

            let method_fn = Arc::new(Expr::new_unknown(
                Ref((*method_name.value).clone()),
                method_name.span.clone(),
            ));

            Ok(Call(method_fn, all_args))
        }
    }
}

/// Converts a fail expression to an HIR expression kind.
fn convert_fail(
    error_expr: &Spanned<ast::Expr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, CompileError> {
    let hir_error = convert_expr(error_expr, generics)?;

    Ok(CoreExpr(CoreData::Fail(Box::new(Arc::new(hir_error)))))
}
