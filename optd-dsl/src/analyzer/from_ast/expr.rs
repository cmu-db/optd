//! Expression conversion from AST to HIR
//!
//! This module contains functions for converting AST expression nodes to their
//! corresponding HIR representations.

use crate::analyzer::hir::{BinOp, CoreData, Expr, ExprKind, Literal, TypedSpan, UnaryOp, Value};
use crate::analyzer::types::{Identifier, Type};
use crate::parser::ast;
use crate::utils::error::CompileError;
use crate::utils::span::{Span, Spanned};
use std::collections::HashSet;
use std::sync::Arc;

use ExprKind::*;

/// Converts an AST expression to an HIR expression.
///
/// This function is the main dispatcher for expression conversion, routing
/// the conversion to specialized functions based on the expression kind.
pub(super) fn convert_expr(
    spanned_expr: &Spanned<ast::Expr>,
    generics: &HashSet<Identifier>,
) -> Result<Expr<TypedSpan>, CompileError> {
    let span = spanned_expr.span.clone();

    // Always set initial type to Unknown - it will be resolved during type checking
    match &*spanned_expr.value {
        ast::Expr::Error => panic!("AST should no longer contain errors"),
        ast::Expr::Literal(lit) => convert_literal_expr(lit, span),
        ast::Expr::Ref(ident) => convert_ref_expr(ident, span, generics),
        ast::Expr::Binary(left, op, right) => convert_binary_expr(left, op, right, span, generics),
        ast::Expr::Unary(op, operand) => convert_unary_expr(op, operand, span, generics),
        ast::Expr::Let(field, init, body) => convert_let_expr(field, init, body, span, generics),
        ast::Expr::IfThenElse(condition, then_branch, else_branch) => {
            convert_if_then_else_expr(condition, then_branch, else_branch, span, generics)
        }
        ast::Expr::PatternMatch(_scrutinee, _arms) => todo!(),
        ast::Expr::Array(elements) => convert_array_expr(elements, span, generics),
        ast::Expr::Tuple(elements) => convert_tuple_expr(elements, span, generics),
        ast::Expr::Map(entries) => convert_map_expr(entries, span, generics),
        ast::Expr::Constructor(_name, _args) => todo!(),
        ast::Expr::Closure(_params, _body) => todo!(),
        ast::Expr::Postfix(_expr, _op) => todo!(),
        ast::Expr::Fail(error_expr) => convert_fail_expr(error_expr, span, generics),
    }
}

/// Converts a literal expression to an HIR expression.
fn convert_literal_expr(
    literal: &ast::Literal,
    span: Span,
) -> Result<Expr<TypedSpan>, CompileError> {
    let (hir_lit, lit_type) = match literal {
        ast::Literal::Int64(val) => (Literal::Int64(*val), Type::Int64),
        ast::Literal::String(val) => (Literal::String(val.clone()), Type::String),
        ast::Literal::Bool(val) => (Literal::Bool(*val), Type::Bool),
        ast::Literal::Float64(val) => (Literal::Float64(val.0), Type::Float64),
        ast::Literal::Unit => (Literal::Unit, Type::Unit),
    };

    Ok(Expr::new_with(
        CoreVal(Value::new_with(
            CoreData::Literal(hir_lit),
            lit_type.clone(),
            span.clone(),
        )),
        lit_type,
        span,
    ))
}

/// Converts a reference expression to an HIR expression.
fn convert_ref_expr(
    ident: &String,
    span: Span,
    generics: &HashSet<Identifier>,
) -> Result<Expr<TypedSpan>, CompileError> {
    // Check if reference is to a generic type parameter
    let ty = if generics.contains(ident) {
        Type::Generic(ident.clone())
    } else {
        Type::Unknown
    };

    Ok(Expr::new_with(Ref(ident.clone()), ty, span))
}

/// Converts a binary expression to an HIR expression.
fn convert_binary_expr(
    left: &Spanned<ast::Expr>,
    op: &ast::BinOp,
    right: &Spanned<ast::Expr>,
    span: Span,
    generics: &HashSet<Identifier>,
) -> Result<Expr<TypedSpan>, CompileError> {
    let kind = match op {
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

            Binary(hir_left.into(), hir_op, hir_right.into())
        }

        // Desugar not equal (!= becomes !(a == b)).
        ast::BinOp::Neq => {
            let hir_left = convert_expr(left, generics)?;
            let hir_right = convert_expr(right, generics)?;

            let eq_expr = Expr {
                kind: Binary(hir_left.into(), BinOp::Eq, hir_right.into()),
                metadata: TypedSpan {
                    span: left.span.clone(), // Using left's span as a placeholder.
                    ty: Type::Unknown,
                },
            };

            Unary(UnaryOp::Not, eq_expr.into())
        }

        // Desugar greater than (> becomes b < a by swapping operands).
        ast::BinOp::Gt => {
            let hir_left = convert_expr(right, generics)?;
            let hir_right = convert_expr(left, generics)?;

            Binary(hir_left.into(), BinOp::Lt, hir_right.into())
        }

        // Desugar greater than or equal (>= becomes !(a < b)).
        ast::BinOp::Ge => {
            let hir_left = convert_expr(left, generics)?;
            let hir_right = convert_expr(right, generics)?;

            let lt_expr = Expr {
                kind: Binary(hir_left.into(), BinOp::Lt, hir_right.into()),
                metadata: TypedSpan {
                    span: left.span.clone(), // Using left's span as a placeholder.
                    ty: Type::Unknown,
                },
            };

            Unary(UnaryOp::Not, lt_expr.into())
        }

        // Desugar less than or equal (<= becomes a < b || a == b).
        ast::BinOp::Le => {
            let hir_left = Arc::new(convert_expr(left, generics)?);
            let hir_right = Arc::new(convert_expr(right, generics)?);

            let lt_expr = Expr {
                kind: Binary(hir_left.clone(), BinOp::Lt, hir_right.clone()),
                metadata: TypedSpan {
                    span: left.span.clone(), // Using left's span as a placeholder.
                    ty: Type::Unknown,
                },
            };

            let eq_expr = Expr {
                kind: Binary(hir_left, BinOp::Eq, hir_right),
                metadata: TypedSpan {
                    span: right.span.clone(), // Using right's span as a placeholder.
                    ty: Type::Unknown,
                },
            };

            Binary(lt_expr.into(), BinOp::Or, eq_expr.into())
        }
    };

    Ok(Expr::new_with(kind, Type::Unknown, span))
}

/// Converts a unary expression to an HIR expression.
fn convert_unary_expr(
    op: &ast::UnaryOp,
    operand: &Spanned<ast::Expr>,
    span: Span,
    generics: &HashSet<Identifier>,
) -> Result<Expr<TypedSpan>, CompileError> {
    let hir_operand = convert_expr(operand, generics)?;

    let hir_op = match op {
        ast::UnaryOp::Neg => UnaryOp::Neg,
        ast::UnaryOp::Not => UnaryOp::Not,
    };

    Ok(Expr::new_with(
        Unary(hir_op, hir_operand.into()),
        Type::Unknown,
        span,
    ))
}

/// Converts a let expression to an HIR expression.
fn convert_let_expr(
    field: &Spanned<ast::Field>,
    init: &Spanned<ast::Expr>,
    body: &Spanned<ast::Expr>,
    span: Span,
    generics: &HashSet<Identifier>,
) -> Result<Expr<TypedSpan>, CompileError> {
    let hir_init = convert_expr(init, generics)?;
    let hir_body = convert_expr(body, generics)?;
    let var_name = (*field.name).clone();

    Ok(Expr::new_with(
        Let(var_name, hir_init.into(), hir_body.into()),
        Type::Unknown,
        span,
    ))
}

/// Converts an if-then-else expression to an HIR expression.
fn convert_if_then_else_expr(
    condition: &Spanned<ast::Expr>,
    then_branch: &Spanned<ast::Expr>,
    else_branch: &Spanned<ast::Expr>,
    span: Span,
    generics: &HashSet<Identifier>,
) -> Result<Expr<TypedSpan>, CompileError> {
    let hir_condition = convert_expr(condition, generics)?;
    let hir_then = convert_expr(then_branch, generics)?;
    let hir_else = convert_expr(else_branch, generics)?;

    Ok(Expr::new_with(
        IfThenElse(hir_condition.into(), hir_then.into(), hir_else.into()),
        Type::Unknown,
        span,
    ))
}

/// Converts an array expression to an HIR expression.
fn convert_array_expr(
    elements: &[Spanned<ast::Expr>],
    span: Span,
    generics: &HashSet<Identifier>,
) -> Result<Expr<TypedSpan>, CompileError> {
    let mut hir_elements = Vec::with_capacity(elements.len());

    for elem in elements {
        let hir_elem = convert_expr(elem, generics)?;
        hir_elements.push(Arc::new(hir_elem));
    }

    Ok(Expr::new_with(
        CoreExpr(CoreData::Array(hir_elements)),
        Type::Unknown,
        span,
    ))
}

/// Converts a tuple expression to an HIR expression.
fn convert_tuple_expr(
    elements: &[Spanned<ast::Expr>],
    span: Span,
    generics: &HashSet<Identifier>,
) -> Result<Expr<TypedSpan>, CompileError> {
    let mut hir_elements = Vec::with_capacity(elements.len());

    for elem in elements {
        let hir_elem = convert_expr(elem, generics)?;
        hir_elements.push(Arc::new(hir_elem));
    }

    Ok(Expr::new_with(
        CoreExpr(CoreData::Tuple(hir_elements)),
        Type::Unknown,
        span,
    ))
}

/// Converts a map expression to an HIR expression.
fn convert_map_expr(
    entries: &[(Spanned<ast::Expr>, Spanned<ast::Expr>)],
    span: Span,
    generics: &HashSet<Identifier>,
) -> Result<Expr<TypedSpan>, CompileError> {
    let mut hir_entries = Vec::with_capacity(entries.len());

    for (key, value) in entries {
        let hir_key = convert_expr(key, generics)?;
        let hir_value = convert_expr(value, generics)?;
        hir_entries.push((Arc::new(hir_key), Arc::new(hir_value)));
    }

    Ok(Expr::new_with(Map(hir_entries), Type::Unknown, span))
}

/// Converts a fail expression to an HIR expression.
fn convert_fail_expr(
    error_expr: &Spanned<ast::Expr>,
    span: Span,
    generics: &HashSet<Identifier>,
) -> Result<Expr<TypedSpan>, CompileError> {
    let hir_error = convert_expr(error_expr, generics)?;

    Ok(Expr::new_with(
        CoreExpr(CoreData::Fail(Box::new(Arc::new(hir_error)))),
        Type::Unknown,
        span,
    ))
}
