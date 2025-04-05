//! Pattern conversion from AST to HIR
//!
//! This module contains functions for converting AST pattern nodes to their
//! corresponding HIR representations.

use super::expr::convert_expr;
use crate::analyzer::hir::{Identifier, Literal, MatchArm, Pattern, PatternKind, TypedSpan};
use crate::analyzer::semantic_checker::error::SemanticErrorKind;
use crate::parser::ast;
use crate::utils::span::Spanned;
use PatternKind::*;
use std::collections::HashSet;
use std::sync::Arc;

/// Converts a list of AST match arms to HIR match arms.
///
/// This function is the main entry point for pattern conversion, handling both
/// the patterns and expressions in match arms.
pub(super) fn convert_match_arms(
    arms: &[Spanned<ast::MatchArm>],
    generics: &HashSet<Identifier>,
) -> Result<Vec<MatchArm<TypedSpan>>, SemanticErrorKind> {
    arms.iter()
        .map(|arm| {
            let pattern = convert_pattern(&arm.pattern);
            let expr = convert_expr(&arm.expr, generics)?;

            Ok(MatchArm {
                pattern,
                expr: Arc::new(expr),
            })
        })
        .collect()
}

/// Converts an AST pattern to an HIR pattern.
///
/// All patterns are created with Unknown type for now, as type inference
/// will be handled in a later phase.
fn convert_pattern(spanned_pattern: &Spanned<ast::Pattern>) -> Pattern<TypedSpan> {
    let span = spanned_pattern.span.clone();

    let kind = match &*spanned_pattern.value {
        ast::Pattern::Error => panic!("AST should no longer contain errors"),

        ast::Pattern::Bind(name, inner_pattern) => {
            let hir_inner = convert_pattern(inner_pattern);
            Bind((*name.value).clone(), hir_inner.into())
        }

        ast::Pattern::Constructor(name, args) => {
            let hir_args = args.iter().map(|arg| convert_pattern(arg)).collect();

            // Wait until after type inference to transform this into
            // an operator if needed.
            Struct((*name.value).clone(), hir_args)
        }

        ast::Pattern::Literal(lit) => {
            let hir_lit = match lit {
                ast::Literal::Int64(val) => Literal::Int64(*val),
                ast::Literal::String(val) => Literal::String(val.clone()),
                ast::Literal::Bool(val) => Literal::Bool(*val),
                ast::Literal::Float64(val) => Literal::Float64(val.0),
                ast::Literal::Unit => Literal::Unit,
            };

            Literal(hir_lit)
        }

        ast::Pattern::Wildcard => Wildcard,

        ast::Pattern::EmptyArray => EmptyArray,

        ast::Pattern::ArrayDecomp(head, tail) => {
            let hir_head = convert_pattern(head);
            let hir_tail = convert_pattern(tail);

            ArrayDecomp(hir_head.into(), hir_tail.into())
        }
    };

    Pattern::new_unknown(kind, span)
}
