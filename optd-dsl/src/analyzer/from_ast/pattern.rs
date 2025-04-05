//! Pattern conversion from AST to HIR
//!
//! This module contains functions for converting AST pattern nodes to their
//! corresponding HIR representations.

use super::expr::convert_expr;
use crate::analyzer::hir::{Identifier, Literal, MatchArm, Pattern, PatternKind, TypedSpan};
use crate::analyzer::semantic_check::error::SemanticErrorKind;
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
            let hir_args = args.iter().map(convert_pattern).collect();

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

#[cfg(test)]
mod pattern_tests {
    use super::*;
    use crate::analyzer::hir::{Literal, PatternKind};
    use crate::parser::ast;
    use crate::utils::span::{Span, Spanned};
    use std::collections::HashSet;

    // Helper functions to create test patterns
    fn create_test_span() -> Span {
        Span::new("test".to_string(), 0..1)
    }

    fn spanned<T>(value: T) -> Spanned<T> {
        Spanned::new(value, create_test_span())
    }

    fn create_match_arm(pattern: ast::Pattern, expr: ast::Expr) -> Spanned<ast::MatchArm> {
        spanned(ast::MatchArm {
            pattern: spanned(pattern),
            expr: spanned(expr),
        })
    }

    #[test]
    fn test_convert_match_arms() {
        // Create a set of match arms
        let pattern1 = ast::Pattern::Literal(ast::Literal::Int64(1));
        let expr1 = ast::Expr::Literal(ast::Literal::String("one".to_string()));
        let arm1 = create_match_arm(pattern1, expr1);

        let pattern2 = ast::Pattern::Wildcard;
        let expr2 = ast::Expr::Literal(ast::Literal::String("other".to_string()));
        let arm2 = create_match_arm(pattern2, expr2);

        let arms = vec![arm1, arm2];

        // Convert the match arms
        let result = convert_match_arms(&arms, &HashSet::new()).unwrap();

        // Check the converted result
        assert_eq!(result.len(), 2);

        // Check first arm's pattern
        match &result[0].pattern.kind {
            PatternKind::Literal(Literal::Int64(val)) => assert_eq!(*val, 1),
            _ => panic!("Expected Int64 literal pattern"),
        }

        // Check second arm's pattern
        match &result[1].pattern.kind {
            PatternKind::Wildcard => (),
            _ => panic!("Expected wildcard pattern"),
        }
    }

    #[test]
    fn test_convert_patterns() {
        // Test literal pattern
        let int_pattern = spanned(ast::Pattern::Literal(ast::Literal::Int64(42)));
        let result = convert_pattern(&int_pattern);

        match &result.kind {
            PatternKind::Literal(Literal::Int64(val)) => assert_eq!(*val, 42),
            _ => panic!("Expected Int64 literal pattern"),
        }

        // Test wildcard pattern
        let wildcard_pattern = spanned(ast::Pattern::Wildcard);
        let result = convert_pattern(&wildcard_pattern);

        match &result.kind {
            PatternKind::Wildcard => (),
            _ => panic!("Expected Wildcard pattern"),
        }

        // Test binding pattern
        let inner = spanned(ast::Pattern::Wildcard);
        let bind_pattern = spanned(ast::Pattern::Bind(spanned("x".to_string()), inner));

        let result = convert_pattern(&bind_pattern);

        match &result.kind {
            PatternKind::Bind(name, _) => assert_eq!(name, "x"),
            _ => panic!("Expected Bind pattern"),
        }

        // Test constructor pattern
        let constructor_pattern = spanned(ast::Pattern::Constructor(
            spanned("Point".to_string()),
            vec![],
        ));

        let result = convert_pattern(&constructor_pattern);

        match &result.kind {
            PatternKind::Struct(name, _) => assert_eq!(name, "Point"),
            _ => panic!("Expected Struct pattern"),
        }

        // Test array patterns
        let empty_array_pattern = spanned(ast::Pattern::EmptyArray);
        let result = convert_pattern(&empty_array_pattern);

        match &result.kind {
            PatternKind::EmptyArray => (),
            _ => panic!("Expected EmptyArray pattern"),
        }
    }

    #[test]
    fn test_convert_array_decomp_pattern() {
        // Create a head::tail pattern
        let head = spanned(ast::Pattern::Bind(
            spanned("head".to_string()),
            spanned(ast::Pattern::Wildcard),
        ));
        let tail = spanned(ast::Pattern::Bind(
            spanned("tail".to_string()),
            spanned(ast::Pattern::Wildcard),
        ));

        let array_decomp = spanned(ast::Pattern::ArrayDecomp(head, tail));

        let result = convert_pattern(&array_decomp);

        match &result.kind {
            PatternKind::ArrayDecomp(head_pattern, tail_pattern) => {
                // Check head pattern
                match &head_pattern.kind {
                    PatternKind::Bind(name, _) => assert_eq!(name, "head"),
                    _ => panic!("Expected Bind pattern for head"),
                }

                // Check tail pattern
                match &tail_pattern.kind {
                    PatternKind::Bind(name, _) => assert_eq!(name, "tail"),
                    _ => panic!("Expected Bind pattern for tail"),
                }
            }
            _ => panic!("Expected ArrayDecomp pattern"),
        }
    }
}
