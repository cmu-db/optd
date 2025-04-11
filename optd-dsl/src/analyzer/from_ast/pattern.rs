//! Pattern conversion from AST to HIR
//!
//! This module contains functions for converting AST pattern nodes to their
//! corresponding HIR representations.

use super::ASTConverter;
use crate::analyzer::error::AnalyzerErrorKind;
use crate::analyzer::hir::{Identifier, Literal, MatchArm, Pattern, PatternKind, TypedSpan};
use crate::parser::ast;
use crate::utils::span::Spanned;
use PatternKind::*;
use std::collections::HashSet;
use std::sync::Arc;

impl ASTConverter {
    /// Converts a list of AST match arms to HIR match arms.
    ///
    /// This function is the main entry point for pattern conversion, handling both
    /// the patterns and expressions in match arms.
    pub(super) fn convert_match_arms(
        &self,
        arms: &[Spanned<ast::MatchArm>],
        generics: &HashSet<Identifier>,
    ) -> Result<Vec<MatchArm<TypedSpan>>, Box<AnalyzerErrorKind>> {
        arms.iter()
            .map(|arm| {
                let pattern = self.convert_pattern(&arm.pattern)?;
                let expr = self.convert_expr(&arm.expr, generics)?;

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
    fn convert_pattern(
        &self,
        spanned_pattern: &Spanned<ast::Pattern>,
    ) -> Result<Pattern<TypedSpan>, Box<AnalyzerErrorKind>> {
        let span = spanned_pattern.span.clone();

        let kind = match &*spanned_pattern.value {
            ast::Pattern::Error => panic!("AST should no longer contain errors"),

            ast::Pattern::Bind(name, inner_pattern) => {
                let hir_inner = self.convert_pattern(inner_pattern)?;
                Bind((*name.value).clone(), hir_inner.into())
            }

            ast::Pattern::Constructor(name, args) => {
                let hir_args = args
                    .iter()
                    .map(|arg| self.convert_pattern(arg))
                    .collect::<Result<Vec<_>, _>>()?;

                // Check if the corresponding type exists.
                if !self.type_registry.subtypes.contains_key(&*name.value) {
                    return Err(AnalyzerErrorKind::new_undefined_type(
                        &name.value,
                        &name.span,
                    ));
                }

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
                let hir_head = self.convert_pattern(head)?;
                let hir_tail = self.convert_pattern(tail)?;

                ArrayDecomp(hir_head.into(), hir_tail.into())
            }
        };

        Ok(Pattern::new_unknown(kind, span))
    }
}

#[cfg(test)]
mod pattern_tests {
    use crate::analyzer::from_ast::ASTConverter;
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

    fn create_test_adt(name: &str) -> ast::Adt {
        ast::Adt::Product {
            name: spanned(name.to_string()),
            fields: vec![],
        }
    }

    #[test]
    fn test_convert_match_arms() {
        let mut converter = ASTConverter::default();

        // Register "Point" type for constructor patterns in match arms
        let point_adt = create_test_adt("Point");
        converter
            .type_registry
            .register_adt(&point_adt)
            .expect("Failed to register Point type");

        // Create a set of match arms
        let pattern1 = ast::Pattern::Literal(ast::Literal::Int64(1));
        let expr1 = ast::Expr::Literal(ast::Literal::String("one".to_string()));
        let arm1 = create_match_arm(pattern1, expr1);

        let pattern2 = ast::Pattern::Wildcard;
        let expr2 = ast::Expr::Literal(ast::Literal::String("other".to_string()));
        let arm2 = create_match_arm(pattern2, expr2);

        // Create a constructor pattern
        let pattern3 = ast::Pattern::Constructor(spanned("Point".to_string()), vec![]);
        let expr3 = ast::Expr::Literal(ast::Literal::String("point".to_string()));
        let arm3 = create_match_arm(pattern3, expr3);

        let arms = vec![arm1, arm2, arm3];

        // Convert the match arms
        let result = converter
            .convert_match_arms(&arms, &HashSet::new())
            .expect("Failed to convert match arms");

        // Check the converted result
        assert_eq!(result.len(), 3);

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

        // Check third arm's constructor pattern
        match &result[2].pattern.kind {
            PatternKind::Struct(name, _) => assert_eq!(name, "Point"),
            _ => panic!("Expected Struct pattern"),
        }

        // Test with invalid constructor - should fail
        let invalid_pattern = ast::Pattern::Constructor(spanned("UnknownType".to_string()), vec![]);
        let invalid_expr = ast::Expr::Literal(ast::Literal::String("invalid".to_string()));
        let invalid_arm = create_match_arm(invalid_pattern, invalid_expr);

        let invalid_arms = vec![invalid_arm];
        let result = converter.convert_match_arms(&invalid_arms, &HashSet::new());
        assert!(
            result.is_err(),
            "Expected error for undefined type in match arm"
        );
    }

    #[test]
    fn test_convert_patterns() {
        let mut converter = ASTConverter::default();

        // Register "Point" type for constructor pattern test
        let point_adt = create_test_adt("Point");
        converter
            .type_registry
            .register_adt(&point_adt)
            .expect("Failed to register Point type");

        // Test literal pattern
        let int_pattern = spanned(ast::Pattern::Literal(ast::Literal::Int64(42)));
        let result = converter
            .convert_pattern(&int_pattern)
            .expect("Literal pattern conversion should succeed");

        match &result.kind {
            PatternKind::Literal(Literal::Int64(val)) => assert_eq!(*val, 42),
            _ => panic!("Expected Int64 literal pattern"),
        }

        // Test wildcard pattern
        let wildcard_pattern = spanned(ast::Pattern::Wildcard);
        let result = converter
            .convert_pattern(&wildcard_pattern)
            .expect("Wildcard pattern conversion should succeed");

        match &result.kind {
            PatternKind::Wildcard => (),
            _ => panic!("Expected Wildcard pattern"),
        }

        // Test binding pattern
        let inner = spanned(ast::Pattern::Wildcard);
        let bind_pattern = spanned(ast::Pattern::Bind(spanned("x".to_string()), inner));

        let result = converter
            .convert_pattern(&bind_pattern)
            .expect("Binding pattern conversion should succeed");

        match &result.kind {
            PatternKind::Bind(name, _) => assert_eq!(name, "x"),
            _ => panic!("Expected Bind pattern"),
        }

        // Test constructor pattern with registered type
        let constructor_pattern = spanned(ast::Pattern::Constructor(
            spanned("Point".to_string()),
            vec![],
        ));

        let result = converter
            .convert_pattern(&constructor_pattern)
            .expect("Constructor pattern conversion should succeed");

        match &result.kind {
            PatternKind::Struct(name, _) => assert_eq!(name, "Point"),
            _ => panic!("Expected Struct pattern"),
        }

        // Test array patterns
        let empty_array_pattern = spanned(ast::Pattern::EmptyArray);
        let result = converter
            .convert_pattern(&empty_array_pattern)
            .expect("Empty array pattern conversion should succeed");

        match &result.kind {
            PatternKind::EmptyArray => (),
            _ => panic!("Expected EmptyArray pattern"),
        }

        // Test constructor pattern with unregistered type - should return error
        let unknown_constructor = spanned(ast::Pattern::Constructor(
            spanned("UnknownType".to_string()),
            vec![],
        ));
        let result = converter.convert_pattern(&unknown_constructor);
        assert!(
            result.is_err(),
            "Expected error for unknown type in constructor pattern"
        );
    }

    #[test]
    fn test_convert_array_decomp_pattern() {
        let converter = ASTConverter::default();

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

        let result = converter
            .convert_pattern(&array_decomp)
            .expect("Array decomposition pattern conversion should succeed");

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

    #[test]
    fn test_nested_constructor_patterns() {
        let mut converter = ASTConverter::default();

        // Register types for nested constructor patterns
        let types = ["Shape", "Circle", "Rectangle"];
        for ty in &types {
            let adt = create_test_adt(ty);
            converter
                .type_registry
                .register_adt(&adt)
                .expect(&format!("Failed to register {} type", ty));
        }

        // Test simple constructor pattern
        let shape_pattern = spanned(ast::Pattern::Constructor(
            spanned("Shape".to_string()),
            vec![],
        ));
        assert!(converter.convert_pattern(&shape_pattern).is_ok());

        // Test nested constructor patterns (all valid)
        let circle_inner = spanned(ast::Pattern::Constructor(
            spanned("Circle".to_string()),
            vec![],
        ));

        let nested_pattern = spanned(ast::Pattern::Constructor(
            spanned("Shape".to_string()),
            vec![circle_inner],
        ));

        let result = converter.convert_pattern(&nested_pattern);
        assert!(
            result.is_ok(),
            "Valid nested constructor pattern should succeed"
        );

        // Test nested with invalid inner constructor
        let invalid_inner = spanned(ast::Pattern::Constructor(
            spanned("InvalidType".to_string()),
            vec![],
        ));

        let invalid_nested = spanned(ast::Pattern::Constructor(
            spanned("Shape".to_string()),
            vec![invalid_inner],
        ));

        let result = converter.convert_pattern(&invalid_nested);
        assert!(
            result.is_err(),
            "Nested pattern with invalid constructor should fail"
        );
    }
}
