//! Pattern conversion from AST to HIR
//!
//! This module contains functions for converting AST pattern nodes to their
//! corresponding HIR representations.

use super::ASTConverter;
use crate::dsl::analyzer::errors::AnalyzerErrorKind;
use crate::dsl::analyzer::hir::{Identifier, MatchArm, Pattern, PatternKind, TypedSpan};
use crate::dsl::analyzer::types::registry::Type;
use crate::dsl::parser::ast::{self, Pattern as AstPattern};
use crate::dsl::utils::span::Spanned;
use std::collections::HashSet;
use std::sync::Arc;

impl ASTConverter {
    /// Converts a list of AST match arms to HIR match arms.
    ///
    /// This function is the main entry point for pattern conversion, handling both
    /// the patterns and expressions in match arms.
    pub(super) fn convert_match_arms(
        &mut self,
        arms: &[Spanned<ast::MatchArm>],
        generics: &HashSet<Identifier>,
    ) -> Result<Vec<MatchArm<TypedSpan>>, Box<AnalyzerErrorKind>> {
        arms.iter()
            .map(|arm| {
                let pattern = self.convert_pattern(&arm.pattern, &None)?;
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
    /// Each pattern kind determines its own specific type, which will be used
    /// during pattern matching and constraint solving.
    fn convert_pattern(
        &mut self,
        spanned_pattern: &Spanned<AstPattern>,
        parent_adt: &Option<(Identifier, usize)>,
    ) -> Result<Pattern<TypedSpan>, Box<AnalyzerErrorKind>> {
        use PatternKind::*;

        let span = spanned_pattern.span.clone();
        let mut ty = self.registry.new_unknown();

        let kind = match &*spanned_pattern.value {
            AstPattern::Error => panic!("AST should no longer contain errors"),
            AstPattern::Bind(name, inner_pattern) => {
                let hir_inner = self.convert_pattern(inner_pattern, parent_adt)?;
                ty = hir_inner.metadata.ty.clone();

                Bind((*name.value).clone(), hir_inner.into())
            }
            AstPattern::Constructor(name, args) => {
                self.validate_constructor(name, &span, args.len())?;
                ty = Type::Adt(*name.value.clone());

                let hir_args = args
                    .iter()
                    .enumerate()
                    .map(|(idx, arg)| self.convert_pattern(arg, &Some((*name.value.clone(), idx))))
                    .collect::<Result<Vec<_>, _>>()?;

                Struct((*name.value).clone(), hir_args)
            }
            AstPattern::Literal(lit) => {
                let (hir_lit, hir_ty) = self.convert_literal(lit);
                ty = hir_ty;

                Literal(hir_lit)
            }
            AstPattern::Wildcard => {
                // Add implicit type annotation when in field of struct.
                if let Some((name, idx)) = parent_adt {
                    ty = self
                        .registry
                        .get_product_field_type_by_index(name, *idx)
                        .unwrap();
                }

                Wildcard
            }
            AstPattern::EmptyArray => {
                ty = Type::Array(self.registry.new_unknown().into());

                EmptyArray
            }
            AstPattern::ArrayDecomp(head, tail) => {
                let hir_head = self.convert_pattern(head, &None)?;
                let hir_tail = self.convert_pattern(tail, &None)?;

                ArrayDecomp(hir_head.into(), hir_tail.into())
            }
        };

        Ok(Pattern::new_with(kind, ty, span))
    }
}

#[cfg(test)]
mod pattern_tests {
    use crate::dsl::analyzer::from_ast::ASTConverter;
    use crate::dsl::analyzer::hir::{Literal, PatternKind};
    use crate::dsl::parser::ast::{self, Field, Literal as AstLiteral, Pattern as AstPattern};
    use crate::dsl::utils::span::{Span, Spanned};
    use std::collections::HashSet;

    // Helper functions to create test patterns
    fn create_test_span() -> Span {
        Span::new("test".to_string(), 0..1)
    }

    fn spanned<T>(value: T) -> Spanned<T> {
        Spanned::new(value, create_test_span())
    }

    fn create_match_arm(pattern: AstPattern, expr: ast::Expr) -> Spanned<ast::MatchArm> {
        spanned(ast::MatchArm {
            pattern: spanned(pattern),
            expr: spanned(expr),
        })
    }

    fn create_product_adt(name: &str, field_count: usize) -> ast::Adt {
        let fields = (0..field_count)
            .map(|i| {
                spanned(Field {
                    name: spanned(format!("field{}", i)),
                    ty: spanned(ast::Type::Int64),
                })
            })
            .collect();

        ast::Adt::Product {
            name: spanned(name.to_string()),
            fields,
        }
    }

    #[test]
    fn test_convert_match_arms() {
        let mut converter = ASTConverter::default();

        // Register "Point" type for constructor patterns in match arms
        // with 2 fields
        let point_adt = create_product_adt("Point", 2);
        converter
            .registry
            .register_adt(&point_adt)
            .expect("Failed to register Point type");

        // Create a set of match arms
        let pattern1 = AstPattern::Literal(AstLiteral::Int64(1));
        let expr1 = ast::Expr::Literal(AstLiteral::String("one".to_string()));
        let arm1 = create_match_arm(pattern1, expr1);

        let pattern2 = AstPattern::Wildcard;
        let expr2 = ast::Expr::Literal(AstLiteral::String("other".to_string()));
        let arm2 = create_match_arm(pattern2, expr2);

        // Create a constructor pattern with the correct number of fields (2)
        let pattern3 = AstPattern::Constructor(
            spanned("Point".to_string()),
            vec![spanned(AstPattern::Wildcard), spanned(AstPattern::Wildcard)],
        );
        let expr3 = ast::Expr::Literal(AstLiteral::String("point".to_string()));
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
            PatternKind::Struct(name, args) => {
                assert_eq!(name, "Point");
                assert_eq!(args.len(), 2); // Ensure it has the right number of args
            }
            _ => panic!("Expected Struct pattern"),
        }

        // Test with invalid constructor - should fail
        let invalid_pattern = AstPattern::Constructor(
            spanned("UnknownType".to_string()),
            vec![spanned(AstPattern::Wildcard), spanned(AstPattern::Wildcard)],
        );
        let invalid_expr = ast::Expr::Literal(AstLiteral::String("invalid".to_string()));
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

        // Register "Point" type for constructor pattern test with 2 fields
        let point_adt = create_product_adt("Point", 2);
        converter
            .registry
            .register_adt(&point_adt)
            .expect("Failed to register Point type");

        // Test literal pattern
        let int_pattern = spanned(AstPattern::Literal(AstLiteral::Int64(42)));
        let result = converter
            .convert_pattern(&int_pattern, &None)
            .expect("Literal pattern conversion should succeed");

        match &result.kind {
            PatternKind::Literal(Literal::Int64(val)) => assert_eq!(*val, 42),
            _ => panic!("Expected Int64 literal pattern"),
        }

        // Test wildcard pattern
        let wildcard_pattern = spanned(AstPattern::Wildcard);
        let result = converter
            .convert_pattern(&wildcard_pattern, &None)
            .expect("Wildcard pattern conversion should succeed");

        match &result.kind {
            PatternKind::Wildcard => (),
            _ => panic!("Expected Wildcard pattern"),
        }

        // Test binding pattern
        let inner = spanned(AstPattern::Wildcard);
        let bind_pattern = spanned(AstPattern::Bind(spanned("x".to_string()), inner));

        let result = converter
            .convert_pattern(&bind_pattern, &None)
            .expect("Binding pattern conversion should succeed");

        match &result.kind {
            PatternKind::Bind(name, _) => assert_eq!(name, "x"),
            _ => panic!("Expected Bind pattern"),
        }

        // Test constructor pattern with registered type and correct field count
        let constructor_pattern = spanned(AstPattern::Constructor(
            spanned("Point".to_string()),
            vec![spanned(AstPattern::Wildcard), spanned(AstPattern::Wildcard)],
        ));

        let result = converter
            .convert_pattern(&constructor_pattern, &None)
            .expect("Constructor pattern conversion should succeed");

        match &result.kind {
            PatternKind::Struct(name, fields) => {
                assert_eq!(name, "Point");
                assert_eq!(fields.len(), 2);
            }
            _ => panic!("Expected Struct pattern"),
        }

        // Test array patterns
        let empty_array_pattern = spanned(AstPattern::EmptyArray);
        let result = converter
            .convert_pattern(&empty_array_pattern, &None)
            .expect("Empty array pattern conversion should succeed");

        match &result.kind {
            PatternKind::EmptyArray => (),
            _ => panic!("Expected EmptyArray pattern"),
        }

        // Test constructor pattern with unregistered type - should return error
        let unknown_constructor = spanned(AstPattern::Constructor(
            spanned("UnknownType".to_string()),
            vec![spanned(AstPattern::Wildcard), spanned(AstPattern::Wildcard)],
        ));
        let result = converter.convert_pattern(&unknown_constructor, &None);
        assert!(
            result.is_err(),
            "Expected error for unknown type in constructor pattern"
        );
    }

    #[test]
    fn test_convert_array_decomp_pattern() {
        let mut converter = ASTConverter::default();

        // Create a head::tail pattern
        let head = spanned(AstPattern::Bind(
            spanned("head".to_string()),
            spanned(AstPattern::Wildcard),
        ));
        let tail = spanned(AstPattern::Bind(
            spanned("tail".to_string()),
            spanned(AstPattern::Wildcard),
        ));

        let array_decomp = spanned(AstPattern::ArrayDecomp(head, tail));

        let result = converter
            .convert_pattern(&array_decomp, &None)
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

        // Register types for nested constructor patterns with field counts
        let types = [("Shape", 1), ("Circle", 1), ("Rectangle", 1)];

        for (name, field_count) in &types {
            let adt = create_product_adt(name, *field_count);
            converter
                .registry
                .register_adt(&adt)
                .unwrap_or_else(|_| panic!("Failed to register {} type", name));
        }

        // Test simple constructor pattern with correct field count
        let shape_pattern = spanned(AstPattern::Constructor(
            spanned("Shape".to_string()),
            vec![spanned(AstPattern::Wildcard)],
        ));
        assert!(converter.convert_pattern(&shape_pattern, &None).is_ok());

        // Test nested constructor patterns (all valid)
        let circle_inner = spanned(AstPattern::Constructor(
            spanned("Circle".to_string()),
            vec![spanned(AstPattern::Wildcard)],
        ));

        let nested_pattern = spanned(AstPattern::Constructor(
            spanned("Shape".to_string()),
            vec![circle_inner],
        ));

        let result = converter.convert_pattern(&nested_pattern, &None);
        assert!(
            result.is_ok(),
            "Valid nested constructor pattern should succeed"
        );

        // Test nested with invalid inner constructor
        let invalid_inner = spanned(AstPattern::Constructor(
            spanned("InvalidType".to_string()),
            vec![spanned(AstPattern::Wildcard)],
        ));

        let invalid_nested = spanned(AstPattern::Constructor(
            spanned("Shape".to_string()),
            vec![invalid_inner],
        ));

        let result = converter.convert_pattern(&invalid_nested, &None);
        assert!(
            result.is_err(),
            "Nested pattern with invalid constructor should fail"
        );
    }

    #[test]
    fn test_constructor_pattern_field_count_validation() {
        let mut converter = ASTConverter::default();

        // Register a Point type with 2 fields
        let point_adt = create_product_adt("Point", 2);
        converter
            .registry
            .register_adt(&point_adt)
            .expect("Failed to register Point type");

        // Test with correct number of arguments (2)
        let correct_pattern = spanned(AstPattern::Constructor(
            spanned("Point".to_string()),
            vec![spanned(AstPattern::Wildcard), spanned(AstPattern::Wildcard)],
        ));

        let result = converter.convert_pattern(&correct_pattern, &None);
        assert!(
            result.is_ok(),
            "Constructor pattern with correct field count should succeed"
        );

        // Test with too few arguments (1 instead of 2)
        let too_few_args = spanned(AstPattern::Constructor(
            spanned("Point".to_string()),
            vec![spanned(AstPattern::Wildcard)],
        ));

        let result = converter.convert_pattern(&too_few_args, &None);
        assert!(
            result.is_err(),
            "Constructor pattern with too few arguments should fail"
        );

        // Test with too many arguments (3 instead of 2)
        let too_many_args = spanned(AstPattern::Constructor(
            spanned("Point".to_string()),
            vec![
                spanned(AstPattern::Wildcard),
                spanned(AstPattern::Wildcard),
                spanned(AstPattern::Wildcard),
            ],
        ));

        let result = converter.convert_pattern(&too_many_args, &None);
        assert!(
            result.is_err(),
            "Constructor pattern with too many arguments should fail"
        );
    }
}
