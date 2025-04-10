//! Expression conversion from AST to HIR
//!
//! This module contains functions for converting AST expression nodes to their
//! corresponding HIR representations.

use super::pattern::convert_match_arms;
use super::types::{convert_type, create_function_type};
use crate::analyzer::error::AnalyzerErrorKind;
use crate::analyzer::hir::{
    BinOp, CoreData, Expr, ExprKind, FunKind, Identifier, Literal, TypedSpan, UnaryOp, Value,
};
use crate::analyzer::types::Type;
use crate::parser::ast::{self, BinOp as AstBinOp, Expr as AstExpr};
use crate::utils::span::{Span, Spanned};
use ExprKind::*;
use std::collections::HashSet;
use std::sync::Arc;

/// Converts an AST expression to an HIR expression.
///
/// This function is the main dispatcher for expression conversion, routing
/// the conversion to specialized functions based on the expression kind.
pub(super) fn convert_expr(
    spanned_expr: &Spanned<AstExpr>,
    generics: &HashSet<Identifier>,
) -> Result<Expr<TypedSpan>, AnalyzerErrorKind> {
    let span = spanned_expr.span.clone();
    let mut ty = Type::Unknown;

    let kind = match &*spanned_expr.value {
        AstExpr::Error => panic!("AST should no longer contain errors"),
        AstExpr::Literal(lit) => convert_literal(lit, &span),
        AstExpr::Ref(ident) => convert_ref(ident),
        AstExpr::Binary(left, op, right) => convert_binary(left, op, right, &span, generics)?,
        AstExpr::Unary(op, operand) => convert_unary(op, operand, generics)?,
        AstExpr::Let(field, init, body) => {
            // We extract the potential type annotation.
            ty = convert_type(&field.ty.value, generics);
            convert_let(field, init, body, generics)?
        }
        AstExpr::IfThenElse(condition, then_branch, else_branch) => {
            convert_if_then_else(condition, then_branch, else_branch, generics)?
        }
        AstExpr::PatternMatch(scrutinee, arms) => convert_pattern_match(scrutinee, arms, generics)?,
        AstExpr::Array(elements) => convert_array(elements, generics)?,
        AstExpr::Tuple(elements) => convert_tuple(elements, generics)?,
        AstExpr::Map(entries) => convert_map(entries, generics)?,
        AstExpr::Constructor(name, args) => convert_constructor(name, args, generics)?,
        AstExpr::Closure(params, body) => {
            // We extract the potential type annotations.
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
        AstExpr::Postfix(expr, op) => convert_postfix(expr, op, generics)?,
        AstExpr::Fail(error_expr) => convert_fail(error_expr, generics)?,
        AstExpr::Block(block) => convert_block(block, generics)?,
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
    left: &Spanned<AstExpr>,
    op: &AstBinOp,
    right: &Spanned<AstExpr>,
    span: &Span,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
    match op {
        AstBinOp::Add
        | AstBinOp::Sub
        | AstBinOp::Mul
        | AstBinOp::Div
        | AstBinOp::Lt
        | AstBinOp::Eq
        | AstBinOp::And
        | AstBinOp::Or
        | AstBinOp::Range
        | AstBinOp::Concat => {
            let hir_left = convert_expr(left, generics)?;
            let hir_right = convert_expr(right, generics)?;
            let hir_op = match op {
                AstBinOp::Add => BinOp::Add,
                AstBinOp::Sub => BinOp::Sub,
                AstBinOp::Mul => BinOp::Mul,
                AstBinOp::Div => BinOp::Div,
                AstBinOp::Lt => BinOp::Lt,
                AstBinOp::Eq => BinOp::Eq,
                AstBinOp::And => BinOp::And,
                AstBinOp::Or => BinOp::Or,
                AstBinOp::Range => BinOp::Range,
                AstBinOp::Concat => BinOp::Concat,
                _ => unreachable!(),
            };

            Ok(Binary(hir_left.into(), hir_op, hir_right.into()))
        }

        // Desugar not equal (!= becomes !(a == b)).
        AstBinOp::Neq => {
            let hir_left = convert_expr(left, generics)?;
            let hir_right = convert_expr(right, generics)?;

            let eq_expr = Expr::new_unknown(
                Binary(hir_left.into(), BinOp::Eq, hir_right.into()),
                span.clone(),
            );

            Ok(Unary(UnaryOp::Not, eq_expr.into()))
        }

        // Desugar greater than (> becomes b < a by swapping operands).
        AstBinOp::Gt => {
            let hir_left = convert_expr(right, generics)?;
            let hir_right = convert_expr(left, generics)?;

            Ok(Binary(hir_left.into(), BinOp::Lt, hir_right.into()))
        }

        // Desugar greater than or equal (>= becomes !(a < b)).
        AstBinOp::Ge => {
            let hir_left = convert_expr(left, generics)?;
            let hir_right = convert_expr(right, generics)?;

            let lt_expr = Expr::new_unknown(
                Binary(hir_left.into(), BinOp::Lt, hir_right.into()),
                span.clone(),
            );

            Ok(Unary(UnaryOp::Not, lt_expr.into()))
        }

        // Desugar less than or equal (<= becomes a < b || a == b).
        AstBinOp::Le => {
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
    operand: &Spanned<AstExpr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
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
    init: &Spanned<AstExpr>,
    body: &Spanned<AstExpr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
    let hir_init = convert_expr(init, generics)?;
    let hir_body = convert_expr(body, generics)?;
    let var_name = (*field.name).clone();

    Ok(Let(var_name, hir_init.into(), hir_body.into()))
}

/// Converts a pattern match expression to an HIR expression kind.
fn convert_pattern_match(
    scrutinee: &Spanned<AstExpr>,
    arms: &[Spanned<ast::MatchArm>],
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
    let hir_scrutinee = convert_expr(scrutinee, generics)?;
    let hir_arms = convert_match_arms(arms, generics)?;

    Ok(PatternMatch(hir_scrutinee.into(), hir_arms))
}

/// Converts an if-then-else expression to an HIR expression kind.
fn convert_if_then_else(
    condition: &Spanned<AstExpr>,
    then_branch: &Spanned<AstExpr>,
    else_branch: &Spanned<AstExpr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
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
    elements: &[Spanned<AstExpr>],
    generics: &HashSet<Identifier>,
) -> Result<Vec<Arc<Expr<TypedSpan>>>, AnalyzerErrorKind> {
    let mut hir_elements = Vec::with_capacity(elements.len());

    for elem in elements {
        let hir_elem = convert_expr(elem, generics)?;
        hir_elements.push(Arc::new(hir_elem));
    }

    Ok(hir_elements)
}

/// Converts an array expression to an HIR expression kind.
fn convert_array(
    elements: &[Spanned<AstExpr>],
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
    let hir_elements = convert_expr_list(elements, generics)?;
    Ok(CoreExpr(CoreData::Array(hir_elements)))
}

/// Converts a tuple expression to an HIR expression kind.
fn convert_tuple(
    elements: &[Spanned<AstExpr>],
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
    let hir_elements = convert_expr_list(elements, generics)?;
    Ok(CoreExpr(CoreData::Tuple(hir_elements)))
}

/// Converts a map expression to an HIR expression kind.
fn convert_map(
    entries: &[(Spanned<AstExpr>, Spanned<AstExpr>)],
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
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
    args: &[Spanned<AstExpr>],
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
    let hir_args = convert_expr_list(args, generics)?;

    Ok(CoreExpr(CoreData::Struct(name.clone(), hir_args)))
}

/// Converts a closure expression to an HIR expression kind.
fn convert_closure(
    params: &[(Identifier, Type)],
    body: &Spanned<AstExpr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
    let param_names = params.iter().map(|(name, _)| name.clone()).collect();
    let hir_body = convert_expr(body, generics)?;

    Ok(CoreExpr(CoreData::Function(FunKind::Closure(
        param_names,
        hir_body.into(),
    ))))
}

/// Converts a postfix expression to an HIR expression kind.
fn convert_postfix(
    expr: &Spanned<AstExpr>,
    op: &ast::PostfixOp,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
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
    error_expr: &Spanned<AstExpr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
    let hir_error = convert_expr(error_expr, generics)?;

    Ok(CoreExpr(CoreData::Fail(Box::new(Arc::new(hir_error)))))
}

/// Converts a block expression to an HIR expression kind.
fn convert_block(
    block: &Spanned<AstExpr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, AnalyzerErrorKind> {
    let hir_expr = convert_expr(block, generics)?;

    Ok(NewScope(hir_expr.into()))
}

#[cfg(test)]
mod expr_tests {
    use super::*;
    use crate::analyzer::hir::{BinOp, ExprKind, Literal, PatternKind, UnaryOp};
    use crate::parser::ast;
    use crate::utils::span::{Span, Spanned};
    use std::collections::HashSet;
    use std::f64::consts::PI;

    // Helper functions for creating test expressions
    fn create_test_span() -> Span {
        Span::new("test".to_string(), 0..1)
    }

    fn spanned<T>(value: T) -> Spanned<T> {
        Spanned::new(value, create_test_span())
    }

    #[test]
    fn test_convert_literal() {
        // Test integer literal
        let int_lit = spanned(AstExpr::Literal(ast::Literal::Int64(42)));
        let result = convert_expr(&int_lit, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreVal(value) => match &value.data {
                CoreData::Literal(Literal::Int64(val)) => assert_eq!(*val, 42),
                _ => panic!("Expected Int64 literal"),
            },
            _ => panic!("Expected CoreVal"),
        }

        // Test string literal
        let str_lit = spanned(AstExpr::Literal(ast::Literal::String("hello".to_string())));
        let result = convert_expr(&str_lit, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreVal(value) => match &value.data {
                CoreData::Literal(Literal::String(val)) => assert_eq!(val, "hello"),
                _ => panic!("Expected String literal"),
            },
            _ => panic!("Expected CoreVal"),
        }

        // Test boolean literal
        let bool_lit = spanned(AstExpr::Literal(ast::Literal::Bool(true)));
        let result = convert_expr(&bool_lit, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreVal(value) => match &value.data {
                CoreData::Literal(Literal::Bool(val)) => assert!(*val),
                _ => panic!("Expected Bool literal"),
            },
            _ => panic!("Expected CoreVal"),
        }

        // Test float literal
        let float_val = PI;
        let float_lit = spanned(AstExpr::Literal(ast::Literal::Float64(
            ordered_float::OrderedFloat(float_val),
        )));
        let result = convert_expr(&float_lit, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreVal(value) => match &value.data {
                CoreData::Literal(Literal::Float64(val)) => assert_eq!(*val, float_val),
                _ => panic!("Expected Float64 literal"),
            },
            _ => panic!("Expected CoreVal"),
        }

        // Test unit literal
        let unit_lit = spanned(AstExpr::Literal(ast::Literal::Unit));
        let result = convert_expr(&unit_lit, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreVal(value) => match &value.data {
                CoreData::Literal(Literal::Unit) => (),
                _ => panic!("Expected Unit literal"),
            },
            _ => panic!("Expected CoreVal"),
        }
    }

    #[test]
    fn test_convert_reference() {
        let ref_expr = spanned(AstExpr::Ref("variable".to_string()));
        let result = convert_expr(&ref_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Ref(name) => assert_eq!(name, "variable"),
            _ => panic!("Expected Ref"),
        }
    }

    #[test]
    fn test_convert_binary_operators() {
        // Helper to create binary operator tests
        let test_binary_op = |op: AstBinOp, expected_op: BinOp| {
            let left = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
            let right = spanned(AstExpr::Literal(ast::Literal::Int64(2)));
            let bin_expr = spanned(AstExpr::Binary(left, op, right));

            let result = convert_expr(&bin_expr, &HashSet::new()).unwrap();

            match &result.kind {
                ExprKind::Binary(_, actual_op, _) => assert_eq!(*actual_op, expected_op),
                _ => panic!("Expected Binary expression"),
            }
        };

        // Test standard binary operators
        test_binary_op(AstBinOp::Add, BinOp::Add);
        test_binary_op(AstBinOp::Sub, BinOp::Sub);
        test_binary_op(AstBinOp::Mul, BinOp::Mul);
        test_binary_op(AstBinOp::Div, BinOp::Div);
        test_binary_op(AstBinOp::Lt, BinOp::Lt);
        test_binary_op(AstBinOp::Eq, BinOp::Eq);
        test_binary_op(AstBinOp::And, BinOp::And);
        test_binary_op(AstBinOp::Or, BinOp::Or);
        test_binary_op(AstBinOp::Range, BinOp::Range);
        test_binary_op(AstBinOp::Concat, BinOp::Concat);
    }

    #[test]
    fn test_desugared_binary_operators() {
        // Test != (not equal) desugaring to !(left == right)
        let left = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let right = spanned(AstExpr::Literal(ast::Literal::Int64(2)));
        let neq_expr = spanned(AstExpr::Binary(left, AstBinOp::Neq, right));

        let result = convert_expr(&neq_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Unary(op, _) => assert_eq!(*op, UnaryOp::Not),
            _ => panic!("Expected Unary expression (desugared !=)"),
        }

        // Test > (greater than) desugaring to right < left (swapped operands)
        let left = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let right = spanned(AstExpr::Literal(ast::Literal::Int64(2)));
        let gt_expr = spanned(AstExpr::Binary(left, AstBinOp::Gt, right));

        let result = convert_expr(&gt_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Binary(left_expr, op, _) => {
                assert_eq!(*op, BinOp::Lt);

                // Verify operands are swapped (original right is now left)
                match &left_expr.kind {
                    ExprKind::CoreVal(value) => match &value.data {
                        CoreData::Literal(Literal::Int64(val)) => assert_eq!(*val, 2),
                        _ => panic!("Expected Int64 literal"),
                    },
                    _ => panic!("Expected CoreVal"),
                }
            }
            _ => panic!("Expected Binary expression (desugared >)"),
        }

        // Test >= (greater than or equal) desugaring to !(left < right)
        let left = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let right = spanned(AstExpr::Literal(ast::Literal::Int64(2)));
        let ge_expr = spanned(AstExpr::Binary(left, AstBinOp::Ge, right));

        let result = convert_expr(&ge_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Unary(op, _) => assert_eq!(*op, UnaryOp::Not),
            _ => panic!("Expected Unary expression (desugared >=)"),
        }

        // Test <= (less than or equal) desugaring to left < right || left == right
        let left = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let right = spanned(AstExpr::Literal(ast::Literal::Int64(2)));
        let le_expr = spanned(AstExpr::Binary(left, AstBinOp::Le, right));

        let result = convert_expr(&le_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Binary(_, op, _) => assert_eq!(*op, BinOp::Or),
            _ => panic!("Expected Binary expression (desugared <=)"),
        }
    }

    #[test]
    fn test_convert_unary_operators() {
        // Test negation
        let operand = spanned(AstExpr::Literal(ast::Literal::Int64(42)));
        let neg_expr = spanned(AstExpr::Unary(ast::UnaryOp::Neg, operand));

        let result = convert_expr(&neg_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Unary(op, _) => assert_eq!(*op, UnaryOp::Neg),
            _ => panic!("Expected Unary expression"),
        }

        // Test logical not
        let operand = spanned(AstExpr::Literal(ast::Literal::Bool(true)));
        let not_expr = spanned(AstExpr::Unary(ast::UnaryOp::Not, operand));

        let result = convert_expr(&not_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Unary(op, _) => assert_eq!(*op, UnaryOp::Not),
            _ => panic!("Expected Unary expression"),
        }
    }

    #[test]
    fn test_convert_let_expression() {
        let var_name = "x".to_string();
        let field = spanned(ast::Field {
            name: spanned(var_name.clone()),
            ty: spanned(ast::Type::Int64),
        });

        let init = spanned(AstExpr::Literal(ast::Literal::Int64(42)));
        let body = spanned(AstExpr::Ref(var_name.clone()));

        let let_expr = spanned(AstExpr::Let(field, init, body));
        let result = convert_expr(&let_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Let(name, _, _) => assert_eq!(name, &var_name),
            _ => panic!("Expected Let expression"),
        }
    }

    #[test]
    fn test_convert_if_then_else() {
        let condition = spanned(AstExpr::Literal(ast::Literal::Bool(true)));
        let then_branch = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let else_branch = spanned(AstExpr::Literal(ast::Literal::Int64(2)));

        let if_expr = spanned(AstExpr::IfThenElse(condition, then_branch, else_branch));
        let result = convert_expr(&if_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::IfThenElse(_, _, _) => (),
            _ => panic!("Expected IfThenElse expression"),
        }
    }

    #[test]
    fn test_convert_array() {
        let elem1 = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let elem2 = spanned(AstExpr::Literal(ast::Literal::Int64(2)));
        let elem3 = spanned(AstExpr::Literal(ast::Literal::Int64(3)));

        let array_expr = spanned(AstExpr::Array(vec![elem1, elem2, elem3]));
        let result = convert_expr(&array_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreExpr(CoreData::Array(elements)) => {
                assert_eq!(elements.len(), 3);
            }
            _ => panic!("Expected Array expression"),
        }
    }

    #[test]
    fn test_convert_tuple() {
        let elem1 = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let elem2 = spanned(AstExpr::Literal(ast::Literal::Bool(true)));
        let elem3 = spanned(AstExpr::Literal(ast::Literal::String("test".to_string())));

        let tuple_expr = spanned(AstExpr::Tuple(vec![elem1, elem2, elem3]));
        let result = convert_expr(&tuple_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreExpr(CoreData::Tuple(elements)) => {
                assert_eq!(elements.len(), 3);
            }
            _ => panic!("Expected Tuple expression"),
        }
    }

    #[test]
    fn test_convert_map() {
        let key1 = spanned(AstExpr::Literal(ast::Literal::String("key1".to_string())));
        let val1 = spanned(AstExpr::Literal(ast::Literal::Int64(1)));

        let key2 = spanned(AstExpr::Literal(ast::Literal::String("key2".to_string())));
        let val2 = spanned(AstExpr::Literal(ast::Literal::Int64(2)));

        let map_expr = spanned(AstExpr::Map(vec![(key1, val1), (key2, val2)]));
        let result = convert_expr(&map_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Map(entries) => {
                assert_eq!(entries.len(), 2);
            }
            _ => panic!("Expected Map expression"),
        }
    }

    #[test]
    fn test_convert_constructor() {
        let arg1 = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let arg2 = spanned(AstExpr::Literal(ast::Literal::String("test".to_string())));

        let constructor_expr = spanned(AstExpr::Constructor("Point".to_string(), vec![arg1, arg2]));
        let result = convert_expr(&constructor_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreExpr(CoreData::Struct(name, args)) => {
                assert_eq!(name, "Point");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected Struct expression"),
        }
    }

    #[test]
    fn test_convert_closure() {
        let param = spanned(ast::Field {
            name: spanned("x".to_string()),
            ty: spanned(ast::Type::Int64),
        });

        let body = spanned(AstExpr::Ref("x".to_string()));
        let closure_expr = spanned(AstExpr::Closure(vec![param], body));
        let result = convert_expr(&closure_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreExpr(CoreData::Function(FunKind::Closure(params, _))) => {
                assert_eq!(params.len(), 1);
                assert_eq!(params[0], "x");
            }
            _ => panic!("Expected Closure expression"),
        }
    }

    #[test]
    fn test_convert_function_call() {
        let func = spanned(AstExpr::Ref("add".to_string()));
        let arg1 = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let arg2 = spanned(AstExpr::Literal(ast::Literal::Int64(2)));

        let call_expr = spanned(AstExpr::Postfix(
            func,
            ast::PostfixOp::Call(vec![arg1, arg2]),
        ));
        let result = convert_expr(&call_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Call(_, args) => {
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected Call expression"),
        }
    }

    #[test]
    fn test_convert_field_access() {
        let obj = spanned(AstExpr::Ref("point".to_string()));
        let field_name = spanned("x".to_string());

        let field_access_expr = spanned(AstExpr::Postfix(obj, ast::PostfixOp::Field(field_name)));
        let result = convert_expr(&field_access_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::FieldAccess(obj, field) => {
                assert_eq!(field, "x");

                match &obj.kind {
                    ExprKind::Ref(name) => assert_eq!(name, "point"),
                    _ => panic!("Expected Ref expression"),
                }
            }
            _ => panic!("Expected FieldAccess expression"),
        }
    }

    #[test]
    fn test_convert_method_call() {
        let obj = spanned(AstExpr::Ref("list".to_string()));
        let method_name = spanned("add".to_string());
        let arg = spanned(AstExpr::Literal(ast::Literal::Int64(42)));

        let method_call_expr = spanned(AstExpr::Postfix(
            obj,
            ast::PostfixOp::Method(method_name, vec![arg]),
        ));
        let result = convert_expr(&method_call_expr, &HashSet::new()).unwrap();

        // Method calls get desugared to function calls with the object as the first argument
        match &result.kind {
            ExprKind::Call(fn_expr, args) => {
                // Check method name
                match &fn_expr.kind {
                    ExprKind::Ref(name) => assert_eq!(name, "add"),
                    _ => panic!("Expected Ref expression for method name"),
                }

                // Should have 2 arguments (obj + original arg)
                assert_eq!(args.len(), 2);

                // First arg should be the object
                match &args[0].kind {
                    ExprKind::Ref(name) => assert_eq!(name, "list"),
                    _ => panic!("Expected Ref expression for object"),
                }
            }
            _ => panic!("Expected Call expression"),
        }
    }

    #[test]
    fn test_convert_fail() {
        let error_expr = spanned(AstExpr::Literal(ast::Literal::String("error".to_string())));
        let fail_expr = spanned(AstExpr::Fail(error_expr));
        let result = convert_expr(&fail_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreExpr(CoreData::Fail(error)) => match &error.kind {
                ExprKind::CoreVal(value) => match &value.data {
                    CoreData::Literal(Literal::String(msg)) => assert_eq!(msg, "error"),
                    _ => panic!("Expected String literal"),
                },
                _ => panic!("Expected CoreVal"),
            },
            _ => panic!("Expected Fail expression"),
        }
    }

    #[test]
    fn test_convert_pattern_match() {
        // Create a simple pattern match expression
        let scrutinee = spanned(AstExpr::Ref("value".to_string()));

        // Pattern 1: literal pattern
        let pattern1 = spanned(ast::Pattern::Literal(ast::Literal::Int64(0)));
        let expr1 = spanned(AstExpr::Literal(ast::Literal::String("zero".to_string())));
        let arm1 = spanned(ast::MatchArm {
            pattern: pattern1,
            expr: expr1,
        });

        // Pattern 2: variable binding pattern
        let pattern2 = spanned(ast::Pattern::Bind(
            spanned("n".to_string()),
            spanned(ast::Pattern::Wildcard),
        ));
        let expr2 = spanned(AstExpr::Ref("n".to_string()));
        let arm2 = spanned(ast::MatchArm {
            pattern: pattern2,
            expr: expr2,
        });

        let match_expr = spanned(AstExpr::PatternMatch(scrutinee, vec![arm1, arm2]));
        let result = convert_expr(&match_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::PatternMatch(scrutinee_expr, arms) => {
                // Verify scrutinee
                match &scrutinee_expr.kind {
                    ExprKind::Ref(name) => assert_eq!(name, "value"),
                    _ => panic!("Expected Ref expression"),
                }

                // Verify arms count
                assert_eq!(arms.len(), 2);

                // Check first arm pattern (literal)
                match &arms[0].pattern.kind {
                    PatternKind::Literal(Literal::Int64(val)) => assert_eq!(*val, 0),
                    _ => panic!("Expected Literal pattern"),
                }

                // Check second arm pattern (binding)
                match &arms[1].pattern.kind {
                    PatternKind::Bind(name, _) => assert_eq!(name, "n"),
                    _ => panic!("Expected Bind pattern"),
                }
            }
            _ => panic!("Expected PatternMatch expression"),
        }
    }

    #[test]
    fn test_convert_block() {
        // Create a simple expression inside a block
        let inner_expr = spanned(AstExpr::Literal(ast::Literal::Int64(42)));
        let block_expr = spanned(AstExpr::Block(inner_expr));

        let result = convert_expr(&block_expr, &HashSet::new()).unwrap();

        // Check that the block is converted to a NewScope expression
        match &result.kind {
            ExprKind::NewScope(expr) => {
                // Verify the inner expression
                match &expr.kind {
                    ExprKind::CoreVal(value) => match &value.data {
                        CoreData::Literal(Literal::Int64(val)) => assert_eq!(*val, 42),
                        _ => panic!("Expected Int64 literal inside block"),
                    },
                    _ => panic!("Expected CoreVal inside block"),
                }
            }
            _ => panic!("Expected NewScope expression"),
        }
    }

    #[test]
    fn test_expr_conversion_preserves_span() {
        // Create a span with specific location
        let custom_span = Span::new("test_file.txt".to_string(), 10..20);

        // Create a simple expression with that span
        let expr = Spanned::new(
            AstExpr::Literal(ast::Literal::Int64(42)),
            custom_span.clone(),
        );

        // Convert the expression
        let result = convert_expr(&expr, &HashSet::new()).unwrap();

        // Verify the span is preserved
        assert_eq!(result.metadata.span.src_file, "test_file.txt");
        assert_eq!(result.metadata.span.range, (10, 20));
    }
}
