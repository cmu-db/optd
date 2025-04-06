//! Expression conversion from AST to HIR
//!
//! This module contains functions for converting AST expression nodes to their
//! corresponding HIR representations.

use super::pattern::convert_match_arms;
use super::types::{convert_type, create_function_type};
use crate::analyzer::hir::{
    BinOp, CoreData, Expr, ExprKind, FunKind, Identifier, Literal, TypedSpan, UnaryOp, Value,
};
use crate::analyzer::semantic_check::error::SemanticErrorKind;
use crate::analyzer::types::Type;
use crate::parser::ast;
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
) -> Result<Expr<TypedSpan>, SemanticErrorKind> {
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
        ast::Expr::Block(block) => convert_block(block, generics)?,
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
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
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
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
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
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
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
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
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
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
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
) -> Result<Vec<Arc<Expr<TypedSpan>>>, SemanticErrorKind> {
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
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
    let hir_elements = convert_expr_list(elements, generics)?;
    Ok(CoreExpr(CoreData::Array(hir_elements)))
}

/// Converts a tuple expression to an HIR expression kind.
fn convert_tuple(
    elements: &[Spanned<ast::Expr>],
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
    let hir_elements = convert_expr_list(elements, generics)?;
    Ok(CoreExpr(CoreData::Tuple(hir_elements)))
}

/// Converts a map expression to an HIR expression kind.
fn convert_map(
    entries: &[(Spanned<ast::Expr>, Spanned<ast::Expr>)],
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
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
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
    let hir_args = convert_expr_list(args, generics)?;

    Ok(CoreExpr(CoreData::Struct(name.clone(), hir_args)))
}

/// Converts a closure expression to an HIR expression kind.
fn convert_closure(
    params: &[(Identifier, Type)],
    body: &Spanned<ast::Expr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
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
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
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
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
    let hir_error = convert_expr(error_expr, generics)?;

    Ok(CoreExpr(CoreData::Fail(Box::new(Arc::new(hir_error)))))
}

/// Converts a block expression to an HIR expression kind.
fn convert_block(
    block: &Spanned<ast::Expr>,
    generics: &HashSet<Identifier>,
) -> Result<ExprKind<TypedSpan>, SemanticErrorKind> {
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
        let int_lit = spanned(ast::Expr::Literal(ast::Literal::Int64(42)));
        let result = convert_expr(&int_lit, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreVal(value) => match &value.data {
                CoreData::Literal(Literal::Int64(val)) => assert_eq!(*val, 42),
                _ => panic!("Expected Int64 literal"),
            },
            _ => panic!("Expected CoreVal"),
        }

        // Test string literal
        let str_lit = spanned(ast::Expr::Literal(ast::Literal::String(
            "hello".to_string(),
        )));
        let result = convert_expr(&str_lit, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreVal(value) => match &value.data {
                CoreData::Literal(Literal::String(val)) => assert_eq!(val, "hello"),
                _ => panic!("Expected String literal"),
            },
            _ => panic!("Expected CoreVal"),
        }

        // Test boolean literal
        let bool_lit = spanned(ast::Expr::Literal(ast::Literal::Bool(true)));
        let result = convert_expr(&bool_lit, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::CoreVal(value) => match &value.data {
                CoreData::Literal(Literal::Bool(val)) => assert_eq!(*val, true),
                _ => panic!("Expected Bool literal"),
            },
            _ => panic!("Expected CoreVal"),
        }

        // Test float literal
        let float_val = 3.14;
        let float_lit = spanned(ast::Expr::Literal(ast::Literal::Float64(
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
        let unit_lit = spanned(ast::Expr::Literal(ast::Literal::Unit));
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
        let ref_expr = spanned(ast::Expr::Ref("variable".to_string()));
        let result = convert_expr(&ref_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Ref(name) => assert_eq!(name, "variable"),
            _ => panic!("Expected Ref"),
        }
    }

    #[test]
    fn test_convert_binary_operators() {
        // Helper to create binary operator tests
        let test_binary_op = |op: ast::BinOp, expected_op: BinOp| {
            let left = spanned(ast::Expr::Literal(ast::Literal::Int64(1)));
            let right = spanned(ast::Expr::Literal(ast::Literal::Int64(2)));
            let bin_expr = spanned(ast::Expr::Binary(left, op, right));

            let result = convert_expr(&bin_expr, &HashSet::new()).unwrap();

            match &result.kind {
                ExprKind::Binary(_, actual_op, _) => assert_eq!(*actual_op, expected_op),
                _ => panic!("Expected Binary expression"),
            }
        };

        // Test standard binary operators
        test_binary_op(ast::BinOp::Add, BinOp::Add);
        test_binary_op(ast::BinOp::Sub, BinOp::Sub);
        test_binary_op(ast::BinOp::Mul, BinOp::Mul);
        test_binary_op(ast::BinOp::Div, BinOp::Div);
        test_binary_op(ast::BinOp::Lt, BinOp::Lt);
        test_binary_op(ast::BinOp::Eq, BinOp::Eq);
        test_binary_op(ast::BinOp::And, BinOp::And);
        test_binary_op(ast::BinOp::Or, BinOp::Or);
        test_binary_op(ast::BinOp::Range, BinOp::Range);
        test_binary_op(ast::BinOp::Concat, BinOp::Concat);
    }

    #[test]
    fn test_desugared_binary_operators() {
        // Test != (not equal) desugaring to !(left == right)
        let left = spanned(ast::Expr::Literal(ast::Literal::Int64(1)));
        let right = spanned(ast::Expr::Literal(ast::Literal::Int64(2)));
        let neq_expr = spanned(ast::Expr::Binary(left, ast::BinOp::Neq, right));

        let result = convert_expr(&neq_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Unary(op, _) => assert_eq!(*op, UnaryOp::Not),
            _ => panic!("Expected Unary expression (desugared !=)"),
        }

        // Test > (greater than) desugaring to right < left (swapped operands)
        let left = spanned(ast::Expr::Literal(ast::Literal::Int64(1)));
        let right = spanned(ast::Expr::Literal(ast::Literal::Int64(2)));
        let gt_expr = spanned(ast::Expr::Binary(left, ast::BinOp::Gt, right));

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
        let left = spanned(ast::Expr::Literal(ast::Literal::Int64(1)));
        let right = spanned(ast::Expr::Literal(ast::Literal::Int64(2)));
        let ge_expr = spanned(ast::Expr::Binary(left, ast::BinOp::Ge, right));

        let result = convert_expr(&ge_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Unary(op, _) => assert_eq!(*op, UnaryOp::Not),
            _ => panic!("Expected Unary expression (desugared >=)"),
        }

        // Test <= (less than or equal) desugaring to left < right || left == right
        let left = spanned(ast::Expr::Literal(ast::Literal::Int64(1)));
        let right = spanned(ast::Expr::Literal(ast::Literal::Int64(2)));
        let le_expr = spanned(ast::Expr::Binary(left, ast::BinOp::Le, right));

        let result = convert_expr(&le_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Binary(_, op, _) => assert_eq!(*op, BinOp::Or),
            _ => panic!("Expected Binary expression (desugared <=)"),
        }
    }

    #[test]
    fn test_convert_unary_operators() {
        // Test negation
        let operand = spanned(ast::Expr::Literal(ast::Literal::Int64(42)));
        let neg_expr = spanned(ast::Expr::Unary(ast::UnaryOp::Neg, operand));

        let result = convert_expr(&neg_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Unary(op, _) => assert_eq!(*op, UnaryOp::Neg),
            _ => panic!("Expected Unary expression"),
        }

        // Test logical not
        let operand = spanned(ast::Expr::Literal(ast::Literal::Bool(true)));
        let not_expr = spanned(ast::Expr::Unary(ast::UnaryOp::Not, operand));

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

        let init = spanned(ast::Expr::Literal(ast::Literal::Int64(42)));
        let body = spanned(ast::Expr::Ref(var_name.clone()));

        let let_expr = spanned(ast::Expr::Let(field, init, body));
        let result = convert_expr(&let_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::Let(name, _, _) => assert_eq!(name, &var_name),
            _ => panic!("Expected Let expression"),
        }
    }

    #[test]
    fn test_convert_if_then_else() {
        let condition = spanned(ast::Expr::Literal(ast::Literal::Bool(true)));
        let then_branch = spanned(ast::Expr::Literal(ast::Literal::Int64(1)));
        let else_branch = spanned(ast::Expr::Literal(ast::Literal::Int64(2)));

        let if_expr = spanned(ast::Expr::IfThenElse(condition, then_branch, else_branch));
        let result = convert_expr(&if_expr, &HashSet::new()).unwrap();

        match &result.kind {
            ExprKind::IfThenElse(_, _, _) => (),
            _ => panic!("Expected IfThenElse expression"),
        }
    }

    #[test]
    fn test_convert_array() {
        let elem1 = spanned(ast::Expr::Literal(ast::Literal::Int64(1)));
        let elem2 = spanned(ast::Expr::Literal(ast::Literal::Int64(2)));
        let elem3 = spanned(ast::Expr::Literal(ast::Literal::Int64(3)));

        let array_expr = spanned(ast::Expr::Array(vec![elem1, elem2, elem3]));
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
        let elem1 = spanned(ast::Expr::Literal(ast::Literal::Int64(1)));
        let elem2 = spanned(ast::Expr::Literal(ast::Literal::Bool(true)));
        let elem3 = spanned(ast::Expr::Literal(ast::Literal::String("test".to_string())));

        let tuple_expr = spanned(ast::Expr::Tuple(vec![elem1, elem2, elem3]));
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
        let key1 = spanned(ast::Expr::Literal(ast::Literal::String("key1".to_string())));
        let val1 = spanned(ast::Expr::Literal(ast::Literal::Int64(1)));

        let key2 = spanned(ast::Expr::Literal(ast::Literal::String("key2".to_string())));
        let val2 = spanned(ast::Expr::Literal(ast::Literal::Int64(2)));

        let map_expr = spanned(ast::Expr::Map(vec![(key1, val1), (key2, val2)]));
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
        let arg1 = spanned(ast::Expr::Literal(ast::Literal::Int64(1)));
        let arg2 = spanned(ast::Expr::Literal(ast::Literal::String("test".to_string())));

        let constructor_expr = spanned(ast::Expr::Constructor(
            "Point".to_string(),
            vec![arg1, arg2],
        ));
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

        let body = spanned(ast::Expr::Ref("x".to_string()));
        let closure_expr = spanned(ast::Expr::Closure(vec![param], body));
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
        let func = spanned(ast::Expr::Ref("add".to_string()));
        let arg1 = spanned(ast::Expr::Literal(ast::Literal::Int64(1)));
        let arg2 = spanned(ast::Expr::Literal(ast::Literal::Int64(2)));

        let call_expr = spanned(ast::Expr::Postfix(
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
        let obj = spanned(ast::Expr::Ref("point".to_string()));
        let field_name = spanned("x".to_string());

        let field_access_expr = spanned(ast::Expr::Postfix(obj, ast::PostfixOp::Field(field_name)));
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
        let obj = spanned(ast::Expr::Ref("list".to_string()));
        let method_name = spanned("add".to_string());
        let arg = spanned(ast::Expr::Literal(ast::Literal::Int64(42)));

        let method_call_expr = spanned(ast::Expr::Postfix(
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
        let error_expr = spanned(ast::Expr::Literal(ast::Literal::String(
            "error".to_string(),
        )));
        let fail_expr = spanned(ast::Expr::Fail(error_expr));
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
        let scrutinee = spanned(ast::Expr::Ref("value".to_string()));

        // Pattern 1: literal pattern
        let pattern1 = spanned(ast::Pattern::Literal(ast::Literal::Int64(0)));
        let expr1 = spanned(ast::Expr::Literal(ast::Literal::String("zero".to_string())));
        let arm1 = spanned(ast::MatchArm {
            pattern: pattern1,
            expr: expr1,
        });

        // Pattern 2: variable binding pattern
        let pattern2 = spanned(ast::Pattern::Bind(
            spanned("n".to_string()),
            spanned(ast::Pattern::Wildcard),
        ));
        let expr2 = spanned(ast::Expr::Ref("n".to_string()));
        let arm2 = spanned(ast::MatchArm {
            pattern: pattern2,
            expr: expr2,
        });

        let match_expr = spanned(ast::Expr::PatternMatch(scrutinee, vec![arm1, arm2]));
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
        let inner_expr = spanned(ast::Expr::Literal(ast::Literal::Int64(42)));
        let block_expr = spanned(ast::Expr::Block(inner_expr));

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
            ast::Expr::Literal(ast::Literal::Int64(42)),
            custom_span.clone(),
        );

        // Convert the expression
        let result = convert_expr(&expr, &HashSet::new()).unwrap();

        // Verify the span is preserved
        assert_eq!(result.metadata.span.src_file, "test_file.txt");
        assert_eq!(result.metadata.span.range, (10, 20));
    }
}
