//! Expression conversion from AST to HIR
//!
//! This module contains functions for converting AST expression nodes to their
//! corresponding HIR representations.

use super::ASTConverter;
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

impl ASTConverter {
    /// Converts an AST expression to an HIR expression.
    ///
    /// This function is the main dispatcher for expression conversion, routing
    /// the conversion to specialized functions based on the expression kind.
    pub(super) fn convert_expr(
        &self,
        spanned_expr: &Spanned<AstExpr>,
        generics: &HashSet<Identifier>,
    ) -> Result<Expr<TypedSpan>, Box<AnalyzerErrorKind>> {
        let span = spanned_expr.span.clone();
        let mut ty = Type::Unknown;

        let kind = match &*spanned_expr.value {
            AstExpr::Error => panic!("AST should no longer contain errors"),
            AstExpr::Literal(lit) => self.convert_literal(lit, &span),
            AstExpr::Ref(ident) => self.convert_ref(ident),
            AstExpr::Binary(left, op, right) => {
                self.convert_binary(left, op, right, &span, generics)?
            }
            AstExpr::Unary(op, operand) => self.convert_unary(op, operand, generics)?,
            AstExpr::Let(field, init, body) => {
                // We extract the potential type annotation.
                ty = self.convert_type(&field.ty, generics)?;
                self.convert_let(field, init, body, generics)?
            }
            AstExpr::IfThenElse(condition, then_branch, else_branch) => {
                self.convert_if_then_else(condition, then_branch, else_branch, generics)?
            }
            AstExpr::PatternMatch(scrutinee, arms) => {
                self.convert_pattern_match(scrutinee, arms, generics)?
            }
            AstExpr::Array(elements) => self.convert_array(elements, generics)?,
            AstExpr::Tuple(elements) => self.convert_tuple(elements, generics)?,
            AstExpr::Map(entries) => self.convert_map(entries, generics)?,
            AstExpr::Constructor(name, args) => self.convert_constructor(name, args, generics)?,
            AstExpr::Closure(params, body) => {
                // We extract the potential type annotations.
                let params = params
                    .iter()
                    .map(|field| -> Result<_, _> {
                        Ok((
                            (*field.name).clone(),
                            self.convert_type(&field.ty, generics)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, Box<AnalyzerErrorKind>>>()?;

                ty = Self::create_function_type(&params, &Type::Unknown);
                self.convert_closure(&params, body, generics)?
            }
            AstExpr::Postfix(expr, op) => self.convert_postfix(expr, op, generics)?,
            AstExpr::Fail(error_expr) => self.convert_fail(error_expr, generics)?,
            AstExpr::None => CoreVal(Value::new_unknown(CoreData::None, span.clone())),
            AstExpr::Block(block) => self.convert_block(block, generics)?,
        };

        Ok(Expr::new_with(kind, ty, span))
    }

    fn convert_literal(&self, literal: &ast::Literal, span: &Span) -> ExprKind<TypedSpan> {
        let hir_lit = match literal {
            ast::Literal::Int64(val) => Literal::Int64(*val),
            ast::Literal::String(val) => Literal::String(val.clone()),
            ast::Literal::Bool(val) => Literal::Bool(*val),
            ast::Literal::Float64(val) => Literal::Float64(val.0),
            ast::Literal::Unit => Literal::Unit,
        };

        CoreVal(Value::new_unknown(CoreData::Literal(hir_lit), span.clone()))
    }

    fn convert_ref(&self, ident: &Identifier) -> ExprKind<TypedSpan> {
        Ref(ident.clone())
    }

    fn convert_binary(
        &self,
        left: &Spanned<AstExpr>,
        op: &AstBinOp,
        right: &Spanned<AstExpr>,
        span: &Span,
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        use BinOp::*;

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
                let hir_left = self.convert_expr(left, generics)?;
                let hir_right = self.convert_expr(right, generics)?;
                let hir_op = match op {
                    AstBinOp::Add => Add,
                    AstBinOp::Sub => Sub,
                    AstBinOp::Mul => Mul,
                    AstBinOp::Div => Div,
                    AstBinOp::Lt => Lt,
                    AstBinOp::Eq => Eq,
                    AstBinOp::And => And,
                    AstBinOp::Or => Or,
                    AstBinOp::Range => Range,
                    AstBinOp::Concat => Concat,
                    _ => unreachable!(),
                };

                Ok(Binary(hir_left.into(), hir_op, hir_right.into()))
            }

            // Desugar not equal (!= becomes !(a == b)).
            AstBinOp::Neq => {
                let hir_left = self.convert_expr(left, generics)?;
                let hir_right = self.convert_expr(right, generics)?;

                let eq_expr =
                    Expr::new_unknown(Binary(hir_left.into(), Eq, hir_right.into()), span.clone());

                Ok(Unary(UnaryOp::Not, eq_expr.into()))
            }

            // Desugar greater than (> becomes b < a by swapping operands).
            AstBinOp::Gt => {
                let hir_left = self.convert_expr(right, generics)?;
                let hir_right = self.convert_expr(left, generics)?;

                Ok(Binary(hir_left.into(), Lt, hir_right.into()))
            }

            // Desugar greater than or equal (>= becomes !(a < b)).
            AstBinOp::Ge => {
                let hir_left = self.convert_expr(left, generics)?;
                let hir_right = self.convert_expr(right, generics)?;

                let lt_expr =
                    Expr::new_unknown(Binary(hir_left.into(), Lt, hir_right.into()), span.clone());

                Ok(Unary(UnaryOp::Not, lt_expr.into()))
            }

            // Desugar less than or equal (<= becomes a < b || a == b).
            AstBinOp::Le => {
                let hir_left = Arc::new(self.convert_expr(left, generics)?);
                let hir_right = Arc::new(self.convert_expr(right, generics)?);

                let lt_expr = Expr::new_unknown(
                    Binary(hir_left.clone(), Lt, hir_right.clone()),
                    span.clone(),
                );
                let eq_expr = Expr::new_unknown(Binary(hir_left, Eq, hir_right), span.clone());

                Ok(Binary(lt_expr.into(), Or, eq_expr.into()))
            }
        }
    }

    fn convert_unary(
        &self,
        op: &ast::UnaryOp,
        operand: &Spanned<AstExpr>,
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        let hir_operand = self.convert_expr(operand, generics)?;

        let hir_op = match op {
            ast::UnaryOp::Neg => UnaryOp::Neg,
            ast::UnaryOp::Not => UnaryOp::Not,
        };

        Ok(Unary(hir_op, hir_operand.into()))
    }

    fn convert_let(
        &self,
        field: &Spanned<ast::Field>,
        init: &Spanned<AstExpr>,
        body: &Spanned<AstExpr>,
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        let hir_init = self.convert_expr(init, generics)?;
        let hir_body = self.convert_expr(body, generics)?;
        let var_name = (*field.name).clone();

        Ok(Let(var_name, hir_init.into(), hir_body.into()))
    }

    fn convert_pattern_match(
        &self,
        scrutinee: &Spanned<AstExpr>,
        arms: &[Spanned<ast::MatchArm>],
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        let hir_scrutinee = self.convert_expr(scrutinee, generics)?;
        let hir_arms = self.convert_match_arms(arms, generics)?;

        Ok(PatternMatch(hir_scrutinee.into(), hir_arms))
    }

    fn convert_if_then_else(
        &self,
        condition: &Spanned<AstExpr>,
        then_branch: &Spanned<AstExpr>,
        else_branch: &Spanned<AstExpr>,
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        let hir_condition = self.convert_expr(condition, generics)?;
        let hir_then = self.convert_expr(then_branch, generics)?;
        let hir_else = self.convert_expr(else_branch, generics)?;

        Ok(IfThenElse(
            hir_condition.into(),
            hir_then.into(),
            hir_else.into(),
        ))
    }

    fn convert_expr_list(
        &self,
        elements: &[Spanned<AstExpr>],
        generics: &HashSet<Identifier>,
    ) -> Result<Vec<Arc<Expr<TypedSpan>>>, Box<AnalyzerErrorKind>> {
        let mut hir_elements = Vec::with_capacity(elements.len());

        for elem in elements {
            let hir_elem = self.convert_expr(elem, generics)?;
            hir_elements.push(hir_elem.into());
        }

        Ok(hir_elements)
    }

    fn convert_array(
        &self,
        elements: &[Spanned<AstExpr>],
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        let hir_elements = self.convert_expr_list(elements, generics)?;
        Ok(CoreExpr(CoreData::Array(hir_elements)))
    }

    fn convert_tuple(
        &self,
        elements: &[Spanned<AstExpr>],
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        let hir_elements = self.convert_expr_list(elements, generics)?;
        Ok(CoreExpr(CoreData::Tuple(hir_elements)))
    }

    fn convert_map(
        &self,
        entries: &[(Spanned<AstExpr>, Spanned<AstExpr>)],
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        let mut hir_entries = Vec::with_capacity(entries.len());

        for (key, value) in entries {
            let key = self.convert_expr(key, generics)?;
            let value = self.convert_expr(value, generics)?;
            hir_entries.push((key.into(), value.into()));
        }

        Ok(Map(hir_entries))
    }

    fn convert_constructor(
        &self,
        name: &Spanned<Identifier>,
        args: &[Spanned<AstExpr>],
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        let hir_args = self.convert_expr_list(args, generics)?;

        // Check if the corresponding type exists.
        if !self.type_registry.subtypes.contains_key(&*name.value) {
            return Err(AnalyzerErrorKind::new_undefined_type(
                &name.value,
                &name.span,
            ));
        }

        Ok(CoreExpr(CoreData::Struct(*name.value.clone(), hir_args)))
    }

    fn convert_closure(
        &self,
        params: &[(Identifier, Type)],
        body: &Spanned<AstExpr>,
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        let param_names = params.iter().map(|(name, _)| name.clone()).collect();
        let hir_body = self.convert_expr(body, generics)?;

        Ok(CoreExpr(CoreData::Function(FunKind::Closure(
            param_names,
            hir_body.into(),
        ))))
    }

    fn convert_postfix(
        &self,
        expr: &Spanned<AstExpr>,
        op: &ast::PostfixOp,
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        let hir_expr = self.convert_expr(expr, generics)?;

        match op {
            ast::PostfixOp::Call(args) => {
                let hir_args = self.convert_expr_list(args, generics)?;
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
                    .chain(self.convert_expr_list(args, generics)?)
                    .collect();

                let method_fn = Arc::new(Expr::new_unknown(
                    Ref((*method_name.value).clone()),
                    method_name.span.clone(),
                ));

                Ok(Call(method_fn, all_args))
            }
        }
    }

    fn convert_fail(
        &self,
        error_expr: &Spanned<AstExpr>,
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        let hir_error = self.convert_expr(error_expr, generics)?;

        Ok(CoreExpr(CoreData::Fail(Box::new(Arc::new(hir_error)))))
    }

    fn convert_block(
        &self,
        block: &Spanned<AstExpr>,
        generics: &HashSet<Identifier>,
    ) -> Result<ExprKind<TypedSpan>, Box<AnalyzerErrorKind>> {
        let hir_expr = self.convert_expr(block, generics)?;

        Ok(NewScope(hir_expr.into()))
    }
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

    fn create_test_adt(name: &str) -> ast::Adt {
        ast::Adt::Product {
            name: spanned(name.to_string()),
            fields: vec![],
        }
    }

    #[test]
    fn test_convert_literal() {
        let converter = ASTConverter::default();

        // Test integer literal
        let int_lit = spanned(AstExpr::Literal(ast::Literal::Int64(42)));
        let result = converter
            .convert_expr(&int_lit, &HashSet::new())
            .expect("Integer literal conversion should succeed");

        match &result.kind {
            ExprKind::CoreVal(value) => match &value.data {
                CoreData::Literal(Literal::Int64(val)) => assert_eq!(*val, 42),
                _ => panic!("Expected Int64 literal"),
            },
            _ => panic!("Expected CoreVal"),
        }

        // Test string literal
        let str_lit = spanned(AstExpr::Literal(ast::Literal::String("hello".to_string())));
        let result = converter
            .convert_expr(&str_lit, &HashSet::new())
            .expect("String literal conversion should succeed");

        match &result.kind {
            ExprKind::CoreVal(value) => match &value.data {
                CoreData::Literal(Literal::String(val)) => assert_eq!(val, "hello"),
                _ => panic!("Expected String literal"),
            },
            _ => panic!("Expected CoreVal"),
        }

        // Test boolean literal
        let bool_lit = spanned(AstExpr::Literal(ast::Literal::Bool(true)));
        let result = converter
            .convert_expr(&bool_lit, &HashSet::new())
            .expect("Boolean literal conversion should succeed");

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
        let result = converter
            .convert_expr(&float_lit, &HashSet::new())
            .expect("Float literal conversion should succeed");

        match &result.kind {
            ExprKind::CoreVal(value) => match &value.data {
                CoreData::Literal(Literal::Float64(val)) => assert_eq!(*val, float_val),
                _ => panic!("Expected Float64 literal"),
            },
            _ => panic!("Expected CoreVal"),
        }

        // Test unit literal
        let unit_lit = spanned(AstExpr::Literal(ast::Literal::Unit));
        let result = converter
            .convert_expr(&unit_lit, &HashSet::new())
            .expect("Unit literal conversion should succeed");

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
        let converter = ASTConverter::default();

        let ref_expr = spanned(AstExpr::Ref("variable".to_string()));
        let result = converter
            .convert_expr(&ref_expr, &HashSet::new())
            .expect("Reference conversion should succeed");

        match &result.kind {
            ExprKind::Ref(name) => assert_eq!(name, "variable"),
            _ => panic!("Expected Ref"),
        }
    }

    #[test]
    fn test_convert_binary_operators() {
        let converter = ASTConverter::default();

        // Helper to create binary operator tests
        let test_binary_op = |op: AstBinOp, expected_op: BinOp| {
            let left = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
            let right = spanned(AstExpr::Literal(ast::Literal::Int64(2)));
            let bin_expr = spanned(AstExpr::Binary(left, op, right));

            let result = converter
                .convert_expr(&bin_expr, &HashSet::new())
                .expect("Binary operator conversion should succeed");

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
        let converter = ASTConverter::default();

        // Test != (not equal) desugaring to !(left == right)
        let left = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let right = spanned(AstExpr::Literal(ast::Literal::Int64(2)));
        let neq_expr = spanned(AstExpr::Binary(left, AstBinOp::Neq, right));

        let result = converter
            .convert_expr(&neq_expr, &HashSet::new())
            .expect("Not equal operator conversion should succeed");

        match &result.kind {
            ExprKind::Unary(op, _) => assert_eq!(*op, UnaryOp::Not),
            _ => panic!("Expected Unary expression (desugared !=)"),
        }

        // Test > (greater than) desugaring to right < left (swapped operands)
        let left = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let right = spanned(AstExpr::Literal(ast::Literal::Int64(2)));
        let gt_expr = spanned(AstExpr::Binary(left, AstBinOp::Gt, right));

        let result = converter
            .convert_expr(&gt_expr, &HashSet::new())
            .expect("Greater than operator conversion should succeed");

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

        let result = converter
            .convert_expr(&ge_expr, &HashSet::new())
            .expect("Greater than or equal operator conversion should succeed");

        match &result.kind {
            ExprKind::Unary(op, _) => assert_eq!(*op, UnaryOp::Not),
            _ => panic!("Expected Unary expression (desugared >=)"),
        }

        // Test <= (less than or equal) desugaring to left < right || left == right
        let left = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let right = spanned(AstExpr::Literal(ast::Literal::Int64(2)));
        let le_expr = spanned(AstExpr::Binary(left, AstBinOp::Le, right));

        let result = converter
            .convert_expr(&le_expr, &HashSet::new())
            .expect("Less than or equal operator conversion should succeed");

        match &result.kind {
            ExprKind::Binary(_, op, _) => assert_eq!(*op, BinOp::Or),
            _ => panic!("Expected Binary expression (desugared <=)"),
        }
    }

    #[test]
    fn test_convert_unary_operators() {
        let converter = ASTConverter::default();

        // Test negation
        let operand = spanned(AstExpr::Literal(ast::Literal::Int64(42)));
        let neg_expr = spanned(AstExpr::Unary(ast::UnaryOp::Neg, operand));

        let result = converter
            .convert_expr(&neg_expr, &HashSet::new())
            .expect("Negation operator conversion should succeed");

        match &result.kind {
            ExprKind::Unary(op, _) => assert_eq!(*op, UnaryOp::Neg),
            _ => panic!("Expected Unary expression"),
        }

        // Test logical not
        let operand = spanned(AstExpr::Literal(ast::Literal::Bool(true)));
        let not_expr = spanned(AstExpr::Unary(ast::UnaryOp::Not, operand));

        let result = converter
            .convert_expr(&not_expr, &HashSet::new())
            .expect("Not operator conversion should succeed");

        match &result.kind {
            ExprKind::Unary(op, _) => assert_eq!(*op, UnaryOp::Not),
            _ => panic!("Expected Unary expression"),
        }
    }

    #[test]
    fn test_convert_let_expression() {
        let converter = ASTConverter::default();

        let var_name = "x".to_string();
        let field = spanned(ast::Field {
            name: spanned(var_name.clone()),
            ty: spanned(ast::Type::Int64),
        });

        let init = spanned(AstExpr::Literal(ast::Literal::Int64(42)));
        let body = spanned(AstExpr::Ref(var_name.clone()));

        let let_expr = spanned(AstExpr::Let(field, init, body));
        let result = converter
            .convert_expr(&let_expr, &HashSet::new())
            .expect("Let expression conversion should succeed");

        match &result.kind {
            ExprKind::Let(name, _, _) => assert_eq!(name, &var_name),
            _ => panic!("Expected Let expression"),
        }
    }

    #[test]
    fn test_convert_if_then_else() {
        let converter = ASTConverter::default();

        let condition = spanned(AstExpr::Literal(ast::Literal::Bool(true)));
        let then_branch = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let else_branch = spanned(AstExpr::Literal(ast::Literal::Int64(2)));

        let if_expr = spanned(AstExpr::IfThenElse(condition, then_branch, else_branch));
        let result = converter
            .convert_expr(&if_expr, &HashSet::new())
            .expect("If-then-else expression conversion should succeed");

        match &result.kind {
            ExprKind::IfThenElse(_, _, _) => (),
            _ => panic!("Expected IfThenElse expression"),
        }
    }

    #[test]
    fn test_convert_array() {
        let converter = ASTConverter::default();

        let elem1 = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let elem2 = spanned(AstExpr::Literal(ast::Literal::Int64(2)));
        let elem3 = spanned(AstExpr::Literal(ast::Literal::Int64(3)));

        let array_expr = spanned(AstExpr::Array(vec![elem1, elem2, elem3]));
        let result = converter
            .convert_expr(&array_expr, &HashSet::new())
            .expect("Array expression conversion should succeed");

        match &result.kind {
            ExprKind::CoreExpr(CoreData::Array(elements)) => {
                assert_eq!(elements.len(), 3);
            }
            _ => panic!("Expected Array expression"),
        }
    }

    #[test]
    fn test_convert_tuple() {
        let converter = ASTConverter::default();

        let elem1 = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let elem2 = spanned(AstExpr::Literal(ast::Literal::Bool(true)));
        let elem3 = spanned(AstExpr::Literal(ast::Literal::String("test".to_string())));

        let tuple_expr = spanned(AstExpr::Tuple(vec![elem1, elem2, elem3]));
        let result = converter
            .convert_expr(&tuple_expr, &HashSet::new())
            .expect("Tuple expression conversion should succeed");

        match &result.kind {
            ExprKind::CoreExpr(CoreData::Tuple(elements)) => {
                assert_eq!(elements.len(), 3);
            }
            _ => panic!("Expected Tuple expression"),
        }
    }

    #[test]
    fn test_convert_map() {
        let converter = ASTConverter::default();

        let key1 = spanned(AstExpr::Literal(ast::Literal::String("key1".to_string())));
        let val1 = spanned(AstExpr::Literal(ast::Literal::Int64(1)));

        let key2 = spanned(AstExpr::Literal(ast::Literal::String("key2".to_string())));
        let val2 = spanned(AstExpr::Literal(ast::Literal::Int64(2)));

        let map_expr = spanned(AstExpr::Map(vec![(key1, val1), (key2, val2)]));
        let result = converter
            .convert_expr(&map_expr, &HashSet::new())
            .expect("Map expression conversion should succeed");

        match &result.kind {
            ExprKind::Map(entries) => {
                assert_eq!(entries.len(), 2);
            }
            _ => panic!("Expected Map expression"),
        }
    }

    #[test]
    fn test_convert_constructor() {
        let mut converter = ASTConverter::default();

        // Register "Point" type in the registry
        let point_adt = create_test_adt("Point");
        converter
            .type_registry
            .register_adt(&point_adt)
            .expect("Failed to register Point type");

        let arg1 = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let arg2 = spanned(AstExpr::Literal(ast::Literal::String("test".to_string())));

        let constructor_expr = spanned(AstExpr::Constructor(
            spanned("Point".to_string()),
            vec![arg1.clone(), arg2.clone()],
        ));
        let result = converter
            .convert_expr(&constructor_expr, &HashSet::new())
            .expect("Constructor expression conversion should succeed");

        match &result.kind {
            ExprKind::CoreExpr(CoreData::Struct(name, args)) => {
                assert_eq!(name, "Point");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected Struct expression"),
        }

        // Test with unregistered type - should return an error
        let unknown_constructor = spanned(AstExpr::Constructor(
            spanned("UnknownType".to_string()),
            vec![arg1, arg2],
        ));

        let result = converter.convert_expr(&unknown_constructor, &HashSet::new());
        assert!(
            result.is_err(),
            "Expected error for unknown constructor type"
        );
    }

    #[test]
    fn test_convert_nested_constructors() {
        let mut converter = ASTConverter::default();

        // Register multiple types for testing nested constructors
        let types = ["Container", "Item", "Property"];
        for ty in &types {
            let adt = create_test_adt(ty);
            converter
                .type_registry
                .register_adt(&adt)
                .expect(&format!("Failed to register {} type", ty));
        }

        // Create nested constructor expressions
        let property = spanned(AstExpr::Constructor(
            spanned("Property".to_string()),
            vec![spanned(AstExpr::Literal(ast::Literal::String(
                "color".to_string(),
            )))],
        ));

        let item = spanned(AstExpr::Constructor(
            spanned("Item".to_string()),
            vec![property],
        ));

        let container = spanned(AstExpr::Constructor(
            spanned("Container".to_string()),
            vec![item],
        ));

        // Test nested constructors - should succeed
        let result = converter.convert_expr(&container, &HashSet::new());
        assert!(
            result.is_ok(),
            "Valid nested constructors should convert successfully"
        );

        // Test with one invalid type in the nested structure
        let invalid_property = spanned(AstExpr::Constructor(
            spanned("InvalidType".to_string()),
            vec![spanned(AstExpr::Literal(ast::Literal::String(
                "color".to_string(),
            )))],
        ));

        let invalid_item = spanned(AstExpr::Constructor(
            spanned("Item".to_string()),
            vec![invalid_property],
        ));

        let invalid_container = spanned(AstExpr::Constructor(
            spanned("Container".to_string()),
            vec![invalid_item],
        ));

        // Should fail due to the invalid type
        let result = converter.convert_expr(&invalid_container, &HashSet::new());
        assert!(
            result.is_err(),
            "Nested constructor with invalid type should fail"
        );
    }

    #[test]
    fn test_constructor_in_complex_expressions() {
        let mut converter = ASTConverter::default();

        // Register necessary types
        let types = ["User", "Address", "Order"];
        for ty in &types {
            let adt = create_test_adt(ty);
            converter
                .type_registry
                .register_adt(&adt)
                .expect(&format!("Failed to register {} type", ty));
        }

        // Create a constructor inside a let expression
        let address_constructor = spanned(AstExpr::Constructor(
            spanned("Address".to_string()),
            vec![spanned(AstExpr::Literal(ast::Literal::String(
                "123 Main St".to_string(),
            )))],
        ));

        let field = spanned(ast::Field {
            name: spanned("addr".to_string()),
            ty: spanned(ast::Type::Identifier("Address".to_string())),
        });

        let body = spanned(AstExpr::Ref("addr".to_string()));

        let let_expr = spanned(AstExpr::Let(field, address_constructor, body.clone()));

        // Test constructor inside let - should succeed
        let result = converter.convert_expr(&let_expr, &HashSet::new());
        assert!(
            result.is_ok(),
            "Constructor inside let expression should succeed"
        );

        // Test with invalid type in constructor inside let
        let invalid_constructor = spanned(AstExpr::Constructor(
            spanned("InvalidType".to_string()),
            vec![spanned(AstExpr::Literal(ast::Literal::String(
                "123 Main St".to_string(),
            )))],
        ));

        let field = spanned(ast::Field {
            name: spanned("addr".to_string()),
            ty: spanned(ast::Type::Identifier("Address".to_string())),
        });

        let invalid_let = spanned(AstExpr::Let(field, invalid_constructor, body.clone()));

        // Should fail due to the invalid constructor type
        let result = converter.convert_expr(&invalid_let, &HashSet::new());
        assert!(
            result.is_err(),
            "Let with invalid constructor type should fail"
        );
    }

    #[test]
    fn test_convert_closure() {
        let converter = ASTConverter::default();

        let param = spanned(ast::Field {
            name: spanned("x".to_string()),
            ty: spanned(ast::Type::Int64),
        });

        let body = spanned(AstExpr::Ref("x".to_string()));
        let closure_expr = spanned(AstExpr::Closure(vec![param], body));
        let result = converter
            .convert_expr(&closure_expr, &HashSet::new())
            .expect("Closure expression conversion should succeed");

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
        let converter = ASTConverter::default();

        let func = spanned(AstExpr::Ref("add".to_string()));
        let arg1 = spanned(AstExpr::Literal(ast::Literal::Int64(1)));
        let arg2 = spanned(AstExpr::Literal(ast::Literal::Int64(2)));

        let call_expr = spanned(AstExpr::Postfix(
            func,
            ast::PostfixOp::Call(vec![arg1, arg2]),
        ));
        let result = converter
            .convert_expr(&call_expr, &HashSet::new())
            .expect("Function call conversion should succeed");

        match &result.kind {
            ExprKind::Call(_, args) => {
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected Call expression"),
        }
    }

    #[test]
    fn test_convert_field_access() {
        let converter = ASTConverter::default();

        let obj = spanned(AstExpr::Ref("point".to_string()));
        let field_name = spanned("x".to_string());

        let field_access_expr = spanned(AstExpr::Postfix(obj, ast::PostfixOp::Field(field_name)));
        let result = converter
            .convert_expr(&field_access_expr, &HashSet::new())
            .expect("Field access conversion should succeed");

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
        let converter = ASTConverter::default();

        let obj = spanned(AstExpr::Ref("list".to_string()));
        let method_name = spanned("add".to_string());
        let arg = spanned(AstExpr::Literal(ast::Literal::Int64(42)));

        let method_call_expr = spanned(AstExpr::Postfix(
            obj,
            ast::PostfixOp::Method(method_name, vec![arg]),
        ));
        let result = converter
            .convert_expr(&method_call_expr, &HashSet::new())
            .expect("Method call conversion should succeed");

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
        let converter = ASTConverter::default();

        let error_expr = spanned(AstExpr::Literal(ast::Literal::String("error".to_string())));
        let fail_expr = spanned(AstExpr::Fail(error_expr));
        let result = converter
            .convert_expr(&fail_expr, &HashSet::new())
            .expect("Fail expression conversion should succeed");

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
        let mut converter = ASTConverter::default();

        // Register a type for constructor patterns
        let point_adt = create_test_adt("Point");
        converter
            .type_registry
            .register_adt(&point_adt)
            .expect("Failed to register Point type");

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

        let match_expr = spanned(AstExpr::PatternMatch(scrutinee.clone(), vec![arm1, arm2]));
        let result = converter
            .convert_expr(&match_expr, &HashSet::new())
            .expect("Pattern match conversion should succeed");

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

        // Test pattern match with constructor pattern
        let constructor_pattern = spanned(ast::Pattern::Constructor(
            spanned("Point".to_string()),
            vec![],
        ));
        let expr3 = spanned(AstExpr::Literal(ast::Literal::String("point".to_string())));
        let arm3 = spanned(ast::MatchArm {
            pattern: constructor_pattern,
            expr: expr3.clone(),
        });

        let match_with_constructor = spanned(AstExpr::PatternMatch(scrutinee.clone(), vec![arm3]));
        let result = converter.convert_expr(&match_with_constructor, &HashSet::new());
        assert!(
            result.is_ok(),
            "Pattern match with valid constructor should succeed"
        );

        // Test pattern match with invalid constructor pattern
        let invalid_pattern = spanned(ast::Pattern::Constructor(
            spanned("InvalidType".to_string()),
            vec![],
        ));
        let invalid_arm = spanned(ast::MatchArm {
            pattern: invalid_pattern,
            expr: expr3,
        });

        let invalid_match = spanned(AstExpr::PatternMatch(scrutinee, vec![invalid_arm]));
        let result = converter.convert_expr(&invalid_match, &HashSet::new());
        assert!(
            result.is_err(),
            "Pattern match with invalid constructor should fail"
        );
    }

    #[test]
    fn test_convert_block() {
        let converter = ASTConverter::default();

        // Create a simple expression inside a block
        let inner_expr = spanned(AstExpr::Literal(ast::Literal::Int64(42)));
        let block_expr = spanned(AstExpr::Block(inner_expr));

        let result = converter
            .convert_expr(&block_expr, &HashSet::new())
            .expect("Block expression conversion should succeed");

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
        let converter = ASTConverter::default();

        // Create a span with specific location
        let custom_span = Span::new("test_file.txt".to_string(), 10..20);

        // Create a simple expression with that span
        let expr = Spanned::new(
            AstExpr::Literal(ast::Literal::Int64(42)),
            custom_span.clone(),
        );

        // Convert the expression
        let result = converter
            .convert_expr(&expr, &HashSet::new())
            .expect("Expression with custom span conversion should succeed");

        // Verify the span is preserved
        assert_eq!(result.metadata.span.src_file, "test_file.txt");
        assert_eq!(result.metadata.span.range, (10, 20));
    }
}
