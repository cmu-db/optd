use super::hir::{
    Annotation, BinOp, ExprKind, FunKind, HIR, Identifier, Literal, TypedSpan, UdfKind, UnaryOp,
};
use super::semantic_checker::error::SemanticError;
use crate::parser::ast::{self, Function, Module};
use crate::utils::error::CompileError;
use crate::utils::span::Spanned;
use crate::{
    analyzer::{
        context::Context,
        hir::{CoreData, Expr, Value},
        r#type::{Type, TypeRegistry},
    },
    parser::ast::Item,
};
use CompileError::*;
use ExprKind::*;
use FunKind::*;
use UdfKind::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Converts an AST to a High-level Intermediate Representation (HIR).
#[derive(Debug, Clone, Default)]
pub struct ASTConverter {
    /// Registry of types for type checking and subtyping.
    type_registry: TypeRegistry,
    /// Context for variable bindings.
    context: Context<TypedSpan>,
    /// Annotations for HIR expressions.
    annotations: HashMap<Identifier, Vec<Annotation>>,
}

impl ASTConverter {
    /// Converts an AST module to a partially typed and spanned HIR.
    ///
    /// This method processes all items in the AST module and transforms them into the HIR representation.
    /// For functions, it handles receivers, parameters, function bodies, and return types.
    ///
    /// # Arguments
    ///
    /// * `module` - The AST module to convert
    ///
    /// # Returns
    ///
    /// The HIR representation and TypeRegistry, or a CompileError if conversion fails.
    pub fn convert(
        mut self,
        module: &Module,
    ) -> Result<(HIR<TypedSpan>, TypeRegistry), CompileError> {
        // Process all module items sequentially.
        for item in &module.items {
            match item {
                Item::Adt(spanned_adt) => {
                    self.type_registry.register_adt(&spanned_adt.value)?;
                }
                Item::Function(spanned_fn) => {
                    self.process_function(spanned_fn)?;
                }
            }
        }

        Ok((
            HIR {
                context: self.context,
                annotations: self.annotations,
            },
            self.type_registry,
        ))
    }

    fn process_function(&mut self, spanned_fn: &Spanned<Function>) -> Result<(), CompileError> {
        let func = &spanned_fn.value;
        let name = &*func.name.value;
        let return_type = self.convert_type(&func.return_type.value);
        let fn_span = func.name.span.clone();

        // Reject functions without parameters.
        if func.receiver.is_none() && func.params.is_none() {
            return Err(SemanticError(SemanticError::new_incomplete_function(
                name.clone(),
                fn_span,
            )));
        }

        // Register the function in the context, while checking for duplicates.
        let params = self.get_parameters(func);
        let fn_type = self.create_function_type(&params, &return_type);
        match &func.body {
            Some(body_expr) => {
                // Process function with body.
                let body_hir = self.convert_expr(&body_expr)?;
                let param_names = params.iter().map(|(name, _)| name.clone()).collect();

                let fn_value = Value::new_with(
                    CoreData::Function(Closure(param_names, Arc::new(body_hir))),
                    fn_span,
                    fn_type,
                );

                self.context.try_bind(name.clone(), fn_value)?;
            }
            None => {
                // Process external function (UDF).
                let fn_value = Value::new_with(
                    CoreData::Function(Udf(Unlinked(name.clone()))),
                    fn_span,
                    fn_type,
                );

                self.context.try_bind(name.clone(), fn_value)?;
            }
        };

        // Register function annotations if present.
        if !func.annotations.is_empty() {
            let annotations = func
                .annotations
                .iter()
                .map(|annotation| (*annotation.value).clone())
                .collect();

            self.annotations.insert(name.clone(), annotations);
        }

        Ok(())
    }

    fn get_parameters(&self, func: &Function) -> Vec<(Identifier, Type)> {
        // Start with receiver if it exists.
        let mut param_fields = match &func.receiver {
            Some(receiver) => {
                vec![(
                    (*receiver.name).clone(),
                    self.convert_type(&receiver.ty.value),
                )]
            }
            None => vec![],
        };

        // Add regular parameters if they exist.
        if let Some(params) = &func.params {
            let regular_params = params
                .iter()
                .map(|field| ((*field.name).clone(), self.convert_type(&field.ty.value)))
                .collect::<Vec<_>>();
            param_fields.extend(regular_params);
        }

        param_fields
    }

    fn create_function_type(&self, params: &[(Identifier, Type)], return_type: &Type) -> Type {
        let param_types: Vec<Type> = params.iter().map(|(_, ty)| ty.clone()).collect();

        let param_type = if params.is_empty() {
            Type::Unit
        } else if param_types.len() == 1 {
            param_types[0].clone()
        } else {
            Type::Tuple(param_types)
        };

        Type::Closure(param_type.into(), return_type.clone().into())
    }

    fn convert_expr(
        &self,
        spanned_expr: &Spanned<ast::Expr>,
    ) -> Result<Expr<TypedSpan>, CompileError> {
        let span = spanned_expr.span.clone();
        let typed_span = TypedSpan {
            span: span.clone(),
            ty: Type::Unknown,
        };

        let kind = match &*spanned_expr.value {
            ast::Expr::Error => panic!("AST should no longer contain errors"),

            ast::Expr::Literal(lit) => {
                let (hir_lit, lit_type) = self.convert_literal(lit);
                CoreVal(Value::new_with(CoreData::Literal(hir_lit), span, lit_type))
            }

            ast::Expr::Ref(ident) => Ref(ident.clone()),

            ast::Expr::Binary(left, op, right) => self.convert_binary_expr(left, op, right)?,

            ast::Expr::Unary(op, operand) => self.convert_unary_expr(op, operand)?,

            ast::Expr::Let(field, init, body) => {
                let hir_init = self.convert_expr(init)?;
                let hir_body = self.convert_expr(body)?;
                let var_name = (*field.name).clone();

                Let(var_name, Arc::new(hir_init), Arc::new(hir_body))
            }

            ast::Expr::IfThenElse(condition, then_branch, else_branch) => {
                let hir_condition = self.convert_expr(condition)?;
                let hir_then = self.convert_expr(then_branch)?;
                let hir_else = self.convert_expr(else_branch)?;

                IfThenElse(
                    Arc::new(hir_condition),
                    Arc::new(hir_then),
                    Arc::new(hir_else),
                )
            }

            ast::Expr::PatternMatch(_scrutinee, _arms) => todo!(),

            ast::Expr::Array(elements) => {
                let mut hir_elements = Vec::with_capacity(elements.len());

                for elem in elements {
                    let hir_elem = self.convert_expr(elem)?;
                    hir_elements.push(Arc::new(hir_elem));
                }

                CoreExpr(CoreData::Array(hir_elements))
            }

            ast::Expr::Tuple(elements) => {
                let mut hir_elements = Vec::with_capacity(elements.len());

                for elem in elements {
                    let hir_elem = self.convert_expr(elem)?;
                    hir_elements.push(Arc::new(hir_elem));
                }

                CoreExpr(CoreData::Tuple(hir_elements))
            }

            ast::Expr::Map(entries) => {
                let mut hir_entries = Vec::with_capacity(entries.len());

                for (key, value) in entries {
                    let hir_key = self.convert_expr(key)?;
                    let hir_value = self.convert_expr(value)?;
                    hir_entries.push((Arc::new(hir_key), Arc::new(hir_value)));
                }

                Map(hir_entries)
            }

            ast::Expr::Constructor(_name, _args) => todo!(),

            ast::Expr::Closure(_params, _body) => todo!(),

            ast::Expr::Postfix(_expr, _op) => todo!(),

            ast::Expr::Fail(error_expr) => {
                let hir_error = self.convert_expr(error_expr)?;
                CoreExpr(CoreData::Fail(Box::new(Arc::new(hir_error))))
            }
        };

        Ok(Expr {
            kind,
            metadata: typed_span,
        })
    }

    fn convert_binary_expr(
        &self,
        left: &Spanned<ast::Expr>,
        op: &ast::BinOp,
        right: &Spanned<ast::Expr>,
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
                let hir_left = self.convert_expr(left)?;
                let hir_right = self.convert_expr(right)?;
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
                    ast::BinOp::Neq => todo!(),
                    ast::BinOp::Gt => todo!(),
                    ast::BinOp::Ge => todo!(),
                    ast::BinOp::Le => todo!(),
                };

                Ok(Binary(hir_left.into(), hir_op, hir_right.into()))
            }

            // Desugar not equal (!= becomes !(a == b)).
            ast::BinOp::Neq => {
                let hir_left = self.convert_expr(left)?;
                let hir_right = self.convert_expr(right)?;

                let eq_expr = Expr {
                    kind: Binary(hir_left.into(), BinOp::Eq, hir_right.into()),
                    metadata: TypedSpan {
                        span: left.span.clone(), // Using left's span as a placeholder.
                        ty: Type::Unknown,
                    },
                };

                Ok(Unary(UnaryOp::Not, eq_expr.into()))
            }

            // Desugar greater than (> becomes b < a by swapping operands).
            ast::BinOp::Gt => {
                let hir_left = self.convert_expr(right)?;
                let hir_right = self.convert_expr(left)?;

                Ok(Binary(hir_left.into(), BinOp::Lt, hir_right.into()))
            }

            // Desugar greater than or equal (>= becomes !(a < b)).
            ast::BinOp::Ge => {
                let hir_left = self.convert_expr(left)?;
                let hir_right = self.convert_expr(right)?;

                let lt_expr = Expr {
                    kind: Binary(hir_left.into(), BinOp::Lt, hir_right.into()),
                    metadata: TypedSpan {
                        span: left.span.clone(), // Using left's span as a placeholder.
                        ty: Type::Unknown,
                    },
                };

                Ok(Unary(UnaryOp::Not, lt_expr.into()))
            }

            // Desugar less than or equal (<= becomes a < b || a == b).
            ast::BinOp::Le => {
                let hir_left = Arc::new(self.convert_expr(left)?);
                let hir_right = Arc::new(self.convert_expr(right)?);

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

                Ok(Binary(lt_expr.into(), BinOp::Or, eq_expr.into()))
            }
        }
    }

    fn convert_unary_expr(
        &self,
        op: &ast::UnaryOp,
        operand: &Spanned<ast::Expr>,
    ) -> Result<ExprKind<TypedSpan>, CompileError> {
        let hir_operand = self.convert_expr(operand)?;

        let hir_op = match op {
            ast::UnaryOp::Neg => UnaryOp::Neg,
            ast::UnaryOp::Not => UnaryOp::Not,
        };

        Ok(ExprKind::Unary(hir_op, hir_operand.into()))
    }

    fn convert_literal(&self, literal: &ast::Literal) -> (Literal, Type) {
        match literal {
            ast::Literal::Int64(val) => (Literal::Int64(*val), Type::Int64),
            ast::Literal::String(val) => (Literal::String(val.clone()), Type::String),
            ast::Literal::Bool(val) => (Literal::Bool(*val), Type::Bool),
            ast::Literal::Float64(val) => (Literal::Float64(val.0), Type::Float64),
            ast::Literal::Unit => (Literal::Unit, Type::Unit),
        }
    }

    fn convert_type(&self, ast_type: &ast::Type) -> Type {
        match ast_type {
            ast::Type::Int64 => Type::Int64,
            ast::Type::String => Type::String,
            ast::Type::Bool => Type::Bool,
            ast::Type::Float64 => Type::Float64,
            ast::Type::Unit => Type::Unit,
            ast::Type::Array(elem_type) => Type::Array(self.convert_type(&elem_type.value).into()),
            ast::Type::Closure(param_type, return_type) => Type::Closure(
                self.convert_type(&param_type.value).into(),
                self.convert_type(&return_type.value).into(),
            ),
            ast::Type::Tuple(types) => {
                let hir_types = types.iter().map(|t| self.convert_type(&t.value)).collect();
                Type::Tuple(hir_types)
            }
            ast::Type::Map(key_type, value_type) => Type::Map(
                self.convert_type(&key_type.value).into(),
                self.convert_type(&value_type.value).into(),
            ),
            ast::Type::Questioned(inner_type) => {
                Type::Optional((self.convert_type(&inner_type.value)).into())
            }
            ast::Type::Starred(inner_type) => {
                Type::Stored((self.convert_type(&inner_type.value)).into())
            }
            ast::Type::Dollared(inner_type) => {
                Type::Costed((self.convert_type(&inner_type.value)).into())
            }
            ast::Type::Identifier(name) => Type::Adt(name.clone()),
            ast::Type::Error => panic!("AST should no longer contain errors"),
            ast::Type::Unknown => Type::Unknown,
        }
    }
}
