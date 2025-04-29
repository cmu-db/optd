use super::registry::TypeRegistry;
use crate::dsl::{
    analyzer::{
        context::Context,
        errors::AnalyzerErrorKind,
        hir::{
            BinOp, CoreData, Expr, ExprKind, FunKind, HIR, Identifier, LetBinding, MapEntry,
            MatchArm, Pattern, PatternKind, TypedSpan, UnaryOp, Value,
        },
        type_checks::registry::{Type, TypeKind},
    },
    utils::span::Span,
};
use std::sync::Arc;

impl TypeRegistry {
    /// Generates type constraints from an HIR while verifying scopes.
    ///
    /// This method traverses the HIR and simultaneously:
    /// 1. Ensures all variable references have proper bindings in the lexical scope.
    /// 2. Detects duplicate identifiers in the same scope.
    /// 3. Generates type constraints for the solver to later resolve.
    ///
    /// # Arguments
    ///
    /// * `hir` - The HIR to analyze and generate constraints from.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if no scope errors are found.
    pub fn generate_constraints(
        &mut self,
        hir: &HIR<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use CoreData::*;
        use FunKind::*;

        // Process each function in the HIR context separately.
        hir.context
            .get_all_bindings()
            .iter()
            .try_for_each(|(_, fun)| match &fun.data {
                Function(Closure(args, body)) => {
                    let ctx =
                        self.create_function_scope(hir.context.clone(), &args[..], &fun.metadata)?;

                    let ret_type = self.ty_return.as_ref().cloned().unwrap();
                    self.add_constraint_subtypes(&ret_type, &[body.metadata.clone()]);
                    self.generate_expr(body, ctx)
                }
                Function(Udf(_)) => Ok(()), // UDFs have no body to check.
                _ => panic!("Expected a function, but got: {:?}", fun.data),
            })
    }

    /// Creates a new context with function parameters and types bound in a new scope.
    /// This ensures proper lexical scoping for function bodies.
    fn create_function_scope(
        &mut self,
        base_ctx: Context<TypedSpan>,
        params: &[Identifier],
        metadata: &TypedSpan,
    ) -> Result<Context<TypedSpan>, Box<AnalyzerErrorKind>> {
        use TypeKind::*;

        let mut fn_ctx = base_ctx;
        fn_ctx.push_scope();

        // Extract the parameter types from the function type.
        let param_types = match &*metadata.ty {
            Closure(param_type, ret_type) => {
                self.ty_return = Some(ret_type.clone());
                match &*param_type.value {
                    Tuple(types) => types.clone(),
                    _ => vec![param_type.clone()],
                }
            }
            _ => panic!("Expected a closure type for function parameters"),
        };

        // Bind each parameter with its appropriate type.
        for (name, ty) in params.iter().zip(param_types.iter()) {
            let dummy = Self::dummy_value(ty, &metadata.span);
            fn_ctx.try_bind(name.clone(), dummy)?;
        }

        Ok(fn_ctx)
    }

    /// Main expression scope checker that recursively validates variable references,
    /// introduces new scopes as needed, and generates type constraints for the solver.
    fn generate_expr(
        &mut self,
        expr: &Expr<TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use ExprKind::*;

        match &expr.kind {
            PatternMatch(scrutinee, arms) => {
                self.generate_pattern_match(expr, scrutinee, arms, ctx)
            }
            IfThenElse(cond, then_branch, else_branch) => {
                self.generate_if_then_else(expr, cond, then_branch, else_branch, ctx)
            }
            NewScope(inner_expr) => self.generate_new_scope(expr, inner_expr, ctx),
            Let(binding, body) => self.generate_let(expr, binding, body, ctx),
            Binary(left, op, right) => self.generate_binary(expr, left, op, right, ctx),
            Unary(op, operand) => self.generate_unary(expr, op, operand, ctx),
            Call(func, args) => self.generate_call(expr, func, args, ctx),
            Map(entries) => self.generate_map(expr, entries, ctx),
            Ref(name) => self.generate_ref(expr, name, &ctx),
            Return(inner_expr) => self.generate_return(inner_expr, ctx),
            FieldAccess(obj, field_name) => self.generate_field_access(expr, obj, field_name, ctx),
            CoreExpr(core_data) => self.generate_core_expr(expr, core_data, ctx),
            // TODO(#80): Nothing to do here as only one type of CoreVal is created.
            // 1. External closures (i.e. declared with fn), but those are already handled in
            //    generate_constraints.
            // Note: It makes no sense that functions can be expressions. They should only be
            // values. Once we change that, we just move the function core_expr code.
            CoreVal(_) => panic!("CoreVal should not be in HIR<TypedSpan>"),
        }
    }

    /// Generates constraints for pattern match expressions while verifying scopes.
    /// Enforces two key type relationships:
    /// - `scrutinee >: all arm patterns`
    /// - `expr >: all arm expressions`
    fn generate_pattern_match(
        &mut self,
        expr: &Expr<TypedSpan>,
        scrutinee: &Expr<TypedSpan>,
        arms: &[MatchArm<TypedSpan>],
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let mut arm_types = Vec::with_capacity(arms.len());

        for arm in arms {
            arm_types.push(arm.expr.metadata.clone());

            self.add_constraint_scrutinee(&scrutinee.metadata, &arm.pattern);

            let mut arm_ctx = ctx.clone();
            arm_ctx.push_scope();

            Self::generate_pattern(&arm.pattern, &mut arm_ctx)?;
            self.generate_expr(&arm.expr, arm_ctx)?;
        }

        self.add_constraint_subtypes(&expr.metadata.ty, &arm_types);

        self.generate_expr(scrutinee, ctx)
    }

    /// Adds pattern bindings to the context.
    ///
    /// Pattern validity is checked later in the type checker, as it requires
    /// more information about the scrutinee (e.g. $, *, Array, etc.).
    fn generate_pattern(
        pattern: &Pattern<TypedSpan>,
        ctx: &mut Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use PatternKind::*;

        match &pattern.kind {
            Bind(name, sub_pattern) => {
                let dummy = Self::dummy_value(&pattern.metadata.ty, &pattern.metadata.span);
                ctx.try_bind(name.clone(), dummy)?;
                Self::generate_pattern(sub_pattern, ctx)?;
            }
            Struct(_, field_patterns) => {
                field_patterns
                    .iter()
                    .try_for_each(|field_pat| Self::generate_pattern(field_pat, ctx))?;
            }
            Operator(_) => panic!("Operators may not be in the HIR yet"),
            ArrayDecomp(head, tail) => {
                Self::generate_pattern(head, ctx)?;
                Self::generate_pattern(tail, ctx)?;
            }
            Wildcard | EmptyArray | Literal(_) => {} // Terminal patterns, no action needed.
        }

        Ok(())
    }

    /// Generates constraints for if-then-else expressions while verifying scopes.
    /// Enforces two key type relationships:
    /// - `bool = cond`
    /// - `expr >: then, else`
    fn generate_if_then_else(
        &mut self,
        expr: &Expr<TypedSpan>,
        cond: &Expr<TypedSpan>,
        then_branch: &Expr<TypedSpan>,
        else_branch: &Expr<TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let bool_type = TypedSpan::new(TypeKind::Bool.into(), cond.metadata.span.clone());
        let branch_types = [then_branch.metadata.clone(), else_branch.metadata.clone()];

        self.add_constraint_equal(&bool_type, &cond.metadata);
        self.add_constraint_subtypes(&expr.metadata.ty, &branch_types);

        self.generate_expr(cond, ctx.clone())?;
        self.generate_expr(then_branch, ctx.clone())?;
        self.generate_expr(else_branch, ctx)
    }

    /// Generates constraints for new scope expressions while verifying scopes.
    /// Enforces the type relationship:
    /// - `outer >: inner`
    fn generate_new_scope(
        &mut self,
        expr: &Expr<TypedSpan>,
        inner_expr: &Expr<TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let mut new_ctx = ctx.clone();
        new_ctx.push_scope();

        self.add_constraint_subtypes(&expr.metadata.ty, &[inner_expr.metadata.clone()]);
        self.generate_expr(inner_expr, new_ctx)
    }

    /// Generates constraints for let binding expressions while verifying scopes.
    /// Enforces the type relationship:
    /// - `expr >: body`
    /// - `binding >: binding.expr`
    fn generate_let(
        &mut self,
        expr: &Expr<TypedSpan>,
        binding: &LetBinding<TypedSpan>,
        body: &Expr<TypedSpan>,
        mut ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        // Bind the type to the context.
        let dummy = Self::dummy_value(&binding.metadata.ty, &binding.metadata.span);

        self.add_constraint_subtypes(&binding.metadata.ty, &[binding.expr.metadata.clone()]);
        self.add_constraint_subtypes(&expr.metadata.ty, &[body.metadata.clone()]);

        self.generate_expr(&binding.expr, ctx.clone())?;
        ctx.try_bind(binding.name.to_string(), dummy)?;
        self.generate_expr(body, ctx)
    }

    /// Generates constraints for binary operations while verifying scopes.
    /// Enforces type relationships based on operation:
    /// - Add/Sub/Mul/Div: `expr >: left, right` and `Arithmetic >: left, right`
    /// - Lt: `bool = expr`, `Arithmetic >: left, right`, `left = right`
    /// - Eq: `bool = expr`, `EqHash >: left, right`, `left = right`
    /// - And/Or: `bool = expr`, `bool >: left, right`
    /// - Range: `Array(I64) = expr`, `I64 >: left, right`
    /// - Concat: `expr >: left, right`, `Concat >: left, right`
    fn generate_binary(
        &mut self,
        expr: &Expr<TypedSpan>,
        left: &Expr<TypedSpan>,
        op: &BinOp,
        right: &Expr<TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use BinOp::*;

        let span = &expr.metadata.span;

        let bool_type = TypedSpan::new(TypeKind::Bool.into(), span.clone());
        let arithmetic_type = TypedSpan::new(TypeKind::Arithmetic.into(), span.clone());
        let eqhash_type = TypedSpan::new(TypeKind::EqHash.into(), span.clone());
        let concat_type = TypedSpan::new(TypeKind::Concat.into(), span.clone());
        let i64_type = TypedSpan::new(TypeKind::I64.into(), span.clone());
        let array_i64_type =
            TypedSpan::new(TypeKind::Array(TypeKind::I64.into()).into(), span.clone());

        let op_types = vec![left.metadata.clone(), right.metadata.clone()];

        match op {
            Add | Sub | Mul | Div => {
                self.add_constraint_subtypes(&expr.metadata.ty, &op_types);
                self.add_constraint_subtypes(&arithmetic_type.ty, &op_types);
            }
            Lt => {
                self.add_constraint_equal(&bool_type, &expr.metadata);
                self.add_constraint_subtypes(&arithmetic_type.ty, &op_types);
                self.add_constraint_equal(&left.metadata, &right.metadata);
            }
            Eq => {
                self.add_constraint_equal(&bool_type, &expr.metadata);
                self.add_constraint_subtypes(&eqhash_type.ty, &op_types);
                self.add_constraint_equal(&left.metadata, &right.metadata);
            }
            And | Or => {
                self.add_constraint_equal(&bool_type, &expr.metadata);
                self.add_constraint_subtypes(&bool_type.ty, &op_types);
            }
            Range => {
                self.add_constraint_equal(&array_i64_type, &expr.metadata);
                self.add_constraint_subtypes(&i64_type.ty, &op_types);
            }
            Concat => {
                self.add_constraint_subtypes(&expr.metadata.ty, &op_types);
                self.add_constraint_subtypes(&concat_type.ty, &op_types);
            }
        }

        self.generate_expr(left, ctx.clone())?;
        self.generate_expr(right, ctx)
    }

    /// Generates constraints for unary operations while verifying scopes.
    /// Enforces type relationships based on operation:
    /// - Neg: `expr >: operand`, `Arithmetic >: operand`
    /// - Not: `expr = bool`, `bool = operand`
    fn generate_unary(
        &mut self,
        expr: &Expr<TypedSpan>,
        op: &UnaryOp,
        operand: &Expr<TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use TypeKind::*;
        use UnaryOp::*;

        let span = &expr.metadata.span;
        let bool_type = TypedSpan::new(Bool.into(), span.clone());
        let arithmetic_type = TypedSpan::new(Arithmetic.into(), span.clone());

        match op {
            Neg => {
                self.add_constraint_subtypes(&arithmetic_type.ty, &[operand.metadata.clone()]);
                self.add_constraint_subtypes(&expr.metadata.ty, &[operand.metadata.clone()]);
            }
            Not => {
                self.add_constraint_equal(&bool_type, &operand.metadata);
                self.add_constraint_equal(&bool_type, &expr.metadata);
            }
        }

        self.generate_expr(operand, ctx)
    }

    /// Generates constraints for function calls while verifying scopes.
    /// Enforces the type relationship:
    /// - `Closure(args_types, expr.type) >: func`
    fn generate_call(
        &mut self,
        expr: &Expr<TypedSpan>,
        func: &Expr<TypedSpan>,
        args: &[Arc<Expr<TypedSpan>>],
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let arg_types: Vec<_> = args.iter().map(|arg| arg.metadata.clone()).collect();
        self.add_constraint_call(&expr.metadata, &func.metadata, &arg_types);
        self.generate_expr(func, ctx.clone())?;
        args.iter()
            .try_for_each(|arg| self.generate_expr(arg, ctx.clone()))
    }

    /// Generates constraints for map expressions while verifying scopes.
    /// Enforces the type relationship for each key:
    /// - `EqHash >: key`
    /// - `map_key >: key`
    ///
    /// Enforces the type relationship for each value:
    /// - `map_value >: value`
    fn generate_map(
        &mut self,
        expr: &Expr<TypedSpan>,
        entries: &[MapEntry<TypedSpan>],
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use TypeKind::*;

        entries
            .iter()
            .for_each(|(k, _)| self.add_constraint_subtypes(&EqHash.into(), &[k.metadata.clone()]));

        let Map(map_key, map_value) = &*expr.metadata.ty else {
            panic!("Type should be set to map in from_ast");
        };

        entries.iter().for_each(|(k, v)| {
            self.add_constraint_subtypes(map_key, &[k.metadata.clone()]);
            self.add_constraint_subtypes(map_value, &[v.metadata.clone()]);
        });

        entries.iter().try_for_each(|(k, v)| {
            self.generate_expr(k, ctx.clone())?;
            self.generate_expr(v, ctx.clone())
        })
    }

    /// Generates constraints for variable references while verifying scopes.
    /// Enforces the type relationship:
    /// - `expr = ref_type`
    fn generate_ref(
        &mut self,
        expr: &Expr<TypedSpan>,
        name: &str,
        ctx: &Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        if let Some(value) = ctx.lookup(name) {
            self.add_constraint_equal(&expr.metadata, &value.metadata);
        } else {
            return Err(AnalyzerErrorKind::new_undefined_reference(
                name,
                &expr.metadata.span,
            ));
        }

        Ok(())
    }

    /// Generates constraints for return expressions while verifying scopes.
    /// Enforces the type relationship:
    /// - `ty_return >: inner_expr`
    fn generate_return(
        &mut self,
        inner_expr: &Expr<TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let ty_return = self.ty_return.as_ref().cloned().unwrap();
        self.add_constraint_subtypes(&ty_return, &[inner_expr.metadata.clone()]);
        self.generate_expr(inner_expr, ctx)
    }

    /// Generates constraints for field access expressions while verifying scopes.
    /// Ensures the object of type `obj` has a field with the specified name, and
    /// Enforces the type relationship:
    /// - `expr >: obj.field_name`
    fn generate_field_access(
        &mut self,
        expr: &Expr<TypedSpan>,
        obj: &Expr<TypedSpan>,
        field_name: &str,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        self.add_constraint_field_access(&expr.metadata.ty, &field_name.to_string(), &obj.metadata);
        self.generate_expr(obj, ctx)
    }

    /// Generates constraints for core expressions while verifying scopes.
    /// Handles arrays, tuples, structs, functions, and literals.
    fn generate_core_expr(
        &mut self,
        expr: &Expr<TypedSpan>,
        core_data: &CoreData<std::sync::Arc<Expr<TypedSpan>>, TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use TypeKind::*;

        match core_data {
            CoreData::Array(exprs) => {
                let Array(array_element_type) = &*expr.metadata.ty else {
                    panic!("Type should be set to array in from_ast");
                };

                exprs.iter().for_each(|val| {
                    self.add_constraint_subtypes(array_element_type, &[val.metadata.clone()]);
                });

                exprs
                    .iter()
                    .try_for_each(|expr| self.generate_expr(expr, ctx.clone()))?;
            }
            CoreData::Tuple(exprs) => {
                let Tuple(tuple_element_types) = &*expr.metadata.ty else {
                    panic!("Type should be set to tuple in from_ast");
                };

                exprs
                    .iter()
                    .zip(tuple_element_types.iter())
                    .for_each(|(expr_val, ty)| {
                        self.add_constraint_subtypes(ty, &[expr_val.metadata.clone()]);
                    });

                exprs
                    .iter()
                    .try_for_each(|expr| self.generate_expr(expr, ctx.clone()))?;
            }
            CoreData::Struct(name, exprs) => {
                exprs.iter().enumerate().try_for_each(|(i, field_expr)| {
                    self.add_constraint_subtypes(
                        &self.get_product_field_type_by_index(name, i).unwrap(),
                        &[field_expr.metadata.clone()],
                    );
                    self.generate_expr(field_expr, ctx.clone())
                })?;
            }
            CoreData::Map(_) | CoreData::Logical(_) | CoreData::Physical(_) => {
                panic!("Types may not be in the HIR yet")
            }
            CoreData::Function(FunKind::Udf(_)) => panic!("UDFs may not appear within functions"),
            CoreData::Function(FunKind::Closure(params, body)) => {
                let outer_ret_type = self.ty_return.take();
                let fn_ctx = self.create_function_scope(ctx, &params[..], &expr.metadata)?;
                let inner_ret_type = self.ty_return.clone().unwrap();

                self.add_constraint_subtypes(&inner_ret_type, &[body.metadata.clone()]);
                self.generate_expr(body, fn_ctx)?;

                self.ty_return = outer_ret_type;
            }
            CoreData::Fail(expr) => {
                let string_type = TypedSpan::new(String.into(), expr.metadata.span.clone());
                self.add_constraint_equal(&expr.metadata, &string_type);
                self.generate_expr(expr, ctx)?;
            }
            CoreData::Literal(_) | CoreData::None => {}
        }

        Ok(())
    }

    /// Creates a dummy value with the specified type and span.
    fn dummy_value(ty: &Type, span: &Span) -> Value<TypedSpan> {
        Value::new_with(CoreData::None, ty.clone(), span.clone())
    }
}

#[cfg(test)]
mod scope_check_tests {
    use super::*;
    use crate::dsl::analyzer::{
        context::Context,
        errors::AnalyzerErrorKind,
        hir::{
            CoreData, Expr, ExprKind, FunKind, HIR, LetBinding, MatchArm, Pattern, PatternKind,
            TypedSpan, Value,
        },
        type_checks::{
            converter::create_function_type, registry::type_registry_tests::create_test_span,
        },
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    // Create a dummy core data for expression testing
    fn dummy_core_data() -> CoreData<Arc<Expr<TypedSpan>>, TypedSpan> {
        CoreData::None
    }

    // Setup a context with a function for testing
    fn setup_test_context(
        params: Vec<String>,
        body: Expr<TypedSpan>,
    ) -> (HIR<TypedSpan>, Result<(), Box<AnalyzerErrorKind>>) {
        let mut context = Context::default();
        let mut registry = TypeRegistry::default();

        // Create function value
        let len = params.len();
        let fun_val = Value {
            data: CoreData::Function(FunKind::Closure(params, Arc::new(body))),
            metadata: TypedSpan {
                ty: create_function_type(&vec![TypeKind::None.into(); len], &TypeKind::None.into()),
                span: create_test_span(),
            },
        };

        // Add function to context
        context.bind("test_function".to_string(), fun_val);

        let hir = HIR {
            context,
            annotations: HashMap::new(),
        };

        let result = registry.generate_constraints(&hir);

        (hir, result)
    }

    #[test]
    fn test_valid_reference() {
        let param_name = "x".to_string();
        let body = Expr::new_with(
            ExprKind::Ref(param_name.clone()),
            TypeKind::Nothing.into(),
            create_test_span(),
        );
        let (_, result) = setup_test_context(vec![param_name], body);
        assert!(result.is_ok());
    }

    #[test]
    fn test_undefined_reference() {
        let (_, result) = setup_test_context(
            vec!["x".to_string()],
            Expr::new_with(
                ExprKind::Ref("undefined".to_string()),
                TypeKind::Nothing.into(),
                create_test_span(),
            ),
        );
        assert!(matches!(
            result,
            Err(err) if matches!(*err, AnalyzerErrorKind::UndefinedReference { ref name, .. } if name == "undefined")
        ));
    }

    #[test]
    fn test_let_binding() {
        let let_var = "y".to_string();
        let let_expr = Expr::new_with(
            ExprKind::Let(
                LetBinding::new_with(
                    let_var.clone(),
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(dummy_core_data()),
                        TypeKind::Nothing.into(),
                        create_test_span(),
                    )),
                    TypeKind::Nothing.into(),
                    create_test_span(),
                ),
                Arc::new(Expr::new_with(
                    ExprKind::Ref(let_var),
                    TypeKind::Nothing.into(),
                    create_test_span(),
                )),
            ),
            TypeKind::Nothing.into(),
            create_test_span(),
        );

        let (_, result) = setup_test_context(vec!["x".to_string()], let_expr);
        assert!(result.is_ok());
    }

    #[test]
    fn test_self_reference_in_let_binding() {
        let var_name = "v".to_string();

        // Create a let expression where the variable references itself in its own initializer
        // let v = v + v in v
        let let_expr = Expr::new_with(
            ExprKind::Let(
                LetBinding::new_with(
                    var_name.clone(),
                    Arc::new(Expr::new_with(
                        ExprKind::Binary(
                            Arc::new(Expr::new_with(
                                ExprKind::Ref(var_name.clone()),
                                TypeKind::Nothing.into(),
                                create_test_span(),
                            )),
                            BinOp::Add,
                            Arc::new(Expr::new_with(
                                ExprKind::Ref(var_name.clone()),
                                TypeKind::Nothing.into(),
                                create_test_span(),
                            )),
                        ),
                        TypeKind::Nothing.into(),
                        create_test_span(),
                    )),
                    TypeKind::Nothing.into(),
                    create_test_span(),
                ),
                Arc::new(Expr::new_with(
                    ExprKind::Ref(var_name),
                    TypeKind::Nothing.into(),
                    create_test_span(),
                )),
            ),
            TypeKind::Nothing.into(),
            create_test_span(),
        );

        let (_, result) = setup_test_context(vec![], let_expr);

        assert!(
            matches!(
                result,
                Err(err) if matches!(*err, AnalyzerErrorKind::UndefinedReference { ref name, .. } if name == "v")
            ),
            "Let expression with self-reference should be rejected with UndefinedReference error"
        );
    }

    #[test]
    fn test_duplicate_parameters() {
        let (_, result) = setup_test_context(
            vec!["x".to_string(), "x".to_string()],
            Expr::new_with(
                ExprKind::Ref("x".to_string()),
                TypeKind::Nothing.into(),
                create_test_span(),
            ),
        );
        assert!(matches!(
            result,
            Err(err) if matches!(*err, AnalyzerErrorKind::DuplicateIdentifier { ref name, .. } if name == "x")
        ));
    }

    #[test]
    fn test_duplicate_let() {
        let let_var = "y".to_string();
        let outer_let = Expr::new_with(
            ExprKind::Let(
                LetBinding::new_with(
                    let_var.clone(),
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(dummy_core_data()),
                        TypeKind::Nothing.into(),
                        create_test_span(),
                    )),
                    TypeKind::Nothing.into(),
                    create_test_span(),
                ),
                Arc::new(Expr::new_with(
                    ExprKind::Let(
                        LetBinding::new_with(
                            let_var.clone(),
                            Arc::new(Expr::new_with(
                                ExprKind::CoreExpr(dummy_core_data()),
                                TypeKind::Nothing.into(),
                                create_test_span(),
                            )),
                            TypeKind::Nothing.into(),
                            create_test_span(),
                        ),
                        Arc::new(Expr::new_with(
                            ExprKind::Ref(let_var),
                            TypeKind::Nothing.into(),
                            create_test_span(),
                        )),
                    ),
                    TypeKind::Nothing.into(),
                    create_test_span(),
                )),
            ),
            TypeKind::Nothing.into(),
            create_test_span(),
        );

        let (_, result) = setup_test_context(vec!["x".to_string()], outer_let);
        assert!(matches!(
            result,
            Err(err) if matches!(*err, AnalyzerErrorKind::DuplicateIdentifier { ref name, .. } if name == "y")
        ));
    }

    #[test]
    fn test_nested_function() {
        let outer_param = "x".to_string();
        let inner_param = "y".to_string();

        let inner_fn = Expr::new_with(
            ExprKind::CoreExpr(CoreData::Function(FunKind::Closure(
                vec![inner_param],
                Arc::new(Expr::new_with(
                    ExprKind::Ref(outer_param.clone()),
                    TypeKind::Nothing.into(),
                    create_test_span(),
                )),
            ))),
            TypeKind::Closure(TypeKind::None.into(), TypeKind::None.into()).into(),
            create_test_span(),
        );

        let (_, result) = setup_test_context(vec![outer_param], inner_fn);
        assert!(result.is_ok());
    }

    #[test]
    fn test_pattern_match() {
        let match_expr = Expr::new_with(
            ExprKind::PatternMatch(
                Arc::new(Expr::new_with(
                    ExprKind::Ref("x".to_string()),
                    TypeKind::Nothing.into(),
                    create_test_span(),
                )),
                vec![MatchArm {
                    pattern: Pattern::new_with(
                        PatternKind::Bind(
                            "matched".to_string(),
                            Box::new(Pattern::new_with(
                                PatternKind::Wildcard,
                                TypeKind::Nothing.into(),
                                create_test_span(),
                            )),
                        ),
                        TypeKind::Nothing.into(),
                        create_test_span(),
                    ),
                    expr: Arc::new(Expr::new_with(
                        ExprKind::Ref("matched".to_string()),
                        TypeKind::Nothing.into(),
                        create_test_span(),
                    )),
                }],
            ),
            TypeKind::Nothing.into(),
            create_test_span(),
        );

        let (_, result) = setup_test_context(vec!["x".to_string()], match_expr);
        assert!(result.is_ok());
    }

    #[test]
    fn test_duplicate_pattern_bindings() {
        let match_expr = Expr::new_with(
            ExprKind::PatternMatch(
                Arc::new(Expr::new_with(
                    ExprKind::Ref("x".to_string()),
                    TypeKind::Nothing.into(),
                    create_test_span(),
                )),
                vec![MatchArm {
                    pattern: Pattern::new_with(
                        PatternKind::Bind(
                            "y".to_string(),
                            Box::new(Pattern::new_with(
                                PatternKind::Bind(
                                    "y".to_string(),
                                    Box::new(Pattern::new_with(
                                        PatternKind::Wildcard,
                                        TypeKind::Nothing.into(),
                                        create_test_span(),
                                    )),
                                ),
                                TypeKind::Nothing.into(),
                                create_test_span(),
                            )),
                        ),
                        TypeKind::Nothing.into(),
                        create_test_span(),
                    ),
                    expr: Arc::new(Expr::new_with(
                        ExprKind::Ref("y".to_string()),
                        TypeKind::Nothing.into(),
                        create_test_span(),
                    )),
                }],
            ),
            TypeKind::Nothing.into(),
            create_test_span(),
        );

        let (_, result) = setup_test_context(vec!["x".to_string()], match_expr);
        assert!(matches!(
            result,
            Err(err) if matches!(*err, AnalyzerErrorKind::DuplicateIdentifier { ref name, .. } if name == "y")
        ));
    }

    #[test]
    fn test_block_scope_shadowing() {
        let x_var = "x".to_string();

        // Create a simple let expression with a block that shadows x
        let test_expr = Expr::new_with(
            ExprKind::Let(
                LetBinding::new_with(
                    x_var.clone(),
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(dummy_core_data()),
                        TypeKind::Nothing.into(),
                        create_test_span(),
                    )),
                    TypeKind::Nothing.into(),
                    create_test_span(),
                ),
                Arc::new(Expr::new_with(
                    ExprKind::NewScope(Arc::new(Expr::new_with(
                        ExprKind::Let(
                            LetBinding::new_with(
                                x_var.clone(),
                                Arc::new(Expr::new_with(
                                    ExprKind::CoreExpr(dummy_core_data()),
                                    TypeKind::Nothing.into(),
                                    create_test_span(),
                                )),
                                TypeKind::Nothing.into(),
                                create_test_span(),
                            ),
                            Arc::new(Expr::new_with(
                                ExprKind::Ref(x_var),
                                TypeKind::Nothing.into(),
                                create_test_span(),
                            )),
                        ),
                        TypeKind::Nothing.into(),
                        create_test_span(),
                    ))),
                    TypeKind::Nothing.into(),
                    create_test_span(),
                )),
            ),
            TypeKind::Nothing.into(),
            create_test_span(),
        );

        let (_, result) = setup_test_context(vec![], test_expr);
        assert!(result.is_ok());
    }
}
