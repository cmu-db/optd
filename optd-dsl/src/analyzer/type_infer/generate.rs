use super::solver::Solver;
use crate::{
    analyzer::{
        context::Context,
        errors::AnalyzerErrorKind,
        hir::{
            BinOp, CoreData, Expr, ExprKind, FunKind, HIR, Identifier, LetBinding, MapEntry,
            MatchArm, Pattern, PatternKind, TypedSpan, UnaryOp, Value,
        },
        types::{Type, create_function_type},
    },
    utils::span::Span,
};
use std::sync::Arc;

// TODO(alexis) tomorrow:
// structs, index access returns option, cleanup & audit.
// solver: SAT...
impl Solver<'_> {
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
            .get_all_values()
            .iter()
            .try_for_each(|fun| match &fun.data {
                Function(Closure(args, body)) => {
                    let ctx =
                        self.create_function_scope(hir.context.clone(), &args[..], &fun.metadata)?;
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
        use Type::*;

        let mut fn_ctx = base_ctx;
        fn_ctx.push_scope();

        // Extract the parameter types from the function type.
        let param_types = match &metadata.ty {
            Closure(param_type, _) => match param_type.as_ref() {
                Tuple(types) => types.clone(),
                _ => vec![param_type.as_ref().clone()],
            },
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
            FieldAccess(obj, field_name) => self.generate_field_access(expr, obj, field_name, ctx),
            CoreExpr(core_data) => self.generate_core_expr(expr, core_data, ctx),
            CoreVal(value) => self.generate_core_val(expr, value, ctx),
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
        let mut pattern_types = Vec::with_capacity(arms.len());
        let mut arm_types = Vec::with_capacity(arms.len());

        for arm in arms {
            pattern_types.push(arm.pattern.metadata.clone());
            arm_types.push(arm.expr.metadata.clone());

            let mut arm_ctx = ctx.clone();
            arm_ctx.push_scope();

            self.generate_pattern(&arm.pattern, &mut arm_ctx)?;
            self.generate_expr(&arm.expr, arm_ctx)?;
        }

        self.add_constraint_subtypes(&scrutinee.metadata, &pattern_types);
        self.add_constraint_subtypes(&expr.metadata, &arm_types);

        self.generate_expr(scrutinee, ctx)
    }

    /// Validates pattern bindings, and adds them to the context.
    fn generate_pattern(
        &mut self,
        pattern: &Pattern<TypedSpan>,
        ctx: &mut Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use PatternKind::*;

        match &pattern.kind {
            Bind(name, sub_pattern) => {
                let dummy = Self::dummy_value(&pattern.metadata.ty, &pattern.metadata.span);
                ctx.try_bind(name.clone(), dummy)?;
                self.generate_pattern(sub_pattern, ctx)?;
            }
            Struct(_, field_patterns) => {
                // TODO: Here I should check of structs are valid... Reuse other function, <:
                field_patterns
                    .iter()
                    .try_for_each(|field| self.generate_pattern(field, ctx))?;
            }
            Operator(_) => panic!("Operators may not be in the HIR yet"),
            ArrayDecomp(head, tail) => {
                self.generate_pattern(head, ctx)?;
                self.generate_pattern(tail, ctx)?;
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
        let bool_type = TypedSpan::new(Type::Bool, cond.metadata.span.clone());
        let branch_types = [then_branch.metadata.clone(), else_branch.metadata.clone()];

        self.add_constraint_equal(&bool_type, &cond.metadata);
        self.add_constraint_subtypes(&expr.metadata, &branch_types);

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

        self.add_constraint_subtypes(&expr.metadata, &[inner_expr.metadata.clone()]);
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
        ctx.try_bind(binding.name.to_string(), dummy)?;

        self.add_constraint_subtypes(&binding.metadata, &[binding.expr.metadata.clone()]);
        self.add_constraint_subtypes(&expr.metadata, &[body.metadata.clone()]);

        self.generate_expr(&binding.expr, ctx.clone())?;
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

        let bool_type = TypedSpan::new(Type::Bool, span.clone());
        let arithmetic_type = TypedSpan::new(Type::Arithmetic, span.clone());
        let eqhash_type = TypedSpan::new(Type::EqHash, span.clone());
        let concat_type = TypedSpan::new(Type::Concat, span.clone());
        let i64_type = TypedSpan::new(Type::I64, span.clone());
        let array_i64_type = TypedSpan::new(Type::Array(Type::I64.into()), span.clone());

        let op_types = vec![left.metadata.clone(), right.metadata.clone()];

        match op {
            Add | Sub | Mul | Div => {
                self.add_constraint_subtypes(&expr.metadata, &op_types);
                self.add_constraint_subtypes(&arithmetic_type, &op_types);
            }
            Lt => {
                self.add_constraint_equal(&bool_type, &expr.metadata);
                self.add_constraint_subtypes(&arithmetic_type, &op_types);
                self.add_constraint_equal(&left.metadata, &right.metadata);
            }
            Eq => {
                self.add_constraint_equal(&bool_type, &expr.metadata);
                self.add_constraint_subtypes(&eqhash_type, &op_types);
                self.add_constraint_equal(&left.metadata, &right.metadata);
            }
            And | Or => {
                self.add_constraint_equal(&bool_type, &expr.metadata);
                self.add_constraint_subtypes(&bool_type, &op_types);
            }
            Range => {
                self.add_constraint_equal(&array_i64_type, &expr.metadata);
                self.add_constraint_subtypes(&i64_type, &op_types);
            }
            Concat => {
                self.add_constraint_subtypes(&expr.metadata, &op_types);
                self.add_constraint_subtypes(&concat_type, &op_types);
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
        use Type::*;
        use UnaryOp::*;

        let span = &expr.metadata.span;
        let bool_type = TypedSpan::new(Bool, span.clone());
        let arithmetic_type = TypedSpan::new(Arithmetic, span.clone());

        match op {
            Neg => {
                self.add_constraint_subtypes(&arithmetic_type, &[operand.metadata.clone()]);
                self.add_constraint_subtypes(&expr.metadata, &[operand.metadata.clone()]);
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
    /// - `func >: Closure(args_types, expr.type)`
    fn generate_call(
        &mut self,
        expr: &Expr<TypedSpan>,
        func: &Expr<TypedSpan>,
        args: &[Arc<Expr<TypedSpan>>],
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let arg_types: Vec<_> = args.iter().map(|arg| arg.metadata.ty.clone()).collect();
        let fun_type = TypedSpan::new(
            create_function_type(&arg_types, &expr.metadata.ty),
            expr.metadata.span.clone(),
        );
        self.add_constraint_subtypes(&fun_type, &[func.metadata.clone()]);

        self.generate_expr(func, ctx.clone())?;
        args.iter()
            .try_for_each(|arg| self.generate_expr(arg, ctx.clone()))
    }

    /// Generates constraints for map expressions while verifying scopes.
    /// Enforces the type relationship:
    /// - `Map(EqHash, Universe) >: expr`
    /// - `expr` >: `all maps`
    fn generate_map(
        &mut self,
        expr: &Expr<TypedSpan>,
        entries: &[MapEntry<TypedSpan>],
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use Type::*;

        let span = &expr.metadata.span;

        let map_type = TypedSpan::new(Map(EqHash.into(), Universe.into()), span.clone());
        self.add_constraint_subtypes(&map_type, &[expr.metadata.clone()]);

        let entry_maps: Vec<_> = entries
            .iter()
            .map(|(k, v)| {
                TypedSpan::new(
                    Map(k.metadata.ty.clone().into(), v.metadata.ty.clone().into()),
                    span.clone(),
                )
            })
            .collect();

        self.add_constraint_subtypes(&expr.metadata, &entry_maps);

        entries.iter().try_for_each(|(k, v)| {
            self.generate_expr(k, ctx.clone())?;
            self.generate_expr(v, ctx.clone())
        })
    }

    /// Generates constraints for variable references while verifying scopes.
    /// Enforces the type relationship:
    /// - `expr >: ref_type`
    fn generate_ref(
        &mut self,
        expr: &Expr<TypedSpan>,
        name: &str,
        ctx: &Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        if let Some(value) = ctx.lookup(name) {
            self.add_constraint_subtypes(&expr.metadata, &[value.metadata.clone()]);
        } else {
            return Err(AnalyzerErrorKind::new_undefined_reference(
                name,
                &expr.metadata.span,
            ));
        }

        Ok(())
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
        self.add_constraint_field_access(&expr.metadata, field_name, &obj.metadata);
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
        use Type::*;

        let span = &expr.metadata.span;

        match core_data {
            CoreData::Array(exprs) => {
                let entries: Vec<_> = exprs
                    .iter()
                    .map(|val| TypedSpan::new(Array(val.metadata.ty.clone().into()), span.clone()))
                    .collect();

                self.add_constraint_subtypes(&expr.metadata, &entries);
                exprs
                    .iter()
                    .try_for_each(|expr| self.generate_expr(expr, ctx.clone()))?;
            }
            CoreData::Tuple(exprs) => {
                let element_types: Vec<_> =
                    exprs.iter().map(|expr| expr.metadata.ty.clone()).collect();
                let tuple_type = TypedSpan::new(Tuple(element_types.clone()), span.clone());

                self.add_constraint_subtypes(&expr.metadata, &[tuple_type.clone()]);
                exprs
                    .iter()
                    .try_for_each(|expr| self.generate_expr(expr, ctx.clone()))?;
            }
            CoreData::Struct(_name, exprs) => {
                // TODO: Here I should check of structs are valid (share same function, use <:, access registry)
                exprs
                    .iter()
                    .try_for_each(|expr| self.generate_expr(expr, ctx.clone()))?;
            }
            CoreData::Map(_) | CoreData::Logical(_) | CoreData::Physical(_) => {
                panic!("Types may not be in the HIR yet")
            }
            CoreData::Function(FunKind::Udf(_)) => panic!("UDFs may not appear within functions"),
            CoreData::Function(FunKind::Closure(params, body)) => {
                let fn_ctx = self.create_function_scope(ctx, &params[..], &expr.metadata)?;
                self.generate_expr(body, fn_ctx)?;
            }
            CoreData::Fail(expr) => {
                let string_type = TypedSpan::new(Type::String, expr.metadata.span.clone());
                self.add_constraint_equal(&expr.metadata, &string_type);
                self.generate_expr(expr, ctx)?;
            }
            CoreData::Literal(_) | CoreData::None => {}
        }

        Ok(())
    }

    /// Generates constraints for core values while verifying scopes.
    /// Enforces the type relationship:
    /// - `expr >: value`
    fn generate_core_val(
        &mut self,
        expr: &Expr<TypedSpan>,
        value: &Value<TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        self.add_constraint_subtypes(&expr.metadata, &[value.metadata.clone()]);

        if let CoreData::Function(FunKind::Closure(params, body)) = &value.data {
            let fn_ctx = self.create_function_scope(ctx, &params[..], &value.metadata)?;
            self.generate_expr(body, fn_ctx)?;
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
    use crate::{
        analyzer::{
            context::Context,
            errors::AnalyzerErrorKind,
            hir::{
                CoreData, Expr, ExprKind, FunKind, HIR, LetBinding, MatchArm, Pattern, PatternKind,
                TypedSpan, Value,
            },
            type_infer::solver::Solver,
            types::{Type, TypeRegistry, create_function_type},
        },
        utils::span::Span,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    // Create a test span
    fn test_span(start: usize, end: usize) -> Span {
        Span::new("test".to_string(), start..end)
    }

    // Create a dummy value for binding
    fn dummy_value(span: Span) -> Value<TypedSpan> {
        Value::new_with(CoreData::None, Type::None, span)
    }

    // Setup a context with a function for testing
    fn setup_test_context(
        params: Vec<String>,
        body: Expr<TypedSpan>,
    ) -> (HIR<TypedSpan>, Result<(), Box<AnalyzerErrorKind>>) {
        let mut context = Context::default();
        let registry = TypeRegistry::default();
        let mut solver = Solver::new(&registry);

        // Create function value
        let len = params.len();
        let fun_val = Value {
            data: CoreData::Function(FunKind::Closure(params, Arc::new(body))),
            metadata: TypedSpan {
                ty: create_function_type(&vec![Type::None; len], &Type::None),
                span: test_span(1, 10),
            },
        };

        // Add function to context
        context.bind("test_function".to_string(), fun_val);

        let hir = HIR {
            context,
            annotations: HashMap::new(),
        };

        let result = solver.generate_constraints(&hir);

        (hir, result)
    }

    #[test]
    fn test_valid_reference() {
        let param_name = "x".to_string();
        let body = Expr::new_with(
            ExprKind::Ref(param_name.clone()),
            Type::Nothing,
            test_span(5, 6),
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
                Type::Nothing,
                test_span(5, 13),
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
                        ExprKind::CoreVal(dummy_value(test_span(5, 6))),
                        Type::Nothing,
                        test_span(5, 6),
                    )),
                    Type::Nothing,
                    test_span(5, 6),
                ),
                Arc::new(Expr::new_with(
                    ExprKind::Ref(let_var),
                    Type::Nothing,
                    test_span(7, 8),
                )),
            ),
            Type::Nothing,
            test_span(1, 10),
        );

        let (_, result) = setup_test_context(vec!["x".to_string()], let_expr);
        assert!(result.is_ok());
    }

    #[test]
    fn test_duplicate_parameters() {
        let (_, result) = setup_test_context(
            vec!["x".to_string(), "x".to_string()],
            Expr::new_with(
                ExprKind::Ref("x".to_string()),
                Type::Nothing,
                test_span(5, 6),
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
                        ExprKind::CoreVal(dummy_value(test_span(5, 6))),
                        Type::Nothing,
                        test_span(5, 6),
                    )),
                    Type::Nothing,
                    test_span(5, 6),
                ),
                Arc::new(Expr::new_with(
                    ExprKind::Let(
                        LetBinding::new_with(
                            let_var.clone(),
                            Arc::new(Expr::new_with(
                                ExprKind::CoreVal(dummy_value(test_span(10, 11))),
                                Type::Nothing,
                                test_span(10, 11),
                            )),
                            Type::Nothing,
                            test_span(10, 11),
                        ),
                        Arc::new(Expr::new_with(
                            ExprKind::Ref(let_var),
                            Type::Nothing,
                            test_span(12, 13),
                        )),
                    ),
                    Type::Nothing,
                    test_span(8, 14),
                )),
            ),
            Type::Nothing,
            test_span(1, 15),
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
                    Type::Nothing,
                    test_span(10, 11),
                )),
            ))),
            Type::Closure(Type::None.into(), Type::None.into()),
            test_span(5, 12),
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
                    Type::Nothing,
                    test_span(5, 6),
                )),
                vec![MatchArm {
                    pattern: Pattern::new_with(
                        PatternKind::Bind(
                            "matched".to_string(),
                            Box::new(Pattern::new_with(
                                PatternKind::Wildcard,
                                Type::Nothing,
                                test_span(10, 15),
                            )),
                        ),
                        Type::Nothing,
                        test_span(10, 15),
                    ),
                    expr: Arc::new(Expr::new_with(
                        ExprKind::Ref("matched".to_string()),
                        Type::Nothing,
                        test_span(20, 27),
                    )),
                }],
            ),
            Type::Nothing,
            test_span(1, 30),
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
                    Type::Nothing,
                    test_span(5, 6),
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
                                        Type::Nothing,
                                        test_span(10, 15),
                                    )),
                                ),
                                Type::Nothing,
                                test_span(10, 15),
                            )),
                        ),
                        Type::Nothing,
                        test_span(10, 15),
                    ),
                    expr: Arc::new(Expr::new_with(
                        ExprKind::Ref("y".to_string()),
                        Type::Nothing,
                        test_span(20, 21),
                    )),
                }],
            ),
            Type::Nothing,
            test_span(1, 25),
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
                        ExprKind::CoreVal(dummy_value(test_span(1, 2))),
                        Type::Nothing,
                        test_span(1, 2),
                    )),
                    Type::Nothing,
                    test_span(1, 2),
                ),
                Arc::new(Expr::new_with(
                    ExprKind::NewScope(Arc::new(Expr::new_with(
                        ExprKind::Let(
                            LetBinding::new_with(
                                x_var.clone(),
                                Arc::new(Expr::new_with(
                                    ExprKind::CoreVal(dummy_value(test_span(3, 4))),
                                    Type::Nothing,
                                    test_span(3, 4),
                                )),
                                Type::Nothing,
                                test_span(3, 4),
                            ),
                            Arc::new(Expr::new_with(
                                ExprKind::Ref(x_var),
                                Type::Nothing,
                                test_span(5, 6),
                            )),
                        ),
                        Type::Nothing,
                        test_span(2, 7),
                    ))),
                    Type::Nothing,
                    test_span(1, 8),
                )),
            ),
            Type::Nothing,
            test_span(0, 9),
        );

        let (_, result) = setup_test_context(vec![], test_expr);
        assert!(result.is_ok());
    }
}
