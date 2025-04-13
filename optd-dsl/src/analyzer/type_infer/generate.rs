use std::sync::Arc;

use crate::analyzer::{
    context::Context,
    errors::AnalyzerErrorKind,
    hir::{
        self, BinOp, CoreData, Expr, ExprKind, FunKind, HIR, MatchArm, Pattern, PatternKind,
        TypedSpan, UnaryOp, Value,
    },
    types::Type,
};

impl super::solver::Solver {
    /// Generates type constraints from an HIR while verifying scopes.
    ///
    /// This method traverses the HIR and simultaneously:
    /// 1. Ensures all variable references have proper bindings in the lexical scope
    /// 2. Detects duplicate identifiers in the same scope
    /// 3. Generates type constraints for the solver to later resolve
    ///
    /// # Arguments
    ///
    /// * `hir` - The HIR to analyze and generate constraints from
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
                        self.create_function_scope(hir.context.clone(), args, &fun.metadata)?;
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
        params: &[String],
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
        for (name, ty) in params.into_iter().zip(param_types.iter()) {
            let dummy = Value::new_with(CoreData::None, ty.clone(), metadata.span.clone());
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
            Let(name, value, body) => self.generate_let(expr, name, value, body, ctx),
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
        self.generate_expr(scrutinee, ctx.clone())?;

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

        Ok(())
    }

    /// Generates constraints for if-then-else expressions while verifying scopes.
    /// Enforces two key type relationships:
    /// - `bool >: condition` - Ensures condition evaluates to a boolean
    /// - `expr >: then, else` - Ensures result type is compatible with both branches
    fn generate_if_then_else(
        &mut self,
        expr: &Expr<TypedSpan>,
        cond: &Expr<TypedSpan>,
        then_branch: &Expr<TypedSpan>,
        else_branch: &Expr<TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        self.generate_expr(cond, ctx.clone())?;

        let bool_type = TypedSpan {
            ty: Type::Bool,
            span: cond.metadata.span.clone(),
        };

        let mut branch_types = Vec::with_capacity(2);
        branch_types.push(then_branch.metadata.clone());
        branch_types.push(else_branch.metadata.clone());

        self.add_constraint_subtypes(&bool_type, &[cond.metadata.clone()]);
        self.add_constraint_subtypes(&expr.metadata, &branch_types);

        self.generate_expr(then_branch, ctx.clone())?;
        self.generate_expr(else_branch, ctx)
    }

    /// Generates constraints for new scope expressions while verifying scopes.
    /// Enforces the type relationship:
    /// - `outer >: inner` - Ensures scope result type is compatible with inner expression
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
    /// - `expr >: val >: body` - Ensures proper type flow through the binding and expression
    fn generate_let(
        &mut self,
        expr: &Expr<TypedSpan>,
        name: &str,
        value: &Expr<TypedSpan>,
        body: &Expr<TypedSpan>,
        mut ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        self.generate_expr(value, ctx.clone())?;

        let dummy = Value::new_with(
            CoreData::None,
            value.metadata.ty.clone(),
            value.metadata.span.clone(),
        );
        ctx.try_bind(name.to_string(), dummy)?;

        self.add_constraint_subtypes(&expr.metadata, &[value.metadata.clone()]);
        self.add_constraint_subtypes(&value.metadata, &[body.metadata.clone()]);

        self.generate_expr(body, ctx)
    }

    /// Generates constraints for binary operations while verifying scopes.
    /// Enforces type relationships based on operation:
    /// - Add/Sub/Mul/Div: expr >: left, right and Arithmetic >: left, right
    /// - Lt: bool >: expr, Arithmetic >: left, right, left >: right, right >: left
    /// - Eq: bool >: expr, EqHash >: left, right, left >: right, right >: left
    /// - And/Or: bool >: expr, bool >: left, right
    /// - Range: Array(Int64) >: expr, Int64 >: left, right
    /// - Concat: expr >: left, right, Concat >: left, right
    fn generate_binary(
        &mut self,
        expr: &Expr<TypedSpan>,
        left: &Expr<TypedSpan>,
        op: &BinOp,
        right: &Expr<TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use BinOp::*;

        self.generate_expr(left, ctx.clone())?;
        self.generate_expr(right, ctx)?;

        let span = &expr.metadata.span;

        let bool_type = TypedSpan::new(Type::Bool, span.clone());
        let arithmetic_type = TypedSpan::new(Type::Arithmetic, span.clone());
        let eqhash_type = TypedSpan::new(Type::EqHash, span.clone());
        let concat_type = TypedSpan::new(Type::Concat, span.clone());
        let int64_type = TypedSpan::new(Type::Int64, span.clone());
        let array_int64_type = TypedSpan::new(Type::Array(Type::Int64.into()), span.clone());

        let op_types = vec![left.metadata.clone(), right.metadata.clone()];

        match op {
            Add | Sub | Mul | Div => {
                self.add_constraint_subtypes(&expr.metadata, &op_types);
                self.add_constraint_subtypes(&arithmetic_type, &op_types);
            }
            Lt => {
                self.add_constraint_subtypes(&bool_type, &[expr.metadata.clone()]);
                self.add_constraint_subtypes(&arithmetic_type, &op_types);
                self.add_constraint_subtypes(&left.metadata, &[right.metadata.clone()]);
                self.add_constraint_subtypes(&right.metadata, &[left.metadata.clone()]);
            }
            Eq => {
                self.add_constraint_subtypes(&bool_type, &[expr.metadata.clone()]);
                self.add_constraint_subtypes(&eqhash_type, &op_types);
                self.add_constraint_subtypes(&left.metadata, &[right.metadata.clone()]);
                self.add_constraint_subtypes(&right.metadata, &[left.metadata.clone()]);
            }
            And | Or => {
                self.add_constraint_subtypes(&bool_type, &[expr.metadata.clone()]);
                self.add_constraint_subtypes(&bool_type, &op_types);
            }
            Range => {
                self.add_constraint_subtypes(&array_int64_type, &[expr.metadata.clone()]);
                self.add_constraint_subtypes(&int64_type, &op_types);
            }
            Concat => {
                self.add_constraint_subtypes(&expr.metadata, &op_types);
                self.add_constraint_subtypes(&concat_type, &op_types);
            }
        }

        Ok(())
    }

    /// Generates constraints for unary operations while verifying scopes.
    /// Enforces type relationships based on operation:
    /// - Not: bool >: operand, bool >: expr
    /// - Neg: Arithmetic >: operand, expr >: operand
    fn generate_unary(
        &mut self,
        expr: &Expr<TypedSpan>,
        op: &UnaryOp,
        operand: &Expr<TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use UnaryOp::*;

        self.generate_expr(operand, ctx)?;

        let span = &expr.metadata.span;
        let bool_type = TypedSpan::new(Type::Bool, span.clone());
        let arithmetic_type = TypedSpan::new(Type::Arithmetic, span.clone());

        match op {
            Neg => {
                self.add_constraint_subtypes(&arithmetic_type, &[operand.metadata.clone()]);
                self.add_constraint_subtypes(&expr.metadata, &[operand.metadata.clone()]);
            }
            Not => {
                self.add_constraint_subtypes(&bool_type, &[operand.metadata.clone()]);
                self.add_constraint_subtypes(&bool_type, &[expr.metadata.clone()]);
            }
        }

        Ok(())
    }

    /// Generates constraints for function calls while verifying scopes.
    /// Enforces the type relationship:
    /// - Closure(args_types, expr.type) >: func - Ensures function type can accept the arguments
    fn generate_call(
        &mut self,
        expr: &Expr<TypedSpan>,
        func: &Expr<TypedSpan>,
        args: &[Arc<Expr<TypedSpan>>],
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        // TODO: This is wrong as of now!!
        // Need to: handle generics (probably <: resciprocally), add id!
        // Handle arrays, maps, tuples (most likely: inherit from the function)
        // check args, check return type (easy)
        // also: need a way to create new unknown!
        
        /*self.generate_expr(func, ctx.clone())?;

        let arg_metadata: Vec<TypedSpan> = args.iter().map(|arg| arg.metadata.clone()).collect();

        // Create the expected function type
        let fn_type = if args.len() == 1 {
            // For single argument functions
            let fn_type = TypedSpan::new(
                Type::Closure(
                    Box::new(args[0].metadata.ty.clone()),
                    Box::new(expr.metadata.ty.clone()),
                ),
                expr.metadata.span.clone(),
            );
            self.add_constraint_subtypes(&fn_type, &[func.metadata.clone()]);
        } else if !args.is_empty() {
            // For multi-argument functions
            let param_types: Vec<Type> = args.iter().map(|arg| arg.metadata.ty.clone()).collect();

            let fn_type = TypedSpan::new(
                Type::Closure(
                    Box::new(Type::Tuple(param_types)),
                    Box::new(expr.metadata.ty.clone()),
                ),
                expr.metadata.span.clone(),
            );
            self.add_constraint_subtypes(&fn_type, &[func.metadata.clone()]);
        };

        // Process all arguments
        args.iter()
            .try_for_each(|arg| self.generate_expr(arg, ctx.clone()))*/

        Ok(())
    }

    /// Generates constraints for map expressions while verifying scopes.
    /// Ensures all keys have the same type and all values have the same type.
    fn generate_map(
        &mut self,
        expr: &Expr<TypedSpan>,
        entries: &[(Arc<Expr<TypedSpan>>, Arc<Expr<TypedSpan>>)],
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let span = &expr.metadata.span;
        let expr_type = &expr.metadata.ty;

        // If we have entries, determine key and value types
        if !entries.is_empty() {
            let first_key_type = &entries[0].0.metadata.ty;
            let first_value_type = &entries[0].1.metadata.ty;

            // Ensure all keys have the same type
            for (k, _) in entries.iter() {
                self.add_constraint_equal(
                    k.metadata.span.clone(),
                    k.metadata.ty.clone(),
                    first_key_type.clone(),
                );
            }

            // Ensure all values have the same type
            for (_, v) in entries.iter() {
                self.add_constraint_equal(
                    v.metadata.span.clone(),
                    v.metadata.ty.clone(),
                    first_value_type.clone(),
                );
            }

            // Map type should be Map(key_type, value_type)
            self.add_constraint_equal(
                span.clone(),
                expr_type.clone(),
                Type::Map(
                    Box::new(first_key_type.clone()),
                    Box::new(first_value_type.clone()),
                ),
            );

            // Keys should implement EqHash
            self.add_constraint_subtype(span.clone(), first_key_type.clone(), Type::EqHash);
        }

        entries.iter().try_for_each(|(k, v)| {
            self.generate_expr(k, ctx.clone())?;
            self.generate_expr(v, ctx.clone())
        })
    }

    /// Generates constraints for variable references while verifying scopes.
    /// Ensures the referenced variable exists and has the correct type.
    fn generate_ref(
        &mut self,
        expr: &Expr<TypedSpan>,
        name: &str,
        ctx: &Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let span = &expr.metadata.span;
        let expr_type = &expr.metadata.ty;

        // Verify variable references exist.
        if let Some(value) = ctx.lookup(name) {
            // Add constraint: reference type should match the declared type
            self.add_constraint_equal(span.clone(), expr_type.clone(), value.metadata.ty.clone());
        } else {
            return Err(AnalyzerErrorKind::new_undefined_reference(name, span));
        }

        Ok(())
    }

    /// Generates constraints for field access expressions while verifying scopes.
    /// Ensures the object has a field with the specified name.
    fn generate_field_access(
        &mut self,
        expr: &Expr<TypedSpan>,
        obj: &Expr<TypedSpan>,
        field_name: &str,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let span = &expr.metadata.span;
        let expr_type = &expr.metadata.ty;

        self.generate_expr(obj, ctx)?;

        // Generate field access constraint
        self.add_constraint_field_access(
            span.clone(),
            obj.metadata.ty.clone(),
            field_name.to_string(),
            expr_type.clone(),
        );

        Ok(())
    }

    /// Generates constraints for core expressions while verifying scopes.
    /// Handles arrays, tuples, structs, functions, and literals.
    fn generate_core_expr(
        &mut self,
        expr: &Expr<TypedSpan>,
        core_data: &CoreData<std::sync::Arc<Expr<TypedSpan>>, TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let span = &expr.metadata.span;
        let expr_type = &expr.metadata.ty;

        use CoreData::*;
        use FunKind::*;

        match core_data {
            Array(exprs) => {
                // If array is not empty, all elements should have the same type
                if !exprs.is_empty() {
                    let first_elem_type = &exprs[0].metadata.ty;

                    for expr in exprs.iter().skip(1) {
                        self.add_constraint_equal(
                            expr.metadata.span.clone(),
                            expr.metadata.ty.clone(),
                            first_elem_type.clone(),
                        );
                    }

                    // Array type should be Array(element_type)
                    self.add_constraint_equal(
                        span.clone(),
                        expr_type.clone(),
                        Type::Array(Box::new(first_elem_type.clone())),
                    );
                }

                exprs
                    .iter()
                    .try_for_each(|expr| self.generate_expr(expr, ctx.clone()))?;
            }
            Tuple(exprs) => {
                // Tuple type should be Tuple(element_types)
                let element_types: Vec<Type> =
                    exprs.iter().map(|expr| expr.metadata.ty.clone()).collect();

                self.add_constraint_equal(
                    span.clone(),
                    expr_type.clone(),
                    Type::Tuple(element_types),
                );

                exprs
                    .iter()
                    .try_for_each(|expr| self.generate_expr(expr, ctx.clone()))?;
            }
            Struct(name, exprs) => {
                // Struct type should be Adt(name)
                self.add_constraint_equal(span.clone(), expr_type.clone(), Type::Adt(name.clone()));

                exprs
                    .iter()
                    .try_for_each(|expr| self.generate_expr(expr, ctx.clone()))?;
            }
            Function(Closure(params, body)) => {
                // Function definitions create a new lexical scope.
                let fn_ctx = self.create_function_scope(ctx, params, &expr.metadata)?;

                // Generate function type constraint
                self.add_constraint_equal(
                    span.clone(),
                    expr_type.clone(),
                    Type::Closure(
                        Box::new(Type::Tuple(vec![])),
                        Box::new(body.metadata.ty.clone()),
                    ),
                );

                self.generate_expr(body, fn_ctx)?;
            }
            Fail(expr) => {
                self.generate_expr(expr, ctx)?;

                // Fail expression has Nothing type (can be used anywhere)
                self.add_constraint_equal(span.clone(), expr_type.clone(), Type::Nothing);
            }
            Literal(lit) => {
                // Add literal type constraint based on the literal type
                match lit {
                    hir::Literal::Int64(_) => {
                        self.add_constraint_equal(span.clone(), expr_type.clone(), Type::Int64);
                    }
                    hir::Literal::Float64(_) => {
                        self.add_constraint_equal(span.clone(), expr_type.clone(), Type::Float64);
                    }
                    hir::Literal::String(_) => {
                        self.add_constraint_equal(span.clone(), expr_type.clone(), Type::String);
                    }
                    hir::Literal::Bool(_) => {
                        self.add_constraint_equal(span.clone(), expr_type.clone(), Type::Bool);
                    }
                    hir::Literal::Unit => {
                        self.add_constraint_equal(span.clone(), expr_type.clone(), Type::Unit);
                    }
                }
            }
            None => {
                self.add_constraint_equal(span.clone(), expr_type.clone(), Type::None);
            }
            _ => {}
        }

        Ok(())
    }

    /// Generates constraints for core values while verifying scopes.
    /// Handles function values.
    fn generate_core_val(
        &mut self,
        expr: &Expr<TypedSpan>,
        value: &Value<TypedSpan>,
        ctx: Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let span = &expr.metadata.span;
        let expr_type = &expr.metadata.ty;

        // Value's type should match expression type
        self.add_constraint_equal(span.clone(), expr_type.clone(), value.metadata.ty.clone());

        if let CoreData::Function(FunKind::Closure(params, body)) = &value.data {
            let fn_ctx = self.create_function_scope(ctx, params, &value.metadata)?;
            self.generate_expr(body, fn_ctx)?;
        }

        Ok(())
    }

    /// Validates pattern bindings, adds them to the context, and generates type constraints.
    fn generate_pattern(
        &mut self,
        pattern: &Pattern<TypedSpan>,
        ctx: &mut Context<TypedSpan>,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use PatternKind::*;

        let pattern_type = &pattern.metadata.ty;
        let span = &pattern.metadata.span;

        match &pattern.kind {
            Bind(name, sub_pattern) => {
                // Add the binding to the current scope, checking for duplicates.
                let dummy = Value::new_with(
                    CoreData::None,
                    pattern_type.clone(),
                    pattern.metadata.span.clone(),
                );
                ctx.try_bind(name.clone(), dummy)?;

                // Pattern type should match sub-pattern type
                self.add_constraint_equal(
                    span.clone(),
                    pattern_type.clone(),
                    sub_pattern.metadata.ty.clone(),
                );

                self.generate_pattern(sub_pattern, ctx)?;
            }
            Literal(lit) => {
                // Add constraint based on literal type
                match lit {
                    hir::Literal::Int64(_) => {
                        self.add_constraint_equal(span.clone(), pattern_type.clone(), Type::Int64);
                    }
                    hir::Literal::Float64(_) => {
                        self.add_constraint_equal(
                            span.clone(),
                            pattern_type.clone(),
                            Type::Float64,
                        );
                    }
                    hir::Literal::String(_) => {
                        self.add_constraint_equal(span.clone(), pattern_type.clone(), Type::String);
                    }
                    hir::Literal::Bool(_) => {
                        self.add_constraint_equal(span.clone(), pattern_type.clone(), Type::Bool);
                    }
                    hir::Literal::Unit => {
                        self.add_constraint_equal(span.clone(), pattern_type.clone(), Type::Unit);
                    }
                }
            }
            Struct(name, field_patterns) => {
                // Add type constraint for struct pattern
                self.add_constraint_equal(
                    span.clone(),
                    pattern_type.clone(),
                    Type::Adt(name.clone()),
                );

                field_patterns
                    .iter()
                    .try_for_each(|field| self.generate_pattern(field, ctx))?;
            }
            Operator(_) => panic!("Operators may not be in the HIR yet"),
            ArrayDecomp(head, tail) => {
                // The pattern type should be Array(element_type)
                let element_type = head.metadata.ty.clone();

                self.add_constraint_equal(
                    span.clone(),
                    pattern_type.clone(),
                    Type::Array(Box::new(element_type.clone())),
                );

                // Tail should have type Array(element_type)
                self.add_constraint_equal(
                    tail.metadata.span.clone(),
                    tail.metadata.ty.clone(),
                    Type::Array(Box::new(element_type)),
                );

                // Both head and tail can introduce bindings.
                self.generate_pattern(head, ctx)?;
                self.generate_pattern(tail, ctx)?;
            }
            Wildcard => {
                // Wildcard can match any type, no constraints to add
            }
            EmptyArray => {
                // EmptyArray pattern has type Array(element_type)
                // but we don't know the element type
                // We'll leave it to the other constraints to determine this
                let element_id = match pattern_type {
                    Type::Array(elem) => elem,
                    Type::Unknown(id) => {
                        // If pattern type is unknown, constrain it to be an array
                        let unknown_id = *id;
                        let element_type = Type::Unknown(unknown_id + 1);
                        self.add_constraint_equal(
                            span.clone(),
                            pattern_type.clone(),
                            Type::Array(Box::new(element_type.clone())),
                        );
                        &Box::new(element_type)
                    }
                    _ => {
                        // If pattern type is not an array or unknown,
                        // constraint it to be an array with unknown element type
                        let element_type = Type::Unknown(0);
                        self.add_constraint_equal(
                            span.clone(),
                            pattern_type.clone(),
                            Type::Array(Box::new(element_type.clone())),
                        );
                        &Box::new(element_type)
                    }
                };

                // EmptyArray should have type Array of some element type
                self.add_constraint_equal(
                    span.clone(),
                    pattern_type.clone(),
                    Type::Array(element_id.clone()),
                );
            }
        }

        Ok(())
    }
}

/// Tests for the constraint generation functionality
#[cfg(test)]
mod scope_check_tests {
    use crate::{
        analyzer::{
            context::Context,
            errors::AnalyzerErrorKind,
            hir::{
                CoreData, Expr, ExprKind, FunKind, HIR, MatchArm, Pattern, PatternKind, TypedSpan,
                Value,
            },
            types::Type,
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
        let mut solver = super::super::solver::Solver::default();

        // Create function value
        let fun_val = Value {
            data: CoreData::Function(FunKind::Closure(params, Arc::new(body))),
            metadata: TypedSpan {
                ty: Type::Nothing,
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
                let_var.clone(),
                Arc::new(Expr::new_with(
                    ExprKind::CoreVal(dummy_value(test_span(5, 6))),
                    Type::Nothing,
                    test_span(5, 6),
                )),
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
                let_var.clone(),
                Arc::new(Expr::new_with(
                    ExprKind::CoreVal(dummy_value(test_span(5, 6))),
                    Type::Nothing,
                    test_span(5, 6),
                )),
                Arc::new(Expr::new_with(
                    ExprKind::Let(
                        let_var.clone(),
                        Arc::new(Expr::new_with(
                            ExprKind::CoreVal(dummy_value(test_span(10, 11))),
                            Type::Nothing,
                            test_span(10, 11),
                        )),
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
            Type::Nothing,
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
                x_var.clone(),
                Arc::new(Expr::new_with(
                    ExprKind::CoreVal(dummy_value(test_span(1, 2))),
                    Type::Nothing,
                    test_span(1, 2),
                )),
                Arc::new(Expr::new_with(
                    ExprKind::NewScope(Arc::new(Expr::new_with(
                        ExprKind::Let(
                            x_var.clone(),
                            Arc::new(Expr::new_with(
                                ExprKind::CoreVal(dummy_value(test_span(3, 4))),
                                Type::Nothing,
                                test_span(3, 4),
                            )),
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
