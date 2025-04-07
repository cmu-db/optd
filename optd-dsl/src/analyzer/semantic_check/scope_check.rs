use crate::{
    analyzer::{
        context::Context,
        error::AnalyzerErrorKind,
        hir::{CoreData, Expr, ExprKind, FunKind, HIR, Pattern, PatternKind, TypedSpan, Value},
    },
    utils::span::Span,
};

/// Performs scope checking on an HIR.
///
/// Verifies that all variable references exist in the appropriate context
/// and detects duplicate identifiers.
///
/// # Arguments
///
/// * `hir` - The HIR to check for scope errors.
///
/// # Returns
///
/// * `Ok(())` if no scope errors are found.
pub fn scope_check(hir: &HIR<TypedSpan>) -> Result<(), AnalyzerErrorKind> {
    use CoreData::*;
    use FunKind::*;

    // Process each function in the HIR context separately.
    // Functions might reference each other, so we need to check them individually.
    hir.context
        .get_all_values()
        .iter()
        .try_for_each(|fun| match &fun.data {
            Function(Closure(args, body)) => {
                let ctx = create_function_scope(hir.context.clone(), args, &fun.metadata.span)?;
                check_expr(body, ctx)
            }
            Function(Udf(_)) => Ok(()), // UDFs have no body to check.
            _ => panic!("Expected a function, but got: {:?}", fun.data),
        })
}

/// Creates a new context with function parameters bound in a new scope.
/// This ensures proper lexical scoping for function bodies.
fn create_function_scope(
    base_ctx: Context<TypedSpan>,
    params: &[String],
    span: &Span,
) -> Result<Context<TypedSpan>, AnalyzerErrorKind> {
    let mut fn_ctx = base_ctx;
    fn_ctx.push_scope();

    for param in params {
        let dummy = Value::new_unknown(CoreData::None, span.clone());
        fn_ctx.try_bind(param.clone(), dummy)?;
    }

    Ok(fn_ctx)
}

/// Main expression scope checker that recursively validates
/// variable references and introduces new scopes as needed.
fn check_expr(
    expr: &Expr<TypedSpan>,
    mut ctx: Context<TypedSpan>,
) -> Result<(), AnalyzerErrorKind> {
    use ExprKind::*;

    let span = &expr.metadata.span;
    match &expr.kind {
        PatternMatch(scrutinee, arms) => {
            check_expr(scrutinee, ctx.clone())?;

            arms.iter().try_for_each(|arm| {
                // Each match arm introduces its own scope and bindings.
                let mut arm_ctx = ctx.clone();
                arm_ctx.push_scope();

                check_pattern(&arm.pattern, &mut arm_ctx)?;
                check_expr(&arm.expr, arm_ctx)
            })?;
        }
        IfThenElse(cond, then_branch, else_branch) => {
            check_expr(cond, ctx.clone())?;
            check_expr(then_branch, ctx.clone())?;
            check_expr(else_branch, ctx)?;
        }
        NewScope(expr) => {
            let mut new_ctx = ctx.clone();
            new_ctx.push_scope();
            check_expr(expr, new_ctx)?;
        }
        Let(name, value, body) => {
            check_expr(value, ctx.clone())?;

            // Bind the name before checking the body
            // This simulates the lexical scoping rules.
            let dummy = Value::new_unknown(CoreData::None, span.clone());
            ctx.try_bind(name.to_string(), dummy)?;

            check_expr(body, ctx)?;
        }
        Binary(left, _, right) => {
            check_expr(left, ctx.clone())?;
            check_expr(right, ctx)?;
        }
        Unary(_, operand) => check_expr(operand, ctx)?,
        Call(func, args) => {
            check_expr(func, ctx.clone())?;
            args.iter()
                .try_for_each(|arg| check_expr(arg, ctx.clone()))?;
        }
        Map(entries) => {
            entries.iter().try_for_each(|(k, v)| {
                check_expr(k, ctx.clone())?;
                check_expr(v, ctx.clone())
            })?;
        }
        Ref(name) => {
            // Verify variable references exist.
            if ctx.lookup(name).is_none() {
                return Err(AnalyzerErrorKind::new_invalid_reference(
                    name.to_string(),
                    span.clone(),
                ));
            }
        }
        FieldAccess(obj, _) => check_expr(obj, ctx)?,
        CoreExpr(core_data) => match core_data {
            CoreData::Array(exprs) | CoreData::Tuple(exprs) | CoreData::Struct(_, exprs) => {
                exprs
                    .iter()
                    .try_for_each(|expr| check_expr(expr, ctx.clone()))?;
            }

            CoreData::Function(FunKind::Closure(params, body)) => {
                // Function definitions create a new lexical scope.
                let fn_ctx = create_function_scope(ctx, params, span)?;
                check_expr(body, fn_ctx)?;
            }

            CoreData::Fail(expr) => check_expr(expr, ctx)?,

            _ => {}
        },
        CoreVal(value) => {
            if let CoreData::Function(FunKind::Closure(params, body)) = &value.data {
                let fn_ctx = create_function_scope(ctx, params, span)?;
                check_expr(body, fn_ctx)?;
            }
        }
    }

    Ok(())
}

/// Validates pattern bindings and adds them to the context.
/// Patterns in matches introduce bindings that must be checked for duplicates.
fn check_pattern(
    pattern: &Pattern<TypedSpan>,
    ctx: &mut Context<TypedSpan>,
) -> Result<(), AnalyzerErrorKind> {
    use PatternKind::*;

    match &pattern.kind {
        Bind(name, sub_pattern) => {
            // Add the binding to the current scope, checking for duplicates.
            let dummy = Value::new_unknown(CoreData::None, pattern.metadata.span.clone());
            ctx.try_bind(name.clone(), dummy)?;
            check_pattern(sub_pattern, ctx)?;
        }

        Struct(_, field_patterns) => {
            field_patterns
                .iter()
                .try_for_each(|field| check_pattern(field, ctx))?;
        }

        Operator(_) => panic!("Operators may not be in the HIR yet"),

        ArrayDecomp(head, tail) => {
            // Both head and tail can introduce bindings.
            check_pattern(head, ctx)?;
            check_pattern(tail, ctx)?;
        }

        Literal(_) | Wildcard | EmptyArray => {}
    }

    Ok(())
}

#[cfg(test)]
mod scope_check_tests {
    use super::scope_check;
    use crate::{
        analyzer::{
            context::Context,
            error::AnalyzerErrorKind,
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
        Value::new_unknown(CoreData::None, span)
    }

    // Setup a context with a function for testing
    fn setup_test_context(
        params: Vec<String>,
        body: Expr<TypedSpan>,
    ) -> (HIR<TypedSpan>, Result<(), AnalyzerErrorKind>) {
        let mut context = Context::default();

        // Create function value
        let fun_val = Value {
            data: CoreData::Function(FunKind::Closure(params, Arc::new(body))),
            metadata: TypedSpan {
                ty: Type::Unknown,
                span: test_span(1, 10),
            },
        };

        // Add function to context
        context.bind("test_function".to_string(), fun_val);

        let hir = HIR {
            context,
            annotations: HashMap::new(),
        };

        let result = scope_check(&hir);

        (hir, result)
    }

    #[test]
    fn test_valid_reference() {
        let param_name = "x".to_string();
        let body = Expr::new_unknown(ExprKind::Ref(param_name.clone()), test_span(5, 6));
        let (_, result) = setup_test_context(vec![param_name], body);
        assert!(result.is_ok());
    }

    #[test]
    fn test_undefined_reference() {
        let (_, result) = setup_test_context(
            vec!["x".to_string()],
            Expr::new_unknown(ExprKind::Ref("undefined".to_string()), test_span(5, 13)),
        );
        assert!(
            matches!(result, Err(AnalyzerErrorKind::InvalidReference { name, .. }) if name == "undefined")
        );
    }

    #[test]
    fn test_let_binding() {
        let let_var = "y".to_string();
        let let_expr = Expr::new_unknown(
            ExprKind::Let(
                let_var.clone(),
                Arc::new(Expr::new_unknown(
                    ExprKind::CoreVal(dummy_value(test_span(5, 6))),
                    test_span(5, 6),
                )),
                Arc::new(Expr::new_unknown(ExprKind::Ref(let_var), test_span(7, 8))),
            ),
            test_span(1, 10),
        );

        let (_, result) = setup_test_context(vec!["x".to_string()], let_expr);
        assert!(result.is_ok());
    }

    #[test]
    fn test_duplicate_parameters() {
        let (_, result) = setup_test_context(
            vec!["x".to_string(), "x".to_string()],
            Expr::new_unknown(ExprKind::Ref("x".to_string()), test_span(5, 6)),
        );
        assert!(
            matches!(result, Err(AnalyzerErrorKind::DuplicateIdentifier { name, .. }) if name == "x")
        );
    }

    #[test]
    fn test_duplicate_let() {
        let let_var = "y".to_string();
        let outer_let = Expr::new_unknown(
            ExprKind::Let(
                let_var.clone(),
                Arc::new(Expr::new_unknown(
                    ExprKind::CoreVal(dummy_value(test_span(5, 6))),
                    test_span(5, 6),
                )),
                Arc::new(Expr::new_unknown(
                    ExprKind::Let(
                        let_var.clone(),
                        Arc::new(Expr::new_unknown(
                            ExprKind::CoreVal(dummy_value(test_span(10, 11))),
                            test_span(10, 11),
                        )),
                        Arc::new(Expr::new_unknown(ExprKind::Ref(let_var), test_span(12, 13))),
                    ),
                    test_span(8, 14),
                )),
            ),
            test_span(1, 15),
        );

        let (_, result) = setup_test_context(vec!["x".to_string()], outer_let);
        assert!(
            matches!(result, Err(AnalyzerErrorKind::DuplicateIdentifier { name, .. }) if name == "y")
        );
    }

    #[test]
    fn test_nested_function() {
        let outer_param = "x".to_string();
        let inner_param = "y".to_string();

        let inner_fn = Expr::new_unknown(
            ExprKind::CoreExpr(CoreData::Function(FunKind::Closure(
                vec![inner_param],
                Arc::new(Expr::new_unknown(
                    ExprKind::Ref(outer_param.clone()),
                    test_span(10, 11),
                )),
            ))),
            test_span(5, 12),
        );

        let (_, result) = setup_test_context(vec![outer_param], inner_fn);
        assert!(result.is_ok());
    }

    #[test]
    fn test_pattern_match() {
        let match_expr = Expr::new_unknown(
            ExprKind::PatternMatch(
                Arc::new(Expr::new_unknown(
                    ExprKind::Ref("x".to_string()),
                    test_span(5, 6),
                )),
                vec![MatchArm {
                    pattern: Pattern::new_unknown(
                        PatternKind::Bind(
                            "matched".to_string(),
                            Box::new(Pattern::new_unknown(
                                PatternKind::Wildcard,
                                test_span(10, 15),
                            )),
                        ),
                        test_span(10, 15),
                    ),
                    expr: Arc::new(Expr::new_unknown(
                        ExprKind::Ref("matched".to_string()),
                        test_span(20, 27),
                    )),
                }],
            ),
            test_span(1, 30),
        );

        let (_, result) = setup_test_context(vec!["x".to_string()], match_expr);
        assert!(result.is_ok());
    }

    #[test]
    fn test_duplicate_pattern_bindings() {
        let match_expr = Expr::new_unknown(
            ExprKind::PatternMatch(
                Arc::new(Expr::new_unknown(
                    ExprKind::Ref("x".to_string()),
                    test_span(5, 6),
                )),
                vec![MatchArm {
                    pattern: Pattern::new_unknown(
                        PatternKind::Bind(
                            "y".to_string(),
                            Box::new(Pattern::new_unknown(
                                PatternKind::Bind(
                                    "y".to_string(),
                                    Box::new(Pattern::new_unknown(
                                        PatternKind::Wildcard,
                                        test_span(10, 15),
                                    )),
                                ),
                                test_span(10, 15),
                            )),
                        ),
                        test_span(10, 15),
                    ),
                    expr: Arc::new(Expr::new_unknown(
                        ExprKind::Ref("y".to_string()),
                        test_span(20, 21),
                    )),
                }],
            ),
            test_span(1, 25),
        );

        let (_, result) = setup_test_context(vec!["x".to_string()], match_expr);
        assert!(
            matches!(result, Err(AnalyzerErrorKind::DuplicateIdentifier { name, .. }) if name == "y")
        );
    }

    #[test]
    fn test_block_scope_shadowing() {
        let x_var = "x".to_string();

        // Create a simple let expression with a block that shadows x
        let test_expr = Expr::new_unknown(
            ExprKind::Let(
                x_var.clone(),
                Arc::new(Expr::new_unknown(
                    ExprKind::CoreVal(dummy_value(test_span(1, 2))),
                    test_span(1, 2),
                )),
                Arc::new(Expr::new_unknown(
                    ExprKind::NewScope(Arc::new(Expr::new_unknown(
                        ExprKind::Let(
                            x_var.clone(),
                            Arc::new(Expr::new_unknown(
                                ExprKind::CoreVal(dummy_value(test_span(3, 4))),
                                test_span(3, 4),
                            )),
                            Arc::new(Expr::new_unknown(ExprKind::Ref(x_var), test_span(5, 6))),
                        ),
                        test_span(2, 7),
                    ))),
                    test_span(1, 8),
                )),
            ),
            test_span(0, 9),
        );

        let (_, result) = setup_test_context(vec![], test_expr);
        assert!(result.is_ok());
    }
}
