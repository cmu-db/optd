use super::error::SemanticErrorKind;
use crate::{
    analyzer::{
        context::Context,
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
pub fn scope_check(hir: &HIR<TypedSpan>) -> Result<(), SemanticErrorKind> {
    use CoreData::*;

    // Process each function in the HIR context separately.
    // Functions might reference each other, so we need to check them individually.
    hir.context
        .get_all_values()
        .iter()
        .try_for_each(|fun| match &fun.data {
            Function(FunKind::Closure(args, body)) => {
                let ctx = create_function_scope(hir.context.clone(), args, &fun.metadata.span)?;
                check_expr(body, ctx)
            }
            _ => panic!("Expected a function, but got: {:?}", fun.data),
        })
}

/// Creates a new context with function parameters bound in a new scope.
/// This ensures proper lexical scoping for function bodies.
fn create_function_scope(
    base_ctx: Context<TypedSpan>,
    params: &[String],
    span: &Span,
) -> Result<Context<TypedSpan>, SemanticErrorKind> {
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
) -> Result<(), SemanticErrorKind> {
    use ExprKind::*;

    let span = &expr.metadata.span;
    match &expr.kind {
        Ref(name) => {
            // Verify variable references exist.
            if ctx.lookup(name).is_none() {
                return Err(SemanticErrorKind::new_invalid_reference(
                    name.to_string(),
                    span.clone(),
                ));
            }
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

        IfThenElse(cond, then_branch, else_branch) => {
            check_expr(cond, ctx.clone())?;
            check_expr(then_branch, ctx.clone())?;
            check_expr(else_branch, ctx)?;
        }

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

        Map(entries) => {
            entries.iter().try_for_each(|(k, v)| {
                check_expr(k, ctx.clone())?;
                check_expr(v, ctx.clone())
            })?;
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
                let fn_ctx = create_function_scope(ctx, params, &span)?;
                check_expr(body, fn_ctx)?;
            }

            CoreData::Fail(expr) => check_expr(expr, ctx)?,

            _ => {}
        },

        CoreVal(value) => {
            if let CoreData::Function(FunKind::Closure(params, body)) = &value.data {
                let fn_ctx = create_function_scope(ctx, params, &span)?;
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
) -> Result<(), SemanticErrorKind> {
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
