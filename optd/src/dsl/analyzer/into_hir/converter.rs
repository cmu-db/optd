use crate::dsl::analyzer::{
    errors::AnalyzerErrorKind,
    hir::{
        CoreData, Expr, ExprKind, FunKind, LetBinding, MatchArm, Pattern, PatternKind, TypedSpan,
    },
    type_checks::registry::TypeRegistry,
};
use std::sync::Arc;

pub(super) fn convert_expr(
    expr: &Arc<Expr<TypedSpan>>,
    registry: &TypeRegistry,
) -> Result<Arc<Expr>, Box<AnalyzerErrorKind>> {
    use ExprKind::*;

    let converted_kind = match &expr.kind {
        PatternMatch(expr, arms) => {
            let converted_arms = arms
                .iter()
                .map(|arm| {
                    Ok(MatchArm {
                        pattern: convert_pattern(&arm.pattern, registry)?,
                        expr: convert_expr(&arm.expr, registry)?,
                    })
                })
                .collect::<Result<Vec<_>, Box<_>>>()?;

            PatternMatch(convert_expr(expr, registry)?, converted_arms)
        }
        IfThenElse(cond, then_expr, else_expr) => IfThenElse(
            convert_expr(cond, registry)?,
            convert_expr(then_expr, registry)?,
            convert_expr(else_expr, registry)?,
        ),
        NewScope(expr) => NewScope(convert_expr(expr, registry)?),
        Let(binding, expr) => Let(
            LetBinding::new(binding.name.clone(), convert_expr(&binding.expr, registry)?),
            convert_expr(expr, registry)?,
        ),
        Binary(left, op, right) => Binary(
            convert_expr(left, registry)?,
            op.clone(),
            convert_expr(right, registry)?,
        ),
        Unary(op, expr) => Unary(op.clone(), convert_expr(expr, registry)?),
        Call(func, args) => {
            let converted_func = convert_expr(func, registry)?;
            let converted_args = args
                .iter()
                .map(|arg| convert_expr(arg, registry))
                .collect::<Result<Vec<_>, _>>()?;

            Call(converted_func, converted_args)
        }
        Map(entries) => {
            let converted_entries = entries
                .iter()
                .map(|(key, value)| {
                    Ok((convert_expr(key, registry)?, convert_expr(value, registry)?))
                })
                .collect::<Result<Vec<_>, Box<_>>>()?;

            Map(converted_entries)
        }
        Ref(ident) => Ref(ident.clone()),
        Return(expr) => Return(convert_expr(expr, registry)?),
        FieldAccess(_, _) => {
            // TODO: Change here.
            todo!()
        }
        CoreExpr(core_data) => CoreExpr(convert_core_data_expr(core_data, registry)?),
        CoreVal(_) => {
            // TODO(#80): Nothing to do here as only one type of CoreVal is created.
            // 1. External closures (i.e. declared with fn), but those are already handled in
            //    convert.
            // Note: It makes no sense that functions can be expressions. They should only be
            // values. Once we change that, we just move the function core_expr code.
            panic!("CoreVal should not be in HIR<TypedSpan>")
        }
    };

    Ok(Expr::new(converted_kind).into())
}

fn convert_core_data_expr(
    core: &CoreData<Arc<Expr<TypedSpan>>, TypedSpan>,
    registry: &TypeRegistry,
) -> Result<CoreData<Arc<Expr>>, Box<AnalyzerErrorKind>> {
    use CoreData::*;
    use FunKind::*;

    let result = match core {
        Literal(lit) => Literal(lit.clone()),
        Array(elements) => {
            let converted_elements = elements
                .iter()
                .map(|expr| convert_expr(expr, registry))
                .collect::<Result<Vec<_>, _>>()?;

            Array(converted_elements)
        }
        Tuple(elements) => {
            let converted_elements = elements
                .iter()
                .map(|expr| convert_expr(expr, registry))
                .collect::<Result<Vec<_>, _>>()?;

            Tuple(converted_elements)
        }
        Struct(name, fields) => {
            // TODO: Change here.
            let converted_fields = fields
                .iter()
                .map(|field| convert_expr(field, registry))
                .collect::<Result<Vec<_>, _>>()?;
            Struct(name.clone(), converted_fields)
        }
        Map(_) | Logical(_) | Physical(_) => {
            panic!("Types may not be in the HIR yet")
        }
        Function(Udf(_)) => panic!("UDFs may not appear within functions"),
        Function(Closure(params, body)) => {
            Function(Closure(params.to_vec(), convert_expr(body, registry)?))
        }
        Fail(expr) => Fail(convert_expr(expr, registry)?.into()),
        None => None,
    };

    Ok(result)
}

fn convert_pattern(
    pattern: &Pattern<TypedSpan>,
    registry: &TypeRegistry,
) -> Result<Pattern, Box<AnalyzerErrorKind>> {
    use PatternKind::*;

    let converted_kind = match &pattern.kind {
        Bind(ident, pattern) => Bind(ident.clone(), convert_pattern(pattern, registry)?.into()),
        Literal(lit) => Literal(lit.clone()),
        Struct(name, patterns) => {
            // TODO: Change here.
            let converted_patterns = patterns
                .iter()
                .map(|pattern| convert_pattern(pattern, registry))
                .collect::<Result<Vec<_>, _>>()?;

            Struct(name.clone(), converted_patterns)
        }
        Operator(_) => {
            panic!("Operator patterns are not supported in HIR<TypedSpan>");
        }
        Wildcard => Wildcard,
        EmptyArray => EmptyArray,
        ArrayDecomp(head, tail) => ArrayDecomp(
            convert_pattern(head, registry)?.into(),
            convert_pattern(tail, registry)?.into(),
        ),
    };

    Ok(Pattern::new(converted_kind))
}
