use super::field_indexing::find_field_index;
use super::operators::{transform_pattern_to_operator, transform_struct_to_operator};
use crate::dsl::analyzer::hir::{Literal, LogicalOp, Materializable, Operator, PhysicalOp};
use crate::dsl::analyzer::type_checks::registry::{LOGICAL_TYPE, TypeKind};
use crate::dsl::analyzer::{
    hir::{
        self, CoreData, Expr, ExprKind, FunKind, LetBinding, MatchArm, Pattern, PatternKind,
        TypedSpan,
    },
    type_checks::registry::TypeRegistry,
};
use std::sync::Arc;

pub fn convert_expr(expr: &Arc<Expr<TypedSpan>>, registry: &TypeRegistry) -> Arc<Expr> {
    use ExprKind::*;

    let converted_kind = match &expr.kind {
        PatternMatch(expr, arms) => {
            let converted_arms = arms
                .iter()
                .map(|arm| MatchArm {
                    pattern: convert_pattern(&arm.pattern, registry),
                    expr: convert_expr(&arm.expr, registry),
                })
                .collect();

            PatternMatch(convert_expr(expr, registry), converted_arms)
        }
        IfThenElse(cond, then_expr, else_expr) => IfThenElse(
            convert_expr(cond, registry),
            convert_expr(then_expr, registry),
            convert_expr(else_expr, registry),
        ),
        NewScope(expr) => NewScope(convert_expr(expr, registry)),
        Let(binding, expr) => Let(
            LetBinding::new(binding.name.clone(), convert_expr(&binding.expr, registry)),
            convert_expr(expr, registry),
        ),
        Binary(left, op, right) => Binary(
            convert_expr(left, registry),
            op.clone(),
            convert_expr(right, registry),
        ),
        Unary(op, expr) => Unary(op.clone(), convert_expr(expr, registry)),
        Call(func, args) => {
            let converted_func = convert_expr(func, registry);
            let converted_args = args.iter().map(|arg| convert_expr(arg, registry)).collect();

            Call(converted_func, converted_args)
        }
        Map(entries) => {
            let converted_entries = entries
                .iter()
                .map(|(key, value)| (convert_expr(key, registry), convert_expr(value, registry)))
                .collect();

            Map(converted_entries)
        }
        Ref(ident) => Ref(ident.clone()),
        Return(expr) => Return(convert_expr(expr, registry)),
        FieldAccess(expr, field_name) => convert_field_access(expr, field_name, registry),
        CoreExpr(core_data) => CoreExpr(convert_core_data_expr(core_data, registry)),
        CoreVal(_) => {
            // TODO(#80): Nothing to do here as only one type of CoreVal is created.
            // 1. External closures (i.e. declared with fn), but those are already handled in
            //    convert.
            // Note: It makes no sense that functions can be expressions. They should only be
            // values. Once we change that, we just move the function core_expr code.
            panic!("CoreVal should not be in HIR<TypedSpan>")
        }
    };

    Expr::new(converted_kind).into()
}

pub fn convert_pattern(pattern: &Pattern<TypedSpan>, registry: &TypeRegistry) -> Pattern {
    use PatternKind::*;

    let converted_kind = match &pattern.kind {
        Bind(ident, pattern) => Bind(ident.clone(), convert_pattern(pattern, registry).into()),
        Literal(lit) => Literal(lit.clone()),
        Struct(name, patterns) => {
            if registry.is_logical_or_physical(name) {
                let (children, data) = transform_pattern_to_operator(name, patterns, registry);

                Operator(hir::Operator {
                    tag: name.clone(),
                    data,
                    children,
                })
            } else {
                let converted_patterns = patterns
                    .iter()
                    .map(|pattern| convert_pattern(pattern, registry))
                    .collect();

                Struct(name.clone(), converted_patterns)
            }
        }
        Operator(_) => {
            panic!("Operator patterns are not supported in HIR<TypedSpan>");
        }
        Wildcard => Wildcard,
        EmptyArray => EmptyArray,
        ArrayDecomp(head, tail) => ArrayDecomp(
            convert_pattern(head, registry).into(),
            convert_pattern(tail, registry).into(),
        ),
    };

    Pattern::new(converted_kind)
}

fn convert_core_data_expr(
    core: &CoreData<Arc<Expr<TypedSpan>>, TypedSpan>,
    registry: &TypeRegistry,
) -> CoreData<Arc<Expr>> {
    use CoreData::*;
    use FunKind::*;
    use Materializable::*;

    match core {
        Literal(lit) => Literal(lit.clone()),
        Array(elements) => {
            let converted_elements = elements
                .iter()
                .map(|expr| convert_expr(expr, registry))
                .collect();

            Array(converted_elements)
        }
        Tuple(elements) => {
            let converted_elements = elements
                .iter()
                .map(|expr| convert_expr(expr, registry))
                .collect();

            Tuple(converted_elements)
        }
        Struct(name, fields) => {
            if registry.is_logical_or_physical(name) {
                let (children, data) = transform_struct_to_operator(name, fields, registry);
                let operator = Operator {
                    tag: name.clone(),
                    data,
                    children,
                };

                if registry.inherits_adt(name, LOGICAL_TYPE) {
                    Logical(Materialized(LogicalOp::logical(operator)))
                } else {
                    Physical(Materialized(PhysicalOp::physical(operator)))
                }
            } else {
                let converted_fields = fields
                    .iter()
                    .map(|field| convert_expr(field, registry))
                    .collect();

                Struct(name.clone(), converted_fields)
            }
        }
        Map(_) | Logical(_) | Physical(_) => {
            panic!("Types may not be in the HIR yet")
        }
        Function(Udf(_)) => panic!("UDFs may not appear within functions"),
        Function(Closure(params, body)) => {
            Function(Closure(params.to_vec(), convert_expr(body, registry)))
        }
        Fail(expr) => Fail(convert_expr(expr, registry).into()),
        None => None,
    }
}

fn convert_field_access(
    expr: &Arc<Expr<TypedSpan>>,
    field_name: &str,
    registry: &TypeRegistry,
) -> ExprKind {
    use ExprKind::*;

    let expr_type = registry.resolve_type(&expr.metadata.ty);
    let struct_name = match &*expr_type.value {
        TypeKind::Adt(name) => name,
        _ => panic!("Field access on non-struct type: error in type inference"),
    };
    let field_index = find_field_index(struct_name, field_name, registry);

    Call(
        convert_expr(expr, registry),
        vec![Expr::new(CoreExpr(CoreData::Literal(Literal::Int64(field_index)))).into()],
    )
}
