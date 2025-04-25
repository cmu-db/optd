use crate::dsl::analyzer::{
    hir::{
        self, CoreData, Expr, ExprKind, FunKind, LetBinding, LogicalOp, MatchArm, Materializable,
        Operator, Pattern, PatternKind, PhysicalOp, TypedSpan,
    },
    type_checks::registry::{LOGICAL_TYPE, PHYSICAL_TYPE, Type, TypeKind, TypeRegistry},
};
use std::sync::Arc;

pub(super) fn convert_expr(expr: &Arc<Expr<TypedSpan>>, registry: &TypeRegistry) -> Arc<Expr> {
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
        FieldAccess(_, _) => {
            // TODO: Change here.
            todo!()
        }
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
            if is_logical_or_physical(registry, name) {
                let (children, data) = transform_expr_to_operator(name, fields, registry);
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

fn convert_pattern(pattern: &Pattern<TypedSpan>, registry: &TypeRegistry) -> Pattern {
    use PatternKind::*;

    let converted_kind = match &pattern.kind {
        Bind(ident, pattern) => Bind(ident.clone(), convert_pattern(pattern, registry).into()),
        Literal(lit) => Literal(lit.clone()),
        Struct(name, patterns) => {
            if is_logical_or_physical(registry, name) {
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

// Separate fields into children (logical/physical) and data (others).
fn transform_expr_to_operator(
    struct_name: &str,
    fields: &[Arc<Expr<TypedSpan>>],
    registry: &TypeRegistry,
) -> (Vec<Arc<Expr>>, Vec<Arc<Expr>>) {
    let mut children = Vec::new();
    let mut data = Vec::new();

    for (index, field) in fields.iter().enumerate() {
        let field_type = registry
            .get_product_field_type_by_index(struct_name, index)
            .unwrap();

        let is_child = is_child_type(&field_type, registry);

        if is_child {
            children.push(convert_expr(field, registry));
        } else {
            data.push(convert_expr(field, registry));
        }
    }

    (children, data)
}

// Pattern version.
fn transform_pattern_to_operator(
    struct_name: &str,
    patterns: &[Pattern<TypedSpan>],
    registry: &TypeRegistry,
) -> (Vec<Pattern>, Vec<Pattern>) {
    let mut children = Vec::new();
    let mut data = Vec::new();

    for (index, pattern) in patterns.iter().enumerate() {
        let field_type = registry
            .get_product_field_type_by_index(struct_name, index)
            .unwrap();

        let is_child = is_child_type(&field_type, registry);

        if is_child {
            children.push(convert_pattern(pattern, registry));
        } else {
            data.push(convert_pattern(pattern, registry));
        }
    }

    (children, data)
}

// Helper function to check if a type should be a child
fn is_child_type(ty: &Type, registry: &TypeRegistry) -> bool {
    use TypeKind::*;

    match &*ty.value {
        Adt(name) => is_logical_or_physical(registry, name),
        Array(element_type) => match &*element_type.value {
            Adt(name) => is_logical_or_physical(registry, name),
            _ => false,
        },
        _ => false,
    }
}

// Helper function to check if an ADT is logical or physical
fn is_logical_or_physical(registry: &TypeRegistry, name: &str) -> bool {
    registry.inherits_adt(name, LOGICAL_TYPE) || registry.inherits_adt(name, PHYSICAL_TYPE)
}
