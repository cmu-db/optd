use super::converter::{convert_expr, convert_pattern};
use crate::dsl::analyzer::{
    hir::{Expr, Pattern, TypedSpan},
    type_checks::registry::TypeRegistry,
};
use std::sync::Arc;

/// Transforms a struct expression into an operator by separating fields into children and data
///
/// # Arguments
/// * `struct_name` - The name of the struct being transformed
/// * `fields` - The fields of the struct
/// * `registry` - The type registry for type lookups
///
/// # Returns
/// A tuple containing (children, data) where children are logical/physical fields and data are regular fields
pub(super) fn transform_struct_to_operator(
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

        if registry.is_child_type(&field_type) {
            children.push(convert_expr(field, registry));
        } else {
            data.push(convert_expr(field, registry));
        }
    }

    (children, data)
}

/// Transforms a struct pattern into an operator pattern by separating fields into children and data
///
/// # Arguments
/// * `struct_name` - The name of the struct being transformed
/// * `patterns` - The patterns of the struct
/// * `registry` - The type registry for type lookups
///
/// # Returns
/// A tuple containing (children, data) where children are logical/physical patterns and data are regular patterns
pub(super) fn transform_pattern_to_operator(
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

        if registry.is_child_type(&field_type) {
            children.push(convert_pattern(pattern, registry));
        } else {
            data.push(convert_pattern(pattern, registry));
        }
    }

    (children, data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::analyzer::{
        hir::{CoreData, ExprKind, Literal, PatternKind},
        type_checks::{
            converter::convert_ast_type,
            registry::{
                LOGICAL_TYPE, TypeKind,
                type_registry_tests::{
                    create_product_adt, create_sum_adt, create_test_span, spanned,
                },
            },
        },
    };
    use crate::dsl::parser::ast::Type as AstType;

    fn create_registry_with_logical_types() -> TypeRegistry {
        let mut registry = TypeRegistry::new();

        let logical_join = create_product_adt(
            "LogicalJoin",
            vec![
                ("join_type", AstType::String),
                ("condition", AstType::String),
                ("left", AstType::Identifier(LOGICAL_TYPE.to_string())),
                ("right", AstType::Identifier(LOGICAL_TYPE.to_string())),
            ],
        );

        let logical_scan = create_product_adt("LogicalScan", vec![("table", AstType::String)]);

        let logical_enum = create_sum_adt(LOGICAL_TYPE, vec![logical_join, logical_scan]);
        registry.register_adt(&logical_enum).unwrap();
        registry
    }

    #[test]
    fn test_transform_struct_to_operator_with_mixed_fields() {
        let registry = create_registry_with_logical_types();

        let fields = vec![
            // Data field: join_type
            Arc::new(Expr::new_with(
                ExprKind::CoreExpr(CoreData::Literal(Literal::String("inner".to_string()))),
                TypeKind::String.into(),
                create_test_span(),
            )),
            // Data field: condition
            Arc::new(Expr::new_with(
                ExprKind::CoreExpr(CoreData::Literal(Literal::String("a = b".to_string()))),
                TypeKind::String.into(),
                create_test_span(),
            )),
            // Child field: left
            Arc::new(Expr::new_with(
                ExprKind::CoreExpr(CoreData::Struct("LogicalScan".to_string(), vec![])),
                TypeKind::Adt(LOGICAL_TYPE.to_string()).into(),
                create_test_span(),
            )),
            // Child field: right
            Arc::new(Expr::new_with(
                ExprKind::CoreExpr(CoreData::Struct("LogicalScan".to_string(), vec![])),
                TypeKind::Adt(LOGICAL_TYPE.to_string()).into(),
                create_test_span(),
            )),
        ];

        let (children, data) = transform_struct_to_operator("LogicalJoin", &fields, &registry);

        assert_eq!(data.len(), 2);
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn test_transform_pattern_to_operator() {
        let registry = create_registry_with_logical_types();

        let patterns = vec![
            // Data pattern: join_type
            Pattern::new_with(
                PatternKind::Wildcard,
                TypeKind::String.into(),
                create_test_span(),
            ),
            // Data pattern: condition
            Pattern::new_with(
                PatternKind::Wildcard,
                TypeKind::String.into(),
                create_test_span(),
            ),
            // Child pattern: left
            Pattern::new_with(
                PatternKind::Wildcard,
                TypeKind::Adt(LOGICAL_TYPE.to_string()).into(),
                create_test_span(),
            ),
            // Child pattern: right
            Pattern::new_with(
                PatternKind::Wildcard,
                TypeKind::Adt(LOGICAL_TYPE.to_string()).into(),
                create_test_span(),
            ),
        ];

        let (children, data) = transform_pattern_to_operator("LogicalJoin", &patterns, &registry);

        assert_eq!(data.len(), 2);
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn test_array_of_logical_as_children() {
        let registry = create_registry_with_logical_types();

        let array_of_logical = convert_ast_type(spanned(AstType::Array(spanned(
            AstType::Identifier(LOGICAL_TYPE.to_string()),
        ))));

        assert!(registry.is_child_type(&array_of_logical));
    }
}
