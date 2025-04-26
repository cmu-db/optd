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
pub fn transform_struct_to_operator(
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
pub fn transform_pattern_to_operator(
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
