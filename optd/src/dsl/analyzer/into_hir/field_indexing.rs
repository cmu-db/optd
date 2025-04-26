use crate::dsl::analyzer::type_checks::{converter::convert_ast_type, registry::TypeRegistry};

/// Finds the index of a field in a struct, accounting for special indexing of logical/physical operators.
///
/// For normal structs, returns the direct field position.
/// For logical/physical operators, returns the position after grouping data fields before children fields.
///
/// # Arguments
/// * `struct_name` - The name of the struct
/// * `field_name` - The name of the field to find
/// * `registry` - The type registry
///
/// # Returns
/// The index position of the field
///
/// # Panics
/// Panics if the struct or field is not found
pub(super) fn find_field_index(
    struct_name: &str,
    field_name: &str,
    registry: &TypeRegistry,
) -> i64 {
    let fields = match registry.product_fields.get(struct_name) {
        Some(fields) => fields,
        None => panic!("Struct {} not found in registry", struct_name),
    };

    if registry.is_logical_or_physical(struct_name) {
        let mut data_count = 0;

        // First pass: count data fields.
        for field in fields {
            let field_type = convert_ast_type(field.ty.clone());
            if !registry.is_child_type(&field_type) {
                data_count += 1;
            }
        }

        // Second pass: find the field index.
        let mut current_data_index = 0;
        let mut current_child_index = 0;

        for field in fields {
            if *field.name.value == field_name {
                let field_type = convert_ast_type(field.ty.clone());
                if registry.is_child_type(&field_type) {
                    // Child field: position after all data fields.
                    return data_count + current_child_index;
                } else {
                    // Data field: normal position.
                    return current_data_index;
                }
            }

            let field_type = convert_ast_type(field.ty.clone());
            if registry.is_child_type(&field_type) {
                current_child_index += 1;
            } else {
                current_data_index += 1;
            }
        }
    } else {
        // Normal struct: field index is just its position.
        for (index, field) in fields.iter().enumerate() {
            if *field.name.value == field_name {
                return index as i64;
            }
        }
    }

    panic!("Field {} not found in struct {}", field_name, struct_name);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::analyzer::type_checks::registry::type_registry_tests::{
        create_product_adt, create_sum_adt,
    };
    use crate::dsl::analyzer::type_checks::registry::{LOGICAL_TYPE, PHYSICAL_TYPE};
    use crate::dsl::parser::ast::Type as AstType;

    fn create_registry_with_operators() -> TypeRegistry {
        let mut registry = TypeRegistry::new();

        // Create a LogicalJoin with mixed fields
        let logical_join = create_product_adt(
            "LogicalJoin",
            vec![
                ("join_type", AstType::String),
                ("condition", AstType::String),
                ("left", AstType::Identifier(LOGICAL_TYPE.to_string())),
                ("right", AstType::Identifier(LOGICAL_TYPE.to_string())),
            ],
        );

        // Create a PhysicalFilter with mixed fields
        let physical_filter = create_product_adt(
            "PhysicalFilter",
            vec![
                ("predicate", AstType::String),
                ("selectivity", AstType::Float64),
                ("child", AstType::Identifier(PHYSICAL_TYPE.to_string())),
            ],
        );

        // Create Logical and Physical enums
        let logical_enum = create_sum_adt(LOGICAL_TYPE, vec![logical_join]);
        let physical_enum = create_sum_adt(PHYSICAL_TYPE, vec![physical_filter]);

        registry.register_adt(&logical_enum).unwrap();
        registry.register_adt(&physical_enum).unwrap();
        registry
    }

    #[test]
    fn test_field_index_normal_struct() {
        let mut registry = TypeRegistry::new();
        let point = create_product_adt(
            "Point",
            vec![
                ("x", AstType::Int64),
                ("y", AstType::Int64),
                ("z", AstType::Int64),
            ],
        );
        registry.register_adt(&point).unwrap();

        assert_eq!(find_field_index("Point", "x", &registry), 0);
        assert_eq!(find_field_index("Point", "y", &registry), 1);
        assert_eq!(find_field_index("Point", "z", &registry), 2);
    }

    #[test]
    fn test_field_index_logical_operator() {
        let registry = create_registry_with_operators();

        // Data fields come first
        assert_eq!(find_field_index("LogicalJoin", "join_type", &registry), 0);
        assert_eq!(find_field_index("LogicalJoin", "condition", &registry), 1);

        // Children fields come after data fields
        assert_eq!(find_field_index("LogicalJoin", "left", &registry), 2);
        assert_eq!(find_field_index("LogicalJoin", "right", &registry), 3);
    }

    #[test]
    fn test_field_index_physical_operator() {
        let registry = create_registry_with_operators();

        // Data fields come first
        assert_eq!(
            find_field_index("PhysicalFilter", "predicate", &registry),
            0
        );
        assert_eq!(
            find_field_index("PhysicalFilter", "selectivity", &registry),
            1
        );

        // Child field comes after data fields
        assert_eq!(find_field_index("PhysicalFilter", "child", &registry), 2);
    }
}
