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
