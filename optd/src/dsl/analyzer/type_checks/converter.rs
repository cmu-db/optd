use super::registry::{Type, TypeKind};
use crate::dsl::parser::ast::Type as AstType;
use crate::dsl::utils::span::{OptionalSpanned, Spanned};
use std::collections::HashMap;

/// Converts an AST type to a HIR Type.
///
/// This function transforms AST type representations into their HIR counterparts,
/// preserving span information for better error reporting and diagnostics.
///
/// # Arguments
///
/// * `ast_ty` - The AST type to convert, wrapped with span information
///
/// # Returns
///
/// A HIR `Type` that corresponds to the provided AST type
pub(crate) fn convert_ast_type(ast_ty: Spanned<AstType>) -> Type {
    use TypeKind::*;

    let span = ast_ty.span;
    let kind = match *ast_ty.value {
        AstType::Identifier(name) => Adt(name),
        AstType::Int64 => I64,
        AstType::String => String,
        AstType::Bool => Bool,
        AstType::Unit => Unit,
        AstType::Float64 => F64,
        AstType::Array(inner) => Array(convert_ast_type(inner)),
        AstType::Closure(params, ret) => Closure(convert_ast_type(params), convert_ast_type(ret)),
        AstType::Tuple(inner) => {
            let inner_types = inner.into_iter().map(convert_ast_type).collect();
            Tuple(inner_types)
        }
        AstType::Map(key, val) => Map(convert_ast_type(key), convert_ast_type(val)),
        AstType::Questioned(inner) => Optional(convert_ast_type(inner)),
        _ => panic!("Registry has not been properly validated"),
    };

    OptionalSpanned::spanned(kind, span)
}

/// Creates a function type from parameter types and return type.
///
/// This function creates a proper closure type representation with appropriate
/// parameter grouping:
/// - No parameters: `() -> ReturnType`
/// - Single parameter: `ParamType -> ReturnType`
/// - Multiple parameters: `(Param1, Param2, ...) -> ReturnType`
///
/// # Arguments
///
/// * `param_types` - Slice of parameter types
/// * `return_type` - The return type of the function
///
/// # Returns
///
/// A `Type` representing the complete function signature
pub(crate) fn create_function_type(param_types: &[Type], return_type: &Type) -> Type {
    use TypeKind::*;

    let param_type = if param_types.is_empty() {
        Unit.into()
    } else if param_types.len() == 1 {
        param_types[0].clone()
    } else {
        Tuple(param_types.to_vec()).into()
    };

    Closure(param_type, return_type.clone()).into()
}

/// Converts a type to its string representation, resolving unknown types when possible.
///
/// This function formats a type as a string, including proper handling of unknown types.
/// When an unknown type is encountered, it will be resolved using the provided map.
/// If the unknown type has been resolved, it will be displayed with appropriate notation
/// to indicate its constraint type (ascending/descending).
///
/// # Arguments
///
/// * `ty` - The type to convert to a string
/// * `resolved_unknown` - Map of unknown type IDs to their resolved concrete types
///
/// # Returns
///
/// A string representation of the type
pub(crate) fn type_display(ty: &Type, resolved_unknown: &HashMap<usize, Type>) -> String {
    use TypeKind::*;

    match &*ty.value {
        // Primitive types
        I64 => "I64".to_string(),
        String => "String".to_string(),
        Bool => "Bool".to_string(),
        F64 => "F64".to_string(),

        // Special types
        Unit => "()".to_string(),
        Universe => "Universe".to_string(),
        Nothing => "Nothing".to_string(),
        None => "None".to_string(),

        // Unknown types
        UnknownAsc(id) => {
            format!(
                "≧{{{}}}",
                type_display(resolved_unknown.get(id).unwrap(), resolved_unknown)
            )
        }
        UnknownDesc(id) => {
            format!(
                "≦{{{}}}",
                type_display(resolved_unknown.get(id).unwrap(), resolved_unknown)
            )
        }

        // User types
        Adt(name) => name.to_string(),
        Generic(name) => format!("Gen<#{}>", name),

        // Composite types
        Array(elem) => format!("[{}]", type_display(elem, resolved_unknown)),
        Closure(param, ret) => format!(
            "{} -> {}",
            type_display(param, resolved_unknown),
            type_display(ret, resolved_unknown)
        ),
        Tuple(elems) => {
            if elems.is_empty() {
                "()".to_string()
            } else {
                let elem_strs: Vec<_> = elems
                    .iter()
                    .map(|elem| type_display(elem, resolved_unknown))
                    .collect();
                format!("({})", elem_strs.join(", "))
            }
        }
        Map(key, val) => format!(
            "{{{}:{}}}",
            type_display(key, resolved_unknown),
            type_display(val, resolved_unknown)
        ),
        Optional(inner) => format!("{}?", type_display(inner, resolved_unknown)),

        // Memo status types
        Stored(inner) => format!("{}*", type_display(inner, resolved_unknown)),
        Costed(inner) => format!("{}$", type_display(inner, resolved_unknown)),

        // Native trait types
        Concat => "Concat".to_string(),
        EqHash => "EqHash".to_string(),
        Arithmetic => "Arithmetic".to_string(),
    }
}

#[cfg(test)]
mod type_converter_tests {
    use super::create_function_type;
    use crate::dsl::analyzer::type_checks::registry::{Type, TypeKind};

    #[test]
    fn test_create_function_type() {
        // Test with no parameters
        let params: Vec<Type> = vec![];
        let return_type = TypeKind::I64.into();
        let result = create_function_type(&params, &return_type);
        match &*result.value {
            TypeKind::Closure(param, ret) => {
                assert_eq!(*param.value, TypeKind::Unit);
                assert_eq!(*ret.value, TypeKind::I64);
            }
            _ => panic!("Expected Closure type"),
        }

        // Test with one parameter
        let params = vec![TypeKind::I64.into()];
        let result = create_function_type(&params, &return_type);
        match &*result.value {
            TypeKind::Closure(param, ret) => {
                assert_eq!(*param.value, TypeKind::I64);
                assert_eq!(*ret.value, TypeKind::I64);
            }
            _ => panic!("Expected Closure type"),
        }

        // Test with multiple parameters
        let params = vec![TypeKind::I64.into(), TypeKind::Bool.into()];
        let result = create_function_type(&params, &return_type);
        match &*result.value {
            TypeKind::Closure(param, ret) => {
                match &*param.value {
                    TypeKind::Tuple(types) => {
                        assert_eq!(types.len(), 2);
                        assert_eq!(*types[0].value, TypeKind::I64);
                        assert_eq!(*types[1].value, TypeKind::Bool);
                    }
                    _ => panic!("Expected Tuple type for parameters"),
                }
                assert_eq!(*ret.value, TypeKind::I64);
            }
            _ => panic!("Expected Closure type"),
        }
    }
}
