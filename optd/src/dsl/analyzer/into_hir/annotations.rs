use crate::dsl::analyzer::errors::AnalyzerErrorKind;
use crate::dsl::analyzer::type_checks::registry::{
    LOGICAL_TYPE, PHYSICAL_PROPS, PHYSICAL_TYPE, Type, TypeKind, TypeRegistry,
};
use crate::dsl::utils::span::Span;
use once_cell::sync::Lazy;

pub static TRANSFORMATION_SIGNATURE_TYPE: Lazy<Type> = Lazy::new(|| {
    use TypeKind::*;
    let param_type = Stored(Adt(LOGICAL_TYPE.to_string()).into()).into();
    let return_type = Optional(Adt(LOGICAL_TYPE.to_string()).into()).into();
    Closure(param_type, return_type).into()
});

pub const TRANSFORMATION_ANNOTATION: &str = "transformation";

pub static IMPLEMENTATION_SIGNATURE_TYPE: Lazy<Type> = Lazy::new(|| {
    use TypeKind::*;
    let params_type = Tuple(vec![
        Stored(Adt(LOGICAL_TYPE.to_string()).into()).into(),
        Optional(Adt(PHYSICAL_PROPS.to_string()).into()).into(),
    ])
    .into();
    let return_type = Optional(Adt(PHYSICAL_TYPE.to_string()).into()).into();
    Closure(params_type, return_type).into()
});

pub const IMPLEMENTATION_ANNOTATION: &str = "implementation";

/// Validates that a function's type matches the expected signature for a given annotation
///
/// # Arguments
///
/// * `annotation` - The annotation to validate
/// * `function_type` - The actual type of the function
/// * `function_span` - The span where the function is defined
/// * `registry` - The type registry for type checking
///
/// # Returns
///
/// `Ok(())` if the function type is valid for the annotation, or an error if validation fails
pub(super) fn validate_annotation(
    annotation: &str,
    function_type: &Type,
    function_span: &Span,
    registry: &mut TypeRegistry,
) -> Result<(), Box<AnalyzerErrorKind>> {
    match annotation {
        TRANSFORMATION_ANNOTATION => {
            if !registry.is_subtype(function_type, &TRANSFORMATION_SIGNATURE_TYPE) {
                return Err(AnalyzerErrorKind::new_invalid_transformation(
                    function_span,
                    function_type,
                    registry.resolved_unknown.clone(),
                ));
            }
        }
        IMPLEMENTATION_ANNOTATION => {
            if !registry.is_subtype(function_type, &IMPLEMENTATION_SIGNATURE_TYPE) {
                return Err(AnalyzerErrorKind::new_invalid_implementation(
                    function_span,
                    function_type,
                    registry.resolved_unknown.clone(),
                ));
            }
        }
        _ => {
            // Unknown annotations are ignored for now.
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{
        analyzer::type_checks::registry::{
            TypeRegistry,
            type_registry_tests::{create_product_adt, create_sum_adt, create_test_span},
        },
        parser::ast::Type as AstType,
    };

    #[test]
    fn test_validate_transformation_annotation_with_optional_return() {
        let mut registry = TypeRegistry::new();

        // Create a transformation function with optional return type
        let function_type = TypeKind::Closure(
            TypeKind::Stored(TypeKind::Adt(LOGICAL_TYPE.to_string()).into()).into(),
            TypeKind::Optional(TypeKind::Adt(LOGICAL_TYPE.to_string()).into()).into(),
        )
        .into();

        let result = validate_annotation(
            TRANSFORMATION_ANNOTATION,
            &function_type,
            &create_test_span(),
            &mut registry,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_transformation_annotation_with_specialized_return() {
        let mut registry = TypeRegistry::new();

        // Create LogicalScan as a subtype of Logical
        let logical_scan = create_product_adt("LogicalScan", vec![("table", AstType::String)]);
        let logical_enum = create_sum_adt(LOGICAL_TYPE, vec![logical_scan]);
        registry.register_adt(&logical_enum).unwrap();

        // Create a transformation function that returns LogicalScan (subtype of Logical)
        let function_type = TypeKind::Closure(
            TypeKind::Stored(TypeKind::Adt(LOGICAL_TYPE.to_string()).into()).into(),
            TypeKind::Adt("LogicalScan".to_string()).into(),
        )
        .into();

        let result = validate_annotation(
            TRANSFORMATION_ANNOTATION,
            &function_type,
            &create_test_span(),
            &mut registry,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_transformation_annotation_failure() {
        let mut registry = TypeRegistry::new();

        // Create an invalid function type (wrong parameter type)
        let function_type = TypeKind::Closure(
            TypeKind::I64.into(),
            TypeKind::Adt(LOGICAL_TYPE.to_string()).into(),
        )
        .into();

        let result = validate_annotation(
            TRANSFORMATION_ANNOTATION,
            &function_type,
            &create_test_span(),
            &mut registry,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_implementation_annotation_with_optional_return() {
        let mut registry = TypeRegistry::new();

        // Create an implementation function with optional return type
        let function_type = TypeKind::Closure(
            TypeKind::Tuple(vec![
                TypeKind::Stored(TypeKind::Adt(LOGICAL_TYPE.to_string()).into()).into(),
                TypeKind::Optional(TypeKind::Adt(PHYSICAL_PROPS.to_string()).into()).into(),
            ])
            .into(),
            TypeKind::Optional(TypeKind::Adt(PHYSICAL_TYPE.to_string()).into()).into(),
        )
        .into();

        let result = validate_annotation(
            IMPLEMENTATION_ANNOTATION,
            &function_type,
            &create_test_span(),
            &mut registry,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_implementation_annotation_with_specialized_return() {
        let mut registry = TypeRegistry::new();

        // Create PhysicalHashJoin as a subtype of Physical
        let physical_hash_join = create_product_adt(
            "PhysicalHashJoin",
            vec![
                ("algorithm", AstType::String),
                ("left", AstType::Identifier(PHYSICAL_TYPE.to_string())),
                ("right", AstType::Identifier(PHYSICAL_TYPE.to_string())),
            ],
        );
        let physical_enum = create_sum_adt(PHYSICAL_TYPE, vec![physical_hash_join]);
        registry.register_adt(&physical_enum).unwrap();

        // Create PhysicalProperties
        let physical_props = create_product_adt(PHYSICAL_PROPS, vec![("cost", AstType::Int64)]);
        registry.register_adt(&physical_props).unwrap();

        // Create an implementation function that returns PhysicalHashJoin (subtype of Physical)
        let function_type = TypeKind::Closure(
            TypeKind::Tuple(vec![
                TypeKind::Stored(TypeKind::Adt(LOGICAL_TYPE.to_string()).into()).into(),
                TypeKind::Optional(TypeKind::Adt(PHYSICAL_PROPS.to_string()).into()).into(),
            ])
            .into(),
            TypeKind::Adt("PhysicalHashJoin".to_string()).into(),
        )
        .into();

        let result = validate_annotation(
            IMPLEMENTATION_ANNOTATION,
            &function_type,
            &create_test_span(),
            &mut registry,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_implementation_annotation_failure_wrong_params() {
        let mut registry = TypeRegistry::new();

        // Create an invalid function type (wrong parameter types)
        let function_type = TypeKind::Closure(
            TypeKind::I64.into(), // Wrong parameter type
            TypeKind::Adt(PHYSICAL_TYPE.to_string()).into(),
        )
        .into();

        let result = validate_annotation(
            IMPLEMENTATION_ANNOTATION,
            &function_type,
            &create_test_span(),
            &mut registry,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_implementation_annotation_failure_wrong_return() {
        let mut registry = TypeRegistry::new();

        // Create an invalid function type (wrong return type)
        let function_type = TypeKind::Closure(
            TypeKind::Tuple(vec![
                TypeKind::Stored(TypeKind::Adt(LOGICAL_TYPE.to_string()).into()).into(),
                TypeKind::Adt(PHYSICAL_PROPS.to_string()).into(),
            ])
            .into(),
            TypeKind::I64.into(), // Wrong return type
        )
        .into();

        let result = validate_annotation(
            IMPLEMENTATION_ANNOTATION,
            &function_type,
            &create_test_span(),
            &mut registry,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_unknown_annotation() {
        let mut registry = TypeRegistry::new();
        let function_type = TypeKind::I64.into();

        // Unknown annotations should always be valid
        let result = validate_annotation(
            "unknown_annotation",
            &function_type,
            &create_test_span(),
            &mut registry,
        );
        assert!(result.is_ok());
    }
}
