//! Type conversion from AST to HIR
//!
//! This module contains functions for converting AST type nodes to their
//! corresponding HIR type representations.

use super::ASTConverter;
use crate::analyzer::errors::AnalyzerErrorKind;
use crate::analyzer::hir::Identifier;
use crate::analyzer::types::Type;
use crate::parser::ast::Type as AstType;
use crate::utils::span::Spanned;
use std::collections::HashSet;

impl ASTConverter {
    /// Converts an AST type to an HIR type.
    ///
    /// This function maps the types from the Abstract Syntax Tree (AST) to their
    /// corresponding High-level Intermediate Representation (HIR) types, handling
    /// primitive types, complex container types, and user-defined types.
    ///
    /// # Arguments
    ///
    /// * `ast_type` - The AST type to convert
    /// * `generics` - A set of scoped generic type identifiers that are currently in scope.
    ///   When encountering an identifier, it first checks if it's in this set
    ///   to determine whether it should be treated as a generic type parameter
    ///   (`Type::Generic`) or as a user-defined ADT type (`Type::Adt`).
    ///
    /// # Returns
    ///
    /// The equivalent HIR type, or an AnalyzerErrorKind if the conversion fails.
    pub(super) fn convert_type(
        &mut self,
        ast_type: &Spanned<AstType>,
        generics: &HashSet<Identifier>,
    ) -> Result<Type, Box<AnalyzerErrorKind>> {
        use Type::*;

        let hir_type = match &*ast_type.value {
            AstType::Int64 => I64,
            AstType::String => String,
            AstType::Bool => Bool,
            AstType::Float64 => F64,
            AstType::Unit => Unit,
            AstType::Array(elem_type) => Array(self.convert_type(elem_type, generics)?.into()),
            AstType::Closure(param_type, return_type) => Closure(
                self.convert_type(param_type, generics)?.into(),
                self.convert_type(return_type, generics)?.into(),
            ),
            AstType::Tuple(types) => {
                let mut hir_types = Vec::new();
                for t in types {
                    let converted_type = self.convert_type(t, generics)?;
                    hir_types.push(converted_type);
                }

                Tuple(hir_types)
            }
            AstType::Map(key_type, value_type) => Map(
                self.convert_type(key_type, generics)?.into(),
                self.convert_type(value_type, generics)?.into(),
            ),
            AstType::Questioned(inner_type) => {
                Optional(self.convert_type(inner_type, generics)?.into())
            }
            AstType::Starred(inner_type) => Stored(self.convert_type(inner_type, generics)?.into()),
            AstType::Dollared(inner_type) => {
                Costed(self.convert_type(inner_type, generics)?.into())
            }
            AstType::Identifier(name) => {
                if generics.contains(name) {
                    Generic(name.clone())
                } else {
                    // Check if the type exists in the registry.
                    if !self.type_registry.subtypes.contains_key(name) {
                        return Err(AnalyzerErrorKind::new_undefined_type(name, &ast_type.span));
                    }

                    Adt(name.clone())
                }
            }
            AstType::Error => panic!("AST should no longer contain errors"),
            AstType::Unknown => self.next_unknown(),
        };

        Ok(hir_type)
    }
}

#[cfg(test)]
mod types_tests {
    use super::*;
    use crate::analyzer::types::Type;
    use crate::parser::ast;
    use crate::utils::span::{Span, Spanned};
    use std::collections::HashSet;

    // Helper functions
    fn create_test_span() -> Span {
        Span::new("test".to_string(), 0..1)
    }

    fn spanned<T>(value: T) -> Spanned<T> {
        Spanned::new(value, create_test_span())
    }

    fn create_test_adt(name: &str) -> ast::Adt {
        ast::Adt::Product {
            name: spanned(name.to_string()),
            fields: vec![],
        }
    }

    #[test]
    fn test_convert_primitive_types() {
        // Test each primitive type
        let test_cases = vec![
            (AstType::Int64, Type::I64),
            (AstType::String, Type::String),
            (AstType::Bool, Type::Bool),
            (AstType::Float64, Type::F64),
            (AstType::Unit, Type::Unit),
            (AstType::Unknown, Type::Unknown(0)),
        ];

        let generics = HashSet::new();
        let mut converter = ASTConverter::default();

        for (ast_type, expected_type) in test_cases {
            let result = converter
                .convert_type(&spanned(ast_type), &generics)
                .expect("Primitive type conversion should succeed");
            assert_eq!(result, expected_type);
        }
    }

    #[test]
    fn test_convert_complex_types() {
        let mut converter = ASTConverter::default();
        let generics = HashSet::new();

        // Register "TestType" for testing complex types
        let test_adt = create_test_adt("TestType");
        converter.type_registry.register_adt(&test_adt).unwrap();

        // Test array type
        let array_type = AstType::Array(spanned(AstType::Int64));
        let result = converter
            .convert_type(&spanned(array_type), &generics)
            .expect("Array type conversion should succeed");
        match result {
            Type::Array(elem_type) => assert_eq!(*elem_type, Type::I64),
            _ => panic!("Expected Array type"),
        }

        // Test closure type
        let param_type = spanned(AstType::Int64);
        let return_type = spanned(AstType::Bool);
        let closure_type = AstType::Closure(param_type, return_type);
        let result = converter
            .convert_type(&spanned(closure_type), &generics)
            .expect("Closure type conversion should succeed");
        match result {
            Type::Closure(param, ret) => {
                assert_eq!(*param, Type::I64);
                assert_eq!(*ret, Type::Bool);
            }
            _ => panic!("Expected Closure type"),
        }

        // Test map type with valid identifiers
        let key_type = spanned(AstType::String);
        let value_type = spanned(AstType::Identifier("TestType".to_string()));
        let map_type = AstType::Map(key_type, value_type);
        let result = converter
            .convert_type(&spanned(map_type), &generics)
            .expect("Map type conversion should succeed");
        match result {
            Type::Map(key, value) => {
                assert_eq!(*key, Type::String);
                assert_eq!(*value, Type::Adt("TestType".to_string()));
            }
            _ => panic!("Expected Map type"),
        }
    }

    #[test]
    fn test_convert_special_types() {
        let mut converter = ASTConverter::default();
        let generics = HashSet::new();

        // Register "TestType" for complex types
        let test_adt = create_test_adt("TestType");
        converter.type_registry.register_adt(&test_adt).unwrap();

        // Test questioned type (Optional)
        let inner_type = spanned(AstType::Int64);
        let questioned_type = AstType::Questioned(inner_type);
        let result = converter
            .convert_type(&spanned(questioned_type), &generics)
            .expect("Optional type conversion should succeed");
        match result {
            Type::Optional(inner) => assert_eq!(*inner, Type::I64),
            _ => panic!("Expected Optional type"),
        }

        // Test starred type (Stored)
        let inner_type = spanned(AstType::Int64);
        let starred_type = AstType::Starred(inner_type);
        let result = converter
            .convert_type(&spanned(starred_type), &generics)
            .expect("Stored type conversion should succeed");
        match result {
            Type::Stored(inner) => assert_eq!(*inner, Type::I64),
            _ => panic!("Expected Stored type"),
        }

        // Test dollared type (Costed)
        let inner_type = spanned(AstType::Int64);
        let dollared_type = AstType::Dollared(inner_type);
        let result = converter
            .convert_type(&spanned(dollared_type), &generics)
            .expect("Costed type conversion should succeed");
        match result {
            Type::Costed(inner) => assert_eq!(*inner, Type::I64),
            _ => panic!("Expected Costed type"),
        }

        // Test special types with identifiers
        let inner_type = spanned(AstType::Identifier("TestType".to_string()));
        let starred_type = AstType::Starred(inner_type);
        let result = converter
            .convert_type(&spanned(starred_type), &generics)
            .expect("Stored type with identifier should succeed");
        match result {
            Type::Stored(inner) => assert_eq!(*inner, Type::Adt("TestType".to_string())),
            _ => panic!("Expected Stored type"),
        }
    }

    #[test]
    fn test_convert_identifier_types() {
        // Create a converter with a registered type
        let mut converter = ASTConverter::default();

        // Register "MyType" in the type registry
        let product_adt = create_test_adt("MyType");
        converter.type_registry.register_adt(&product_adt).unwrap();

        // Test regular ADT identifier with registered type
        let adt_type = AstType::Identifier("MyType".to_string());
        let generics = HashSet::new();
        let result = converter
            .convert_type(&spanned(adt_type), &generics)
            .expect("Registered ADT type conversion should succeed");
        match result {
            Type::Adt(name) => assert_eq!(name, "MyType"),
            _ => panic!("Expected Adt type"),
        }

        // Test undefined type identifier - should return an error
        let undefined_type = AstType::Identifier("UndefinedType".to_string());
        let result = converter.convert_type(&spanned(undefined_type), &generics);
        assert!(result.is_err(), "Expected error for undefined type");

        // Test generic identifier
        let generic_type = AstType::Identifier("T".to_string());
        let mut generics = HashSet::new();
        generics.insert("T".to_string());
        let result = converter
            .convert_type(&spanned(generic_type), &generics)
            .expect("Generic type conversion should succeed");
        match result {
            Type::Generic(name) => assert_eq!(name, "T"),
            _ => panic!("Expected Generic type"),
        }
    }

    #[test]
    fn test_convert_type_with_registry_validation() {
        // Test that convert_type properly validates types against the registry
        let mut converter = ASTConverter::default();

        // Register some types
        let types = ["TypeA", "TypeB", "Container"];
        for ty in &types {
            converter
                .type_registry
                .register_adt(&create_test_adt(ty))
                .unwrap();
        }

        let generics = HashSet::new();

        // Valid types should convert successfully
        assert!(
            converter
                .convert_type(
                    &spanned(AstType::Identifier("TypeA".to_string())),
                    &generics
                )
                .is_ok()
        );
        assert!(
            converter
                .convert_type(
                    &spanned(AstType::Identifier("TypeB".to_string())),
                    &generics
                )
                .is_ok()
        );

        // Invalid types should return errors
        let result = converter.convert_type(
            &spanned(AstType::Identifier("UnknownType".to_string())),
            &generics,
        );
        assert!(result.is_err());

        // Test nested types
        assert!(
            converter
                .convert_type(
                    &spanned(AstType::Array(spanned(AstType::Identifier(
                        "TypeA".to_string()
                    )))),
                    &generics
                )
                .is_ok()
        );

        // Test invalid nested types
        let result = converter.convert_type(
            &spanned(AstType::Array(spanned(AstType::Identifier(
                "UnknownType".to_string(),
            )))),
            &generics,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_type_validation_in_complex_types() {
        let mut converter = ASTConverter::default();

        // Register types
        let data_type_adt = create_test_adt("DataType");
        let key_type_adt = create_test_adt("KeyType");

        converter
            .type_registry
            .register_adt(&data_type_adt)
            .unwrap();
        converter.type_registry.register_adt(&key_type_adt).unwrap();

        let generics = HashSet::new();

        // Test array with valid type
        let array_type = AstType::Array(spanned(AstType::Identifier("DataType".to_string())));
        assert!(
            converter
                .convert_type(&spanned(array_type), &generics)
                .is_ok()
        );

        // Test array with invalid type
        let invalid_array = AstType::Array(spanned(AstType::Identifier("InvalidType".to_string())));
        assert!(
            converter
                .convert_type(&spanned(invalid_array), &generics)
                .is_err()
        );

        // Test map with valid types
        let map_type = AstType::Map(
            spanned(AstType::Identifier("KeyType".to_string())),
            spanned(AstType::Identifier("DataType".to_string())),
        );
        assert!(
            converter
                .convert_type(&spanned(map_type), &generics)
                .is_ok()
        );

        // Test map with invalid key type
        let invalid_map1 = AstType::Map(
            spanned(AstType::Identifier("InvalidKey".to_string())),
            spanned(AstType::Identifier("DataType".to_string())),
        );
        assert!(
            converter
                .convert_type(&spanned(invalid_map1), &generics)
                .is_err()
        );

        // Test map with invalid value type
        let invalid_map2 = AstType::Map(
            spanned(AstType::Identifier("KeyType".to_string())),
            spanned(AstType::Identifier("InvalidValue".to_string())),
        );
        assert!(
            converter
                .convert_type(&spanned(invalid_map2), &generics)
                .is_err()
        );

        // Test nested types
        let nested_type = AstType::Array(spanned(AstType::Map(
            spanned(AstType::Identifier("KeyType".to_string())),
            spanned(AstType::Identifier("DataType".to_string())),
        )));
        assert!(
            converter
                .convert_type(&spanned(nested_type), &generics)
                .is_ok()
        );

        // Test nested types with invalid inner type
        let invalid_nested = AstType::Array(spanned(AstType::Map(
            spanned(AstType::Identifier("KeyType".to_string())),
            spanned(AstType::Identifier("InvalidData".to_string())),
        )));
        assert!(
            converter
                .convert_type(&spanned(invalid_nested), &generics)
                .is_err()
        );
    }

    #[test]
    fn test_type_validation_with_generics() {
        let mut converter = ASTConverter::default();

        // Register type
        let data_type_adt = create_test_adt("DataType");
        converter
            .type_registry
            .register_adt(&data_type_adt)
            .unwrap();

        // Setup generics
        let mut generics = HashSet::new();
        generics.insert("T".to_string());
        generics.insert("U".to_string());

        // Test generic types (should pass validation because they're in the generics set)
        let generic_type = AstType::Identifier("T".to_string());
        assert!(
            converter
                .convert_type(&spanned(generic_type), &generics)
                .is_ok()
        );

        // Test registered non-generic type
        let data_type = AstType::Identifier("DataType".to_string());
        assert!(
            converter
                .convert_type(&spanned(data_type), &generics)
                .is_ok()
        );

        // Test unregistered non-generic type
        let invalid_type = AstType::Identifier("InvalidType".to_string());
        assert!(
            converter
                .convert_type(&spanned(invalid_type), &generics)
                .is_err()
        );

        // Test with complex type involving both generics and registered types
        let complex_type = AstType::Map(
            spanned(AstType::Identifier("T".to_string())),
            spanned(AstType::Identifier("DataType".to_string())),
        );
        assert!(
            converter
                .convert_type(&spanned(complex_type), &generics)
                .is_ok()
        );

        // Test with complex type involving both generics and unregistered types
        let invalid_complex = AstType::Map(
            spanned(AstType::Identifier("T".to_string())),
            spanned(AstType::Identifier("InvalidType".to_string())),
        );
        assert!(
            converter
                .convert_type(&spanned(invalid_complex), &generics)
                .is_err()
        );
    }

    #[test]
    fn test_type_validation_in_special_types() {
        let mut converter = ASTConverter::default();

        // Register type
        let data_type_adt = create_test_adt("DataType");
        converter
            .type_registry
            .register_adt(&data_type_adt)
            .unwrap();

        let generics = HashSet::new();

        // Test Optional with valid type
        let optional_type =
            AstType::Questioned(spanned(AstType::Identifier("DataType".to_string())));
        assert!(
            converter
                .convert_type(&spanned(optional_type), &generics)
                .is_ok()
        );

        // Test Optional with invalid type
        let invalid_optional =
            AstType::Questioned(spanned(AstType::Identifier("InvalidType".to_string())));
        assert!(
            converter
                .convert_type(&spanned(invalid_optional), &generics)
                .is_err()
        );

        // Test Stored with valid type
        let stored_type = AstType::Starred(spanned(AstType::Identifier("DataType".to_string())));
        assert!(
            converter
                .convert_type(&spanned(stored_type), &generics)
                .is_ok()
        );

        // Test Stored with invalid type
        let invalid_stored =
            AstType::Starred(spanned(AstType::Identifier("InvalidType".to_string())));
        assert!(
            converter
                .convert_type(&spanned(invalid_stored), &generics)
                .is_err()
        );

        // Test Costed with valid type
        let costed_type = AstType::Dollared(spanned(AstType::Identifier("DataType".to_string())));
        assert!(
            converter
                .convert_type(&spanned(costed_type), &generics)
                .is_ok()
        );

        // Test Costed with invalid type
        let invalid_costed =
            AstType::Dollared(spanned(AstType::Identifier("InvalidType".to_string())));
        assert!(
            converter
                .convert_type(&spanned(invalid_costed), &generics)
                .is_err()
        );
    }
}
