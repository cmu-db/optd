//! Type conversion from AST to HIR
//!
//! This module contains functions for converting AST type nodes to their
//! corresponding HIR type representations.

use crate::analyzer::hir::Identifier;
use crate::analyzer::types::Type;
use crate::parser::ast;
use std::collections::HashSet;

/// Converts an AST type to an HIR type.
pub(crate) fn convert_type(ast_type: &ast::Type, generics: &HashSet<Identifier>) -> Type {
    match ast_type {
        ast::Type::Int64 => Type::Int64,
        ast::Type::String => Type::String,
        ast::Type::Bool => Type::Bool,
        ast::Type::Float64 => Type::Float64,
        ast::Type::Unit => Type::Unit,
        ast::Type::Array(elem_type) => Type::Array(convert_type(&elem_type.value, generics).into()),
        ast::Type::Closure(param_type, return_type) => Type::Closure(
            Box::new(convert_type(&param_type.value, generics)),
            Box::new(convert_type(&return_type.value, generics)),
        ),
        ast::Type::Tuple(types) => {
            let hir_types = types
                .iter()
                .map(|t| convert_type(&t.value, generics))
                .collect();
            Type::Tuple(hir_types)
        }
        ast::Type::Map(key_type, value_type) => Type::Map(
            convert_type(&key_type.value, generics).into(),
            convert_type(&value_type.value, generics).into(),
        ),
        ast::Type::Questioned(inner_type) => {
            Type::Optional(convert_type(&inner_type.value, generics).into())
        }
        ast::Type::Starred(inner_type) => {
            Type::Stored(convert_type(&inner_type.value, generics).into())
        }
        ast::Type::Dollared(inner_type) => {
            Type::Costed(convert_type(&inner_type.value, generics).into())
        }
        ast::Type::Identifier(name) => {
            if generics.contains(name) {
                Type::Generic(name.clone())
            } else {
                Type::Adt(name.clone())
            }
        }
        ast::Type::Error => panic!("AST should no longer contain errors"),
        ast::Type::Unknown => Type::Unknown,
    }
}

/// Creates a function type from parameter types and return type.
pub(super) fn create_function_type(params: &[(Identifier, Type)], return_type: &Type) -> Type {
    let param_types = params.iter().map(|(_, ty)| ty.clone()).collect::<Vec<_>>();

    let param_type = if params.is_empty() {
        Type::Unit
    } else if param_types.len() == 1 {
        param_types[0].clone()
    } else {
        Type::Tuple(param_types)
    };

    Type::Closure(param_type.into(), return_type.clone().into())
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

    #[test]
    fn test_convert_primitive_types() {
        // Test each primitive type
        let test_cases = vec![
            (ast::Type::Int64, Type::Int64),
            (ast::Type::String, Type::String),
            (ast::Type::Bool, Type::Bool),
            (ast::Type::Float64, Type::Float64),
            (ast::Type::Unit, Type::Unit),
            (ast::Type::Unknown, Type::Unknown),
        ];

        let generics = HashSet::new();

        for (ast_type, expected_type) in test_cases {
            let result = convert_type(&ast_type, &generics);
            assert_eq!(result, expected_type);
        }
    }

    #[test]
    fn test_convert_complex_types() {
        let generics = HashSet::new();

        // Test array type
        let array_type = ast::Type::Array(spanned(ast::Type::Int64));
        let result = convert_type(&array_type, &generics);
        match result {
            Type::Array(elem_type) => assert_eq!(*elem_type, Type::Int64),
            _ => panic!("Expected Array type"),
        }

        // Test closure type
        let param_type = spanned(ast::Type::Int64);
        let return_type = spanned(ast::Type::Bool);
        let closure_type = ast::Type::Closure(param_type, return_type);
        let result = convert_type(&closure_type, &generics);
        match result {
            Type::Closure(param, ret) => {
                assert_eq!(*param, Type::Int64);
                assert_eq!(*ret, Type::Bool);
            }
            _ => panic!("Expected Closure type"),
        }

        // Test map type
        let key_type = spanned(ast::Type::String);
        let value_type = spanned(ast::Type::Int64);
        let map_type = ast::Type::Map(key_type, value_type);
        let result = convert_type(&map_type, &generics);
        match result {
            Type::Map(key, value) => {
                assert_eq!(*key, Type::String);
                assert_eq!(*value, Type::Int64);
            }
            _ => panic!("Expected Map type"),
        }
    }

    #[test]
    fn test_convert_special_types() {
        let generics = HashSet::new();

        // Test questioned type (Optional)
        let inner_type = spanned(ast::Type::Int64);
        let questioned_type = ast::Type::Questioned(inner_type);
        let result = convert_type(&questioned_type, &generics);
        match result {
            Type::Optional(inner) => assert_eq!(*inner, Type::Int64),
            _ => panic!("Expected Optional type"),
        }

        // Test starred type (Stored)
        let inner_type = spanned(ast::Type::Int64);
        let starred_type = ast::Type::Starred(inner_type);
        let result = convert_type(&starred_type, &generics);
        match result {
            Type::Stored(inner) => assert_eq!(*inner, Type::Int64),
            _ => panic!("Expected Stored type"),
        }

        // Test dollared type (Costed)
        let inner_type = spanned(ast::Type::Int64);
        let dollared_type = ast::Type::Dollared(inner_type);
        let result = convert_type(&dollared_type, &generics);
        match result {
            Type::Costed(inner) => assert_eq!(*inner, Type::Int64),
            _ => panic!("Expected Costed type"),
        }
    }

    #[test]
    fn test_convert_identifier_types() {
        // Test regular ADT identifier
        let adt_type = ast::Type::Identifier("MyType".to_string());
        let generics = HashSet::new();
        let result = convert_type(&adt_type, &generics);
        match result {
            Type::Adt(name) => assert_eq!(name, "MyType"),
            _ => panic!("Expected Adt type"),
        }

        // Test generic identifier
        let generic_type = ast::Type::Identifier("T".to_string());
        let mut generics = HashSet::new();
        generics.insert("T".to_string());
        let result = convert_type(&generic_type, &generics);
        match result {
            Type::Generic(name) => assert_eq!(name, "T"),
            _ => panic!("Expected Generic type"),
        }
    }

    #[test]
    fn test_create_function_type() {
        // Test with no parameters
        let params: Vec<(Identifier, Type)> = vec![];
        let return_type = Type::Int64;
        let result = create_function_type(&params, &return_type);
        match result {
            Type::Closure(param, ret) => {
                assert_eq!(*param, Type::Unit);
                assert_eq!(*ret, Type::Int64);
            }
            _ => panic!("Expected Closure type"),
        }

        // Test with one parameter
        let params = vec![("x".to_string(), Type::Int64)];
        let result = create_function_type(&params, &return_type);
        match result {
            Type::Closure(param, ret) => {
                assert_eq!(*param, Type::Int64);
                assert_eq!(*ret, Type::Int64);
            }
            _ => panic!("Expected Closure type"),
        }

        // Test with multiple parameters
        let params = vec![
            ("x".to_string(), Type::Int64),
            ("y".to_string(), Type::Bool),
        ];
        let result = create_function_type(&params, &return_type);
        match result {
            Type::Closure(param, ret) => {
                match &*param {
                    Type::Tuple(types) => {
                        assert_eq!(types.len(), 2);
                        assert_eq!(types[0], Type::Int64);
                        assert_eq!(types[1], Type::Bool);
                    }
                    _ => panic!("Expected Tuple type for parameters"),
                }
                assert_eq!(*ret, Type::Int64);
            }
            _ => panic!("Expected Closure type"),
        }
    }
}
