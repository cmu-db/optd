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
