//! AST to HIR Conversion
//!
//! This module provides functionality for transforming Abstract Syntax Trees (AST)
//! into the High-level Intermediate Representation (HIR).
//!
//! The conversion process handles:
//! - Type resolution and registration
//! - Expression transformation
//! - Function and variable binding
//! - Annotation processing
//!
//! The main entry point is the `ASTConverter` struct which orchestrates the
//! conversion process and maintains the necessary state.

use super::{
    errors::AnalyzerErrorKind,
    hir::{HIR, TypedSpan, Udf},
    type_checks::registry::TypeRegistry,
};
use crate::dsl::parser::ast::{Item, Module};
use converter::ASTConverter;
use std::collections::HashMap;

mod converter;
mod expr;
mod pattern;
mod types;

/// Converts an AST module to a partially typed and spanned HIR.
///
/// This function provides the public interface to convert an AST to HIR without exposing
/// the internal ASTConverter struct.
///
/// # Arguments
///
/// * `module` - The AST module to convert
/// * `udfs` - Optional HashMap of user-defined functions
///
/// # Returns
///
/// The HIR representation and TypeRegistry, or an AnalyzerErrorKind if conversion fails.
pub fn from_ast(
    module: &Module,
    udfs: HashMap<String, Udf>,
) -> Result<(HIR<TypedSpan>, TypeRegistry), Box<AnalyzerErrorKind>> {
    let mut converter = ASTConverter::new_with_udfs(udfs);

    // First pass: Process all ADTs to register types.
    for item in &module.items {
        if let Item::Adt(spanned_adt) = item {
            converter.registry.register_adt(&spanned_adt.value)?;
        }
    }

    // Second pass: Process all functions, using the type registry to verify
    // for invalid type annotations & constructions.
    for item in &module.items {
        if let Item::Function(spanned_fn) = item {
            converter.register_function(spanned_fn)?;
        }
    }

    // Push the context scope of the module, as we have processed all functions.
    converter.context.push_scope();

    Ok((
        HIR {
            context: converter.context,
            annotations: converter.annotations,
        },
        converter.registry,
    ))
}
