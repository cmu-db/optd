//! Conversion module for transforming typed HIR into untyped HIR
//!
//! This module handles the transformation of High-level Intermediate Representation (HIR)
//! from a typed form (with `TypedSpan` metadata) to an untyped form. The conversion process
//! involves:
//!
//! 1. Removing type annotations from expressions and patterns
//! 2. Converting struct expressions to operator expressions where appropriate
//! 3. Transforming field access expressions into indexing operations
//! 4. Validating function annotations to ensure they match expected signatures
//!
//! # Structure
//!
//! The module is organized into several submodules:
//!
//! - `annotations`: Annotation definitions and validation logic
//! - `converter`: Main conversion logic for expressions and patterns
//! - `operators`: Specialized logic for transforming structs into logical/physical operators
//! - `field_indexing`: Utilities for calculating proper field indices, accounting for operator structures
//!
//! # Error Handling
//!
//! The conversion process returns a Result type to handle validation errors,
//! particularly for function annotations that don't match their expected signatures.

use super::errors::AnalyzerErrorKind;
use crate::dsl::analyzer::{
    context::Context,
    hir::{CoreData, FunKind, HIR, TypedSpan, Value},
    type_checks::registry::TypeRegistry,
};
use annotations::validate_annotation;
use converter::convert_expr;

pub(crate) mod annotations;
mod converter;
mod field_indexing;
mod operators;

/// Converts a typed HIR into an untyped HIR by removing all type annotations
///
/// This function takes a HIR with type information and produces a HIR without type information,
/// converting all expressions and patterns in the process. It also validates function annotations
/// to ensure they match the expected signatures.
///
/// # Arguments
///
/// * `hir_typedspan` - The typed HIR to convert
/// * `registry` - The type registry containing type information
///
/// # Returns
///
/// A Result containing either:
/// - A new HIR instance without type annotations
/// - An AnalyzerError if annotation validation fails
///
/// # Errors
///
/// Returns an error if:
/// - A function with a `transformation` annotation doesn't match the expected signature: `Logical* -> Logical`
/// - A function with an `implementation` annotation doesn't match the expected signature: `Physical* -> (PhysicalProperties -> Physical)`
pub fn into_hir(
    hir_typedspan: HIR<TypedSpan>,
    registry: &mut TypeRegistry,
) -> Result<HIR, Box<AnalyzerErrorKind>> {
    use CoreData::*;
    use FunKind::*;

    let mut context: Context = Default::default();

    // Convert all function bindings from the original context.
    for (name, fun) in hir_typedspan.context.get_all_bindings() {
        // Validate annotations if they exist.
        if let Some(annotations) = hir_typedspan.annotations.get(name) {
            annotations.iter().try_for_each(|annotation| {
                validate_annotation(annotation, &fun.metadata.ty, &fun.metadata.span, registry)
            })?;
        }

        let converted_fun = match &fun.data {
            Function(Closure(args, body)) => Closure(args.clone(), convert_expr(body, registry)),
            Function(Udf(udf)) => Udf(udf.clone()),
            _ => panic!("Expected a function, but got: {:?}", fun.data),
        };

        context.bind(name.clone(), Value::new(Function(converted_fun)));
    }

    // Push the context scope of the module, as we have processed all functions.
    context.push_scope();

    Ok(HIR {
        context,
        annotations: hir_typedspan.annotations,
    })
}
