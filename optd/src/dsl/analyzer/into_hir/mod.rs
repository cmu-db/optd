//! Conversion module for transforming typed HIR into untyped HIR
//!
//! This module handles the transformation of High-level Intermediate Representation (HIR)
//! from a typed form (with `TypedSpan` metadata) to an untyped form. The conversion process
//! involves:
//!
//! 1. Removing type annotations from expressions and patterns
//! 2. Converting struct expressions to operator expressions where appropriate
//! 3. Transforming field access expressions into indexing operations
//!
//! # Structure
//!
//! The module is organized into several submodules:
//!
//! - `converter`: Main conversion logic for expressions and patterns
//! - `operators`: Specialized logic for transforming structs into logical/physical operators
//! - `field_indexing`: Utilities for calculating proper field indices, accounting for operator structures

use converter::convert_expr;

use crate::dsl::analyzer::{
    context::Context,
    hir::{CoreData, FunKind, HIR, TypedSpan, Value},
    type_checks::registry::TypeRegistry,
};

mod converter;
mod field_indexing;
mod operators;

/// Converts a typed HIR into an untyped HIR by removing all type annotations
///
/// This function takes a HIR with type information and produces a HIR without type information,
/// converting all expressions and patterns in the process.
///
/// # Arguments
///
/// * `hir` - The typed HIR to convert
/// * `registry` - The type registry containing type information
///
/// # Returns
///
/// A new HIR instance without type annotations
pub fn into_hir(hir_typedspan: HIR<TypedSpan>, registry: &TypeRegistry) -> HIR {
    use CoreData::*;
    use FunKind::*;

    let mut context: Context = Default::default();

    // Convert all function bindings from the original context.
    for (name, fun) in hir_typedspan.context.get_all_bindings() {
        let converted_fun = match &fun.data {
            Function(Closure(args, body)) => Closure(args.clone(), convert_expr(body, registry)),
            Function(Udf(udf)) => Udf(udf.clone()),
            _ => panic!("Expected a function, but got: {:?}", fun.data),
        };

        context.bind(name.clone(), Value::new(Function(converted_fun)));
    }

    // Push the context scope of the module, as we have processed all functions.
    context.push_scope();

    HIR {
        context,
        annotations: hir_typedspan.annotations,
    }
}
