//! Literal conversion from AST to HIR
//!
//! This module contains functions for converting AST literal values to their
//! corresponding HIR representations.

use crate::analyzer::hir::Literal;
use crate::analyzer::types::Type;
use crate::parser::ast;

/// Converts an AST literal to an HIR literal and its corresponding type.
///
/// # Arguments
///
/// * `literal` - The AST literal to convert
///
/// # Returns
///
/// A tuple containing the converted HIR literal and its type
pub(super) fn convert_literal(literal: &ast::Literal) -> (Literal, Type) {
    match literal {
        ast::Literal::Int64(val) => (Literal::Int64(*val), Type::Int64),
        ast::Literal::String(val) => (Literal::String(val.clone()), Type::String),
        ast::Literal::Bool(val) => (Literal::Bool(*val), Type::Bool),
        ast::Literal::Float64(val) => (Literal::Float64(val.0), Type::Float64),
        ast::Literal::Unit => (Literal::Unit, Type::Unit),
    }
}
