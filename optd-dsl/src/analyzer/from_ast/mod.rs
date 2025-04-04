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

pub mod converter;
pub mod expr;
pub mod literals;
pub mod types;

pub use converter::ASTConverter;
