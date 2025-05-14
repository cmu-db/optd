//! This module provides bridge functionality between the DSL's internal representation (HIR) and
//! the query optimizer's intermediate representation (Optd-IR).
//!
//! The bridge consists of two main components:
//!
//! - [`from_cir`]: Converts optd's type representations (CIR) into DSL [`Value`]s (HIR).
//! - [`into_cir`]: Converts HIR [`Value`]s into optd's type representations (CIR).
//!
//! These bidirectional conversions enable the DSL to interact with the query optimizer, allowing
//! rule-based transformations to be applied to query plans while maintaining the ability to work
//! with both representations.
//!
//! [`Value`]: crate::core::analyzer::hir::Value

use crate::{
    cir::{ImplementationRule, RuleBook, TransformationRule},
    dsl::analyzer::{
        hir::{ExprMetadata, HIR},
        into_hir::annotations::{IMPLEMENTATION_ANNOTATION, TRANSFORMATION_ANNOTATION},
    },
};

pub mod from_cir;
pub mod into_cir;

/// Extracts optimization rules from HIR annotations using a functional approach
///
/// This function processes the HIR annotations and builds a RuleBook containing
/// all transformation and implementation rules defined in the program.
pub fn extract_rulebook_from_hir<M: ExprMetadata>(hir: &HIR<M>) -> RuleBook {
    RuleBook {
        transformations: hir
            .annotations
            .iter()
            .filter_map(|(id, annotations)| {
                if annotations.iter().any(|a| a == TRANSFORMATION_ANNOTATION) {
                    Some(TransformationRule(id.clone()))
                } else {
                    None
                }
            })
            .collect(),

        implementations: hir
            .annotations
            .iter()
            .filter_map(|(id, annotations)| {
                if annotations.iter().any(|a| a == IMPLEMENTATION_ANNOTATION) {
                    Some(ImplementationRule(id.clone()))
                } else {
                    None
                }
            })
            .collect(),
    }
}
