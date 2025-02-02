//! The transformation rules are split up in two categories: logical and scalar transformations.
//! See doc.md for more information about the transformation rules.

use scalar::ScalarTransform;

pub mod logical;
pub mod scalar;

#[derive(Clone)]
pub enum Transform {
    // Logical(LogicalTransform),
    Scalar(ScalarTransform),
}
