//! The transformation rules are split up in two categories: logical and scalar transformations.
//! See doc.md for more information about the transformation rules.

use std::sync::Arc;

use crate::cascades::patterns::{LogicalPattern, ScalarPattern};

#[derive(Clone)]
pub enum Transformation {
    Logical(LogicalTransformation),
    Scalar(ScalarTransformation),
}

#[derive(Clone)]
pub struct LogicalTransformation {
    pub name: String,
    pub matches: Vec<LogicalMatch>,
}

#[derive(Clone)]
pub struct LogicalMatch {
    pub pattern: Vec<LogicalPattern>,
    pub composition: Vec<(String, Arc<Transformation>)>,
}

#[derive(Clone)]
pub struct ScalarTransformation {
    pub name: String,
    pub matches: Vec<ScalarMatch>,
}

#[derive(Clone)]
pub struct ScalarMatch {
    pub pattern: Vec<ScalarPattern>,
    pub composition: Vec<(String, Arc<ScalarTransformation>)>,
}
