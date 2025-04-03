use ariadne::{Report, Source};

use crate::utils::{error::Diagnose, span::Span};

/// Error types for semantic analysis
#[derive(Debug)]
pub enum SemanticError {
    /// Error for duplicate ADT names
    DuplicateAdt {
        /// Name of the duplicate ADT
        name: String,
        /// Span of the first declaration
        first_span: Span,
        /// Span of the duplicate declaration
        duplicate_span: Span,
    },
}

impl SemanticError {
    /// Creates a new error for duplicate ADT names
    pub fn new_duplicate_adt(name: String, first_span: Span, duplicate_span: Span) -> Box<Self> {
        Self::DuplicateAdt {
            name,
            first_span,
            duplicate_span,
        }
        .into()
    }
}

impl Diagnose for Box<SemanticError> {
    fn report(&self) -> Report<Span> {
        todo!()
    }

    fn source(&self) -> (String, Source) {
        todo!()
    }
}
