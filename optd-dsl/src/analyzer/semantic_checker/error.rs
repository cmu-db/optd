use crate::utils::{error::Diagnose, span::Span};
use ariadne::{Report, Source};

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

    /// Error for duplicate identifier in the same scope
    DuplicateIdentifier {
        /// Name of the duplicate identifier
        name: String,
        /// Span of the first declaration
        first_span: Span,
        /// Span of the duplicate declaration
        duplicate_span: Span,
    },

    /// Error for an incomplete function
    /// Specifically: no receiver and no arguments, but got accepted by the parser
    IncompleteFunction {
        /// Name of the incomplete function
        name: String,
        /// Span of the incomplete function
        span: Span,
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

    /// Creates a new error for duplicate identifier names within the same scope
    pub fn new_duplicate_identifier(
        name: String,
        first_span: Span,
        duplicate_span: Span,
    ) -> Box<Self> {
        Self::DuplicateIdentifier {
            name,
            first_span,
            duplicate_span,
        }
        .into()
    }

    /// Creates a new error for incomplete functions
    pub fn new_incomplete_function(name: String, span: Span) -> Box<Self> {
        Self::IncompleteFunction { name, span }.into()
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
