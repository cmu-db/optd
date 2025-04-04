use crate::utils::{error::Diagnose, span::Span};
use ariadne::{Color, Label, Report, ReportKind, Source};

/// Wrapper for semantic analysis errors
#[derive(Debug)]
pub struct SemanticError {
    /// The source code being analyzed
    src_code: String,
    /// The specific kind of semantic error
    kind: SemanticErrorKind,
}

impl SemanticError {
    /// Creates a new semantic error
    pub fn new(src_code: String, kind: SemanticErrorKind) -> Box<Self> {
        Box::new(Self { src_code, kind })
    }
}

/// Specific types of semantic errors
#[derive(Debug)]
pub enum SemanticErrorKind {
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

impl SemanticErrorKind {
    /// Creates a new error for duplicate ADT names
    pub fn new_duplicate_adt(name: String, first_span: Span, duplicate_span: Span) -> Self {
        Self::DuplicateAdt {
            name,
            first_span,
            duplicate_span,
        }
    }

    /// Creates a new error for duplicate identifier names within the same scope
    pub fn new_duplicate_identifier(name: String, first_span: Span, duplicate_span: Span) -> Self {
        Self::DuplicateIdentifier {
            name,
            first_span,
            duplicate_span,
        }
    }

    /// Creates a new error for incomplete functions
    pub fn new_incomplete_function(name: String, span: Span) -> Self {
        Self::IncompleteFunction { name, span }
    }
}

impl Diagnose for Box<SemanticError> {
    fn report(&self) -> Report<Span> {
        match &self.kind {
            SemanticErrorKind::DuplicateAdt { name, first_span, duplicate_span } => {
                Report::build(ReportKind::Error, duplicate_span.clone())
                    .with_message(format!("Duplicate ADT definition: '{}'", name))
                    .with_label(
                        Label::new(duplicate_span.clone())
                            .with_message("Duplicate ADT defined here")
                            .with_color(Color::Magenta)
                    )
                    .with_label(
                        Label::new(first_span.clone())
                            .with_message("First defined here")
                            .with_color(Color::Blue)
                    )
                    .with_help("Consider using a different name for this ADT or removing one of the definitions")
                    .finish()
            },

            SemanticErrorKind::DuplicateIdentifier { name, first_span, duplicate_span } => {
                Report::build(ReportKind::Error, duplicate_span.clone())
                    .with_message(format!("Duplicate identifier: '{}'", name))
                    .with_label(
                        Label::new(duplicate_span.clone())
                            .with_message("Duplicate identifier declared here")
                            .with_color(Color::Magenta)
                    )
                    .with_label(
                        Label::new(first_span.clone())
                            .with_message("First declared here")
                            .with_color(Color::Blue)
                    )
                    .with_help("Identifiers must be unique within the same scope")
                    .finish()
            },

            SemanticErrorKind::IncompleteFunction { name, span } => {
                Report::build(ReportKind::Error, span.clone())
                    .with_message(format!("Incomplete function definition: '{}'", name))
                    .with_label(
                        Label::new(span.clone())
                            .with_message("Function must have at least one parameter")
                            .with_color(Color::Magenta)
                    )
                    .with_help("Add at least one parameter or a receiver to this function")
                    .with_note("Functions without parameters are not supported in this language")
                    .finish()
            }
        }
    }

    fn source(&self) -> (String, Source) {
        let span = match &self.kind {
            SemanticErrorKind::DuplicateAdt { duplicate_span, .. } => duplicate_span,
            SemanticErrorKind::DuplicateIdentifier { duplicate_span, .. } => duplicate_span,
            SemanticErrorKind::IncompleteFunction { span, .. } => span,
        };

        (span.src_file.clone(), Source::from(self.src_code.clone()))
    }
}
