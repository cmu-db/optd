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

    /// Error for invalid, out of scope references
    InvalidReference {
        /// Name of the invalid reference
        name: String,
        /// Span of the invalid reference
        span: Span,
    },

    /// Error for cyclic ADT definitions
    CyclicAdt {
        /// Name of the cyclic ADT
        name: String,
        /// Span of the cyclic ADT
        span: Span,
    },

    /// Error for an undefined type
    UndefinedType {
        /// Name of the undefined type
        name: String,
        /// Span of the undefined type
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

    /// Creates a new error for invalid references
    pub fn new_invalid_reference(name: String, span: Span) -> Self {
        Self::InvalidReference { name, span }
    }

    /// Creates a new error for cyclic ADT definitions
    pub fn new_cyclic_adt(name: String, span: Span) -> Self {
        Self::CyclicAdt { name, span }
    }

    /// Creates a new error for an undefined type
    pub fn new_undefined_type(name: String, span: Span) -> Self {
        Self::UndefinedType { name, span }
    }
}

impl Diagnose for Box<SemanticError> {
    fn report(&self) -> Report<Span> {
        use SemanticErrorKind::*;

        match &self.kind {
            DuplicateAdt {
                name,
                first_span,
                duplicate_span,
            } => self.build_duplicate_report(
                duplicate_span,
                first_span,
                &format!("Duplicate ADT definition: '{}'", name),
                "Duplicate ADT defined here",
                "First defined here",
                "Consider using a different name for this ADT or removing one of the definitions",
            ),
            DuplicateIdentifier {
                name,
                first_span,
                duplicate_span,
            } => self.build_duplicate_report(
                duplicate_span,
                first_span,
                &format!("Duplicate identifier: '{}'", name),
                "Duplicate identifier declared here",
                "First declared here",
                "Identifiers must be unique within the same scope",
            ),
            IncompleteFunction { name, span } => Report::build(ReportKind::Error, span.clone())
                .with_message(format!("Incomplete function definition: '{}'", name))
                .with_label(
                    Label::new(span.clone())
                        .with_message("Function must have at least one parameter")
                        .with_color(Color::Magenta),
                )
                .with_help("Add at least one parameter or a receiver to this function")
                .with_note("Functions without parameters are not supported in this language")
                .finish(),
            InvalidReference { name, span } => self.build_single_span_report(
                span,
                &format!("Invalid reference to undefined identifier: '{}'", name),
                &format!("'{}' is not defined in this scope", name),
                "Make sure the variable is declared before use or check for typos in the name",
            ),
            CyclicAdt { name, span } => self.build_single_span_report(
                span,
                &format!("Cyclic ADT definition: '{}'", name),
                "Cyclic ADT definition detected",
                "Consider refactoring the ADT to remove the cyclic dependency",
            ),
            UndefinedType { name, span } => self.build_single_span_report(
                span,
                &format!("Undefined type: '{}'", name),
                "Undefined type reference",
                "Make sure the type is declared before use",
            ),
        }
    }

    fn source(&self) -> (String, Source) {
        use SemanticErrorKind::*;

        let span = match &self.kind {
            DuplicateAdt { duplicate_span, .. } => duplicate_span,
            DuplicateIdentifier { duplicate_span, .. } => duplicate_span,
            IncompleteFunction { span, .. } => span,
            InvalidReference { span, .. } => span,
            CyclicAdt { span, .. } => span,
            UndefinedType { span, .. } => span,
        };

        (span.src_file.clone(), Source::from(self.src_code.clone()))
    }
}

impl SemanticError {
    /// Helper method to build a report for errors with a single span
    fn build_single_span_report(
        &self,
        span: &Span,
        message: &str,
        label_msg: &str,
        help: &str,
    ) -> Report<Span> {
        Report::build(ReportKind::Error, span.clone())
            .with_message(message)
            .with_label(
                Label::new(span.clone())
                    .with_message(label_msg)
                    .with_color(Color::Red),
            )
            .with_help(help)
            .finish()
    }

    /// Helper method to build a report for errors with two spans (duplicates)
    fn build_duplicate_report(
        &self,
        current_span: &Span,
        original_span: &Span,
        message: &str,
        current_label: &str,
        original_label: &str,
        help: &str,
    ) -> Report<Span> {
        Report::build(ReportKind::Error, current_span.clone())
            .with_message(message)
            .with_label(
                Label::new(current_span.clone())
                    .with_message(current_label)
                    .with_color(Color::Magenta),
            )
            .with_label(
                Label::new(original_span.clone())
                    .with_message(original_label)
                    .with_color(Color::Blue),
            )
            .with_help(help)
            .finish()
    }
}
