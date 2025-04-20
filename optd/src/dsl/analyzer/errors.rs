use super::{hir::Identifier, types::registry::Type};
use crate::dsl::utils::{
    errors::Diagnose,
    span::{Span, Spanned},
};
use ariadne::{Color, Label, Report, ReportKind, Source};

/// Wrapper for analysis errors
#[derive(Debug)]
pub struct AnalyzerError {
    /// The source code being analyzed
    src_code: String,
    /// The specific kind of error
    kind: AnalyzerErrorKind,
}

impl AnalyzerError {
    /// Creates a new analyzer error
    pub fn new(src_code: String, kind: AnalyzerErrorKind) -> Box<Self> {
        Box::new(Self { src_code, kind })
    }
}

/// Specific types of analyzer errors
#[derive(Debug)]
pub enum AnalyzerErrorKind {
    DuplicateAdt {
        name: String,
        first_span: Span,
        duplicate_span: Span,
    },

    DuplicateIdentifier {
        name: String,
        first_span: Span,
        duplicate_span: Span,
    },

    // Specifically: no receiver and no arguments,
    // but got accepted by the parser.
    IncompleteFunction {
        name: String,
        span: Span,
    },

    UndefinedReference {
        name: String,
        span: Span,
    },

    CyclicType {
        path: Vec<Spanned<Identifier>>,
    },

    UndefinedType {
        name: String,
        span: Span,
    },

    UnconstructibleType {
        name: String,
        span: Span,
    },

    MissingCoreType {
        name: String,
        src_path: String,
    },

    InvalidType {
        span: Span,
    },

    InvalidInheritance {
        parent_name: String,
        parent_span: Span,
        child_name: String,
        child_span: Span,
    },

    FieldNumberMismatch {
        name: String,
        span: Span,
        expected: usize,
        found: usize,
    },

    InvalidSubtype {
        child: Type,
        parent: Type,
        span: Span,
    },

    InvalidFieldAccess {
        object: Type,
        span: Span,
        field: String,
    },
}

impl AnalyzerErrorKind {
    pub fn new_duplicate_adt(name: &str, first_span: &Span, duplicate_span: &Span) -> Box<Self> {
        Self::DuplicateAdt {
            name: name.to_string(),
            first_span: first_span.clone(),
            duplicate_span: duplicate_span.clone(),
        }
        .into()
    }

    pub fn new_duplicate_identifier(
        name: &str,
        first_span: &Span,
        duplicate_span: &Span,
    ) -> Box<Self> {
        Self::DuplicateIdentifier {
            name: name.to_string(),
            first_span: first_span.clone(),
            duplicate_span: duplicate_span.clone(),
        }
        .into()
    }

    pub fn new_incomplete_function(name: &str, span: &Span) -> Box<Self> {
        Self::IncompleteFunction {
            name: name.to_string(),
            span: span.clone(),
        }
        .into()
    }

    pub fn new_undefined_reference(name: &str, span: &Span) -> Box<Self> {
        Self::UndefinedReference {
            name: name.to_string(),
            span: span.clone(),
        }
        .into()
    }

    pub fn new_cyclic_type(path: &[Spanned<Identifier>]) -> Box<Self> {
        Self::CyclicType {
            path: path.to_vec(),
        }
        .into()
    }

    pub fn new_undefined_type(name: &str, span: &Span) -> Box<Self> {
        Self::UndefinedType {
            name: name.to_string(),
            span: span.clone(),
        }
        .into()
    }

    pub fn new_unconstructible_type(name: &str, span: &Span) -> Box<Self> {
        Self::UnconstructibleType {
            name: name.to_string(),
            span: span.clone(),
        }
        .into()
    }

    pub fn new_missing_core_type(name: &str, src_path: &str) -> Box<Self> {
        Self::MissingCoreType {
            name: name.to_string(),
            src_path: src_path.to_string(),
        }
        .into()
    }

    pub fn new_invalid_type(span: &Span) -> Box<Self> {
        Self::InvalidType { span: span.clone() }.into()
    }

    pub fn new_invalid_inheritance(
        parent_name: &str,
        parent_span: &Span,
        child_name: &str,
        child_span: &Span,
    ) -> Box<Self> {
        Self::InvalidInheritance {
            parent_name: parent_name.to_string(),
            parent_span: parent_span.clone(),
            child_name: child_name.to_string(),
            child_span: child_span.clone(),
        }
        .into()
    }

    pub fn new_field_number_mismatch(
        name: &str,
        span: &Span,
        expected: usize,
        found: usize,
    ) -> Box<Self> {
        Self::FieldNumberMismatch {
            name: name.to_string(),
            span: span.clone(),
            expected,
            found,
        }
        .into()
    }

    pub fn new_invalid_subtype(child: &Type, parent: &Type, span: &Span) -> Box<Self> {
        Self::InvalidSubtype {
            child: child.clone(),
            parent: parent.clone(),
            span: span.clone(),
        }
        .into()
    }

    pub fn new_invalid_field_access(object: &Type, span: &Span, field: &Identifier) -> Box<Self> {
        Self::InvalidFieldAccess {
            object: object.clone(),
            span: span.clone(),
            field: field.clone(),
        }
        .into()
    }
}

impl Diagnose for Box<AnalyzerError> {
    fn report(&self) -> Report<Span> {
        use AnalyzerErrorKind::*;

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
            IncompleteFunction { name, span } => self.build_single_span_report(
                span,
                &format!("Incomplete function definition: '{}'", name),
                "Function must have at least one parameter",
                "Add at least one parameter or a receiver to this function",
            ),
            UndefinedReference { name, span } => self.build_single_span_report(
                span,
                &format!("Undefined reference to: '{}'", name),
                &format!("'{}' is not defined in this scope", name),
                "Make sure the variable is declared before use or check for typos in the name",
            ),
            CyclicType { path } => self.build_cyclic_type_report(path),
            UndefinedType { name, span } => self.build_single_span_report(
                span,
                &format!("Undefined type: '{}'", name),
                "Undefined type reference",
                "Make sure the type is declared before use",
            ),
            UnconstructibleType { name, span } => self.build_single_span_report(
                span,
                &format!("Unconstructible type: '{}'", name),
                "Type cannot be constructed, or does not exist",
                "Only defined leaf types can be constructed",
            ),
            MissingCoreType { name, src_path } => self.build_missing_core_type_report(name, src_path),
            InvalidType { span } => self.build_single_span_report(
                span,
                "Invalid type usage",
                "Type cannot be used in this context",
                "This type is not allowed in this specific location - check the language restrictions for this context",
            ),
            InvalidInheritance {
                parent_name,
                parent_span,
                child_name,
                child_span,
            } => self.build_duplicate_report(
                child_span,
                parent_span,
                &format!("Invalid inheritance: '{}' cannot inherit from '{}'", child_name, parent_name),
                "Invalid inheritance reference",
                "Parent type defined here",
                "Check the inheritance hierarchy and ensure that the parent type is valid",
            ),
            FieldNumberMismatch {
                name,
                span,
                expected,
                found,
            } => self.build_single_span_report(
                span,
                &format!("Field number mismatch in: '{}'", name),
                &format!("Expected {} fields, but found {}", expected, found),
                "Check the number of fields in the type definition",
            ),
            InvalidSubtype { child, parent, span } => self.build_type_mismatch_report(child, parent, span),
            InvalidFieldAccess { object, span, field } => self.build_invalid_field_access_report(object, span, field),
        }
    }

    fn source(&self) -> (String, Source) {
        use AnalyzerErrorKind::*;

        let span = match &self.kind {
            DuplicateAdt { duplicate_span, .. } => duplicate_span,
            DuplicateIdentifier { duplicate_span, .. } => duplicate_span,
            IncompleteFunction { span, .. } => span,
            UndefinedReference { span, .. } => span,
            CyclicType { path } => &path[0].span,
            UndefinedType { span, .. } => span,
            UnconstructibleType { span, .. } => span,
            MissingCoreType { src_path, .. } => &Span::new(src_path.to_string(), 0..0),
            InvalidType { span, .. } => span,
            InvalidInheritance { child_span, .. } => child_span,
            FieldNumberMismatch { span, .. } => span,
            InvalidSubtype { span, .. } => span,
            InvalidFieldAccess { span, .. } => span,
        };

        (span.src_file.clone(), Source::from(self.src_code.clone()))
    }
}

impl AnalyzerError {
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

    /// Helper method to build a report for type mismatch errors
    fn build_type_mismatch_report(&self, child: &Type, parent: &Type, span: &Span) -> Report<Span> {
        let mut report = Report::build(ReportKind::Error, span.clone()).with_message(format!(
            "Type error: '{}' is not a subtype of '{}'",
            child, parent
        ));

        report = report.with_label(
            Label::new(span.clone())
                .with_message(format!("Expected type '{}' but found '{}'", parent, child))
                .with_color(Color::Red),
        );

        if let Some(parent_type_span) = parent.span.as_ref() {
            report = report.with_label(
                Label::new(parent_type_span.clone())
                    .with_message(format!(
                        "Expected type '{}' defined or annotated here",
                        parent
                    ))
                    .with_color(Color::Yellow),
            );
        }

        report
            .with_help("The types are incompatible - consider adding an explicit type conversion or using a compatible type")
            .finish()
    }

    /// Helper method to build a report for invalid field access errors
    fn build_invalid_field_access_report(
        &self,
        object: &Type,
        span: &Span,
        field: &String,
    ) -> Report<Span> {
        let mut report = Report::build(ReportKind::Error, span.clone()).with_message(format!(
            "Invalid field access: type '{}' has no field '{}'",
            object, *field
        ));

        report = report.with_label(
            Label::new(span.clone())
                .with_message(format!("Expression of type '{}'", object))
                .with_color(Color::Blue),
        );

        report
            .with_help("Check for typos in the field name or ensure you're accessing the right type of object")
            .finish()
    }

    /// Helper method to build a report for cyclic ADT definitions
    fn build_cyclic_type_report(&self, path: &[Spanned<Identifier>]) -> Report<Span> {
        let cycle_start = &path[0];
        let mut report = Report::build(ReportKind::Error, cycle_start.span.clone()).with_message(
            format!("Cyclic type definition detected: '{}'", cycle_start.value),
        );

        report = report.with_label(
            Label::new(cycle_start.span.clone())
                .with_message(format!("'{}' is defined here", cycle_start.value))
                .with_order(0)
                .with_color(Color::Red),
        );

        // If the cycle has more than one element, add labels for the references.
        if path.len() > 1 {
            // Add labels for each intermediate reference in the cycle (if any).
            // Skip the first element (definition) and the last element (cycle completion).
            if path.len() > 2 {
                for (i, type_ref) in path.iter().enumerate().skip(1).take(path.len() - 2) {
                    report = report.with_label(
                        Label::new(type_ref.span.clone())
                            .with_message(format!("references '{}'", type_ref.value))
                            .with_color(Color::Blue)
                            .with_order(i as i32),
                    );
                }
            }

            let last_idx = path.len() - 1;
            report = report.with_label(
                Label::new(path[last_idx].span.clone())
                    .with_message(format!(
                        "cycle completes: references '{}' again",
                        path[0].value
                    ))
                    .with_color(Color::Red)
                    .with_order(last_idx as i32),
            );
        } else {
            // Self reference.
            report = report.with_label(
                Label::new(cycle_start.span.clone())
                    .with_message("directly references itself")
                    .with_color(Color::Red),
            );
        }

        report
            .with_help("Break this cycle by introducing a layer of indirection or refactoring the type structure")
            .with_note("This would cause infinite recursion")
            .finish()
    }

    /// Helper method to build a report for missing core types.
    fn build_missing_core_type_report(&self, name: &str, src_path: &str) -> Report<Span> {
        // Create a span at the beginning of the file.
        let span = Span::new(src_path.to_string(), 0..0);

        Report::build(ReportKind::Error, span.clone())
            .with_message(format!("Missing required core type: '{}'", name))
            .with_label(Label::new(span.clone()).with_color(Color::Red))
            .with_help(format!(
                "Add a definition for the '{}' type in your module",
                name
            ))
            .with_note(format!(
                "'{}' is a fundamental type needed by the compiler",
                name
            ))
            .finish()
    }
}
