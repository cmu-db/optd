use crate::{
    lexer::tokens::Token,
    utils::{error::Diagnose, span::Span},
};
use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::error::{Simple, SimpleReason};

/// Wrapper of a Chumsky Parser error that adapts it to the Ariadne reporting system.
///
/// Takes the errors produced by Chumsky during parsing and converts them to
/// rich error diagnostics using Ariadne.
#[derive(Debug)]
pub struct ParserError {
    /// The complete source code being parsed
    src_code: String,
    /// The underlying Chumsky error
    error: Simple<Token, Span>,
}

impl ParserError {
    /// Creates a new parser error from source code and a Chumsky error.
    pub fn new(src_code: String, error: Simple<Token, Span>) -> Self {
        Self { src_code, error }
    }
}

impl Diagnose for ParserError {
    fn report(&self) -> Report<Span> {
        match self.error.reason() {
            SimpleReason::Unclosed { span, delimiter } => {
                // Special handling for unclosed delimiters
                Report::build(ReportKind::Error, self.error.span())
                    .with_message("Unclosed delimiter")
                    .with_label(
                        Label::new(self.error.span())
                            .with_message(format!("Expected closing delimiter for '{}'", delimiter))
                            .with_color(Color::Red),
                    )
                    .with_label(
                        Label::new(span.clone())
                            .with_message(format!("Delimiter opened here"))
                            .with_color(Color::Cyan),
                    )
                    .with_help(format!(
                        "Add a matching closing delimiter for '{}'",
                        delimiter
                    ))
                    .finish()
            }
            SimpleReason::Unexpected => {
                // For unexpected token errors
                let mut report = Report::build(ReportKind::Error, self.error.span())
                    .with_message("Syntax error");

                // What was found
                if let Some(token) = self.error.found() {
                    report = report.with_label(
                        Label::new(self.error.span())
                            .with_message(format!("Unexpected token: '{:?}'", token))
                            .with_color(Color::Red),
                    );
                } else {
                    report = report.with_label(
                        Label::new(self.error.span())
                            .with_message("Unexpected end of input")
                            .with_color(Color::Red),
                    );
                }

                // What was expected
                let expected: Vec<_> = self
                    .error
                    .expected()
                    .filter_map(|token_opt| token_opt.as_ref().map(|token| format!("{:?}", token)))
                    .collect();

                if !expected.is_empty() {
                    let expected_msg = if expected.len() == 1 {
                        format!("Expected '{}'", expected[0])
                    } else {
                        format!(
                            "Expected one of: {}",
                            expected
                                .iter()
                                .map(|t| format!("'{}'", t))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    };
                    report = report.with_note(expected_msg);
                }

                report.finish()
            }
            SimpleReason::Custom(msg) => {
                // For custom error messages
                Report::build(ReportKind::Error, self.error.span())
                    .with_message("Parser error")
                    .with_label(
                        Label::new(self.error.span())
                            .with_message(msg)
                            .with_color(Color::Red),
                    )
                    .finish()
            }
        }
    }

    fn source(&self) -> (String, Source) {
        (
            self.error.span().src_file.clone(),
            Source::from(self.src_code.clone()),
        )
    }
}
