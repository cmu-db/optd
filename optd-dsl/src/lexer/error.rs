use crate::utils::{error::Diagnose, span::Span};
use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::error::{Simple, SimpleReason};

/// Wrapper of a Chumsky Lexer error that adapts it to the Ariadne reporting system.
///
/// Handles lexical errors such as invalid characters and arithmetic overflow
/// in numeric literals.
#[derive(Debug)]
pub struct LexerError {
    /// The complete source code being lexed
    src_code: String,
    /// The underlying Chumsky error
    error: Simple<char, Span>,
}

impl LexerError {
    /// Creates a new lexer error from source code and a Chumsky error.
    pub fn new(src_code: String, error: Simple<char, Span>) -> Self {
        Self { src_code, error }
    }
}

impl Diagnose for LexerError {
    fn report(&self) -> Report<Span> {
        match self.error.reason() {
            SimpleReason::Custom(msg) => {
                // Special handling for numeric overflow
                if msg.contains("overflow") {
                    Report::build(ReportKind::Error, self.error.span())
                        .with_message("Integer overflow")
                        .with_label(
                            Label::new(self.error.span())
                                .with_message("This numeric literal is too large")
                                .with_color(Color::Magenta),
                        )
                        .with_help("Use a smaller number or a different numeric type")
                        .finish()
                } else {
                    // Other custom errors
                    Report::build(ReportKind::Error, self.error.span())
                        .with_message("Lexical error")
                        .with_label(
                            Label::new(self.error.span())
                                .with_message(msg)
                                .with_color(Color::Magenta),
                        )
                        .finish()
                }
            }
            SimpleReason::Unexpected => {
                // For unexpected character errors
                let mut report = Report::build(ReportKind::Error, self.error.span())
                    .with_message("Invalid token");

                // What was found
                if let Some(c) = self.error.found() {
                    report = report.with_label(
                        Label::new(self.error.span())
                            .with_message(format!("Unexpected character: '{}'", c))
                            .with_color(Color::Magenta),
                    );
                } else {
                    report = report.with_label(
                        Label::new(self.error.span())
                            .with_message("Unexpected end of input")
                            .with_color(Color::Magenta),
                    );
                }

                report.finish()
            }
            // Unclosed should never happen in lexer, so panic
            SimpleReason::Unclosed { span, delimiter } => {
                panic!(
                    "Unexpected unclosed delimiter error in lexer: '{}' at {:?}",
                    delimiter, span
                );
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
