use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::error::{Simple, SimpleReason};

use crate::utils::{error::Diagnose, span::Span};

/// Wrapper of a Chumsky Parser error.
#[derive(Debug)]
pub struct LexerError {
    src_code: String,
    error: Simple<char, Span>,
}

impl LexerError {
    pub fn new(src_code: String, error: Simple<char, Span>) -> Self {
        Self { src_code, error }
    }
}

impl Diagnose for LexerError {
    fn report(&self) -> Report<Span> {
        let reason = match &self.error.reason() {
            // For now, custom is only arithmetic overflow of literal as that is not
            // a standard error message from Chumsky so has to be added
            SimpleReason::Custom(msg) => msg.clone(),
            _ => self.error.to_string(),
        };

        Report::build(ReportKind::Error, self.error.span())
            .with_message("Lexer error")
            .with_label(
                Label::new(self.error.span())
                    .with_message(&reason)
                    .with_color(Color::Red),
            )
            .finish()
    }

    fn source(&self) -> (String, Source) {
        (
            self.error.span().src_file,
            Source::from(self.src_code.clone()),
        )
    }
}
