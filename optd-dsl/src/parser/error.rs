use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::error::{Simple, SimpleReason};

use crate::utils::{error::Diagnose, span::Span};

/// Wrapper of a Chumsky Parser error.
#[derive(Debug)]
pub struct ParserError {
    src_code: String,
    error: Simple<char, Span>,
}

impl ParserError {
    pub fn new(src_code: String, error: Simple<char, Span>) -> Self {
        Self { src_code, error }
    }
}

impl Diagnose for ParserError {
    fn report(&self) -> Report<Span> {
        let reason = match &self.error.reason() {
            SimpleReason::Custom(msg) => msg.clone(),
            _ => self.error.to_string(),
        };

        Report::build(ReportKind::Error, self.error.span())
            .with_message("Parser error")
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
