use std::fmt::{self, Display, Formatter};

use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::error::{Simple, SimpleReason};

use crate::utils::{errors::Reporter, span::Span};

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

impl Reporter for LexerError {
    fn report(&self) -> Report<Span> {
        let reason = match &self.error.reason() {
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

impl Display for LexerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let reason = match &self.error.reason() {
            SimpleReason::Custom(msg) => msg.clone(),
            _ => self.error.to_string(),
        };
        write!(f, "Lexer error at {:?}: {}", self.error.span(), reason)
    }
}
