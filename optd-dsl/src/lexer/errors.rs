use std::{
    fmt::{self, Display, Formatter},
    ops::Range,
};

use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::error::{Simple, SimpleReason};

use crate::errors::Reporter;

#[derive(Debug)]
pub struct LexerError {
    src_code: String,
    src_file: String,
    error: Simple<char>,
}

impl LexerError {
    pub fn new(src_code: String, src_file: String, error: Simple<char>) -> Self {
        Self {
            src_code,
            src_file,
            error,
        }
    }
}

impl Reporter for LexerError {
    fn report(&self) -> Report<(String, Range<usize>)> {
        let reason = match &self.error.reason() {
            SimpleReason::Custom(msg) => msg.clone(),
            _ => self.error.to_string(),
        };

        Report::build(
            ReportKind::Error,
            (self.src_file.clone(), self.error.span()),
        )
        .with_message("Lexer error")
        .with_label(
            Label::new((self.src_file.clone(), self.error.span()))
                .with_message(&reason)
                .with_color(Color::Red),
        )
        .finish()
    }

    fn source(&self) -> (String, Source) {
        (self.src_file.clone(), Source::from(self.src_code.clone()))
    }
}

impl Display for LexerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let reason = match &self.error.reason() {
            SimpleReason::Custom(msg) => msg.clone(),
            _ => self.error.to_string(),
        };
        write!(
            f,
            "Lexer error in {} at {:?}: {}",
            self.src_file,
            self.error.span(),
            reason
        )
    }
}
