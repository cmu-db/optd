use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::error::{Simple, SimpleReason};
use core::fmt;
use std::{
    fmt::{Display, Formatter},
    ops::Range,
};

pub trait Reporter {
    fn report(&self) -> Report<(String, Range<usize>)>;
    fn source(&self) -> (String, Source);

    fn eprint(&self) -> std::io::Result<()> {
        let (file, source) = self.source();
        self.report().eprint((file, source))
    }
}

#[derive(Debug)]
pub enum Error {
    LexerError(LexerError),
}

impl From<LexerError> for Error {
    fn from(e: LexerError) -> Self {
        Error::LexerError(e)
    }
}

impl Reporter for Error {
    fn report(&self) -> Report<(String, Range<usize>)> {
        match self {
            Error::LexerError(e) => e.report(),
        }
    }

    fn source(&self) -> (String, Source) {
        match self {
            Error::LexerError(e) => e.source(),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::LexerError(e) => e.fmt(f),
        }
    }
}

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
