use core::fmt;
use std::{
    fmt::{Display, Formatter},
    ops::Range,
};

use ariadne::{Report, Source};

use crate::lexer::errors::LexerError;

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
