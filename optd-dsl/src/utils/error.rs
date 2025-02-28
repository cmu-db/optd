use std::io::Write;

use ariadne::{Report, Source};
use enum_dispatch::enum_dispatch;

use crate::{lexer::error::LexerError, parser::error::ParserError};

use super::span::Span;

/// Reporter of all front end errors
#[enum_dispatch]
pub trait Diagnose {
    // Report: A diagnostic report for a single error from Ariadne
    fn report(&self) -> Report<Span>;
    // The source code and file name of the error
    fn source(&self) -> (String, Source);

    // Function implemented from report and source into a Write trait
    fn print<W: Write>(&self, w: W) -> std::io::Result<()> {
        self.report().write(self.source(), w)
    }
}

#[enum_dispatch(Diagnose)]
#[derive(Debug)]
pub enum Error {
    LexerError(LexerError),
    ParserError(ParserError),
}
