use super::span::Span;
use crate::{
    analyzer::{semantic_checker::error::SemanticError, type_checker::error::TypeError},
    lexer::error::LexerError,
    parser::error::ParserError,
};
use ariadne::{Report, Source};
use enum_dispatch::enum_dispatch;
use std::io::Write;

/// Reporter of all compilation errors
///
/// This trait provides a unified interface for error reporting across all
/// compilation phases of the DSL. It leverages the Ariadne crate for rich,
/// user-friendly error diagnostics with source code context.
#[enum_dispatch]
pub trait Diagnose {
    /// Creates an Ariadne diagnostic report for the error
    fn report(&self) -> Report<Span>;

    /// Returns the source code and filename where the error occurred
    fn source(&self) -> (String, Source);

    /// Writes a formatted error report to the provided output
    ///
    /// This is a convenience method that combines `report()` and `source()`
    /// to produce a complete error diagnostic.
    fn print<W: Write>(&self, w: W) -> std::io::Result<()> {
        self.report().write(self.source(), w)
    }
}

/// Unified error type for all compilation phases
///
/// `CompileError` aggregates errors from all phases of the DSL compilation:
/// - Lexing (tokenization)
/// - Parsing (syntax analysis)
/// - Analysis (semantic analysis)
/// - Typing (type checking)
///
/// Each error variant implements the `Diagnose` trait to provide consistent
/// error reporting using Ariadne's rich diagnostic format.
#[enum_dispatch(Diagnose)]
#[derive(Debug)]
pub enum CompileError {
    /// Errors occurring during the lexing/tokenization phase
    LexerError(LexerError),

    /// Errors occurring during the parsing/syntax analysis phase
    ParserError(ParserError),

    /// Errors occurring during the semantic analysis phase
    SemanticError(SemanticError),

    /// Errors occurring during the type analysis phase
    TypeError(TypeError),
}
