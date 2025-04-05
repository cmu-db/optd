/// Span module for source code location tracking
///
/// This module provides span functionality for tracking the location of
/// elements in source code. It integrates with:
/// - Chumsky: For parser error reporting during syntax analysis
/// - Ariadne: For rich, user-friendly error diagnostics with code context
///
/// The span system connects AST nodes with their original source locations,
/// enabling precise error messages during compilation.
use core::fmt;
use std::ops::{Deref, Range};

/// A location span in source code that tracks the file and character range.
///
/// Used to connect AST nodes with their original location in the source code,
/// enabling accurate error reporting.
#[derive(Clone, PartialEq, Eq)]
pub struct Span {
    /// The source file path or identifier
    pub src_file: String,

    /// The range of character positions (start, end) in the source file
    pub range: (usize, usize),
}

impl Span {
    /// Creates a new span from a source file and character range.
    pub fn new(src_file: String, range: Range<usize>) -> Self {
        Self {
            src_file,
            range: (range.start, range.end),
        }
    }
}

/// A value annotated with its source code location.
///
/// This type enriches AST nodes with location information, which is essential
/// for providing meaningful error messages during compilation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Spanned<T> {
    /// The wrapped value
    pub value: Box<T>,

    /// The source location of the value
    pub span: Span,
}

impl<T> Spanned<T> {
    /// Creates a new spanned value.
    pub fn new(value: T, span: Span) -> Self {
        Self {
            value: Box::new(value),
            span,
        }
    }
}

impl<T> Deref for Spanned<T> {
    type Target = T;

    /// Dereferences the spanned value to access the underlying value.
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl fmt::Debug for Span {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}:{:?}", self.src_file, self.range)
    }
}

impl chumsky::Span for Span {
    type Context = String;
    type Offset = usize;
    fn new(src_file: String, range: Range<usize>) -> Self {
        Self {
            src_file,
            range: (range.start, range.end),
        }
    }
    fn context(&self) -> String {
        self.src_file.clone()
    }
    fn start(&self) -> Self::Offset {
        self.range.0
    }
    fn end(&self) -> Self::Offset {
        self.range.1
    }
}

impl ariadne::Span for Span {
    type SourceId = String;
    fn source(&self) -> &String {
        &self.src_file
    }
    fn start(&self) -> usize {
        self.range.0
    }
    fn end(&self) -> usize {
        self.range.1
    }
}
