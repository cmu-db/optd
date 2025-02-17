use core::fmt;
use std::ops::Range;

#[derive(Clone, PartialEq, Eq)]
pub struct Span {
    pub src_file: String,
    pub range: (usize, usize),
}

impl Span {
    pub fn new(src_file: String, range: Range<usize>) -> Self {
        Self {
            src_file,
            range: (range.start, range.end),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Spanned<T> {
    pub value: Box<T>,
    pub span: Span,
}

impl<T> Spanned<T> {
    pub fn new(value: T, span: Span) -> Self {
        Self {
            value: Box::new(value),
            span,
        }
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
