pub mod analyzer;
pub mod catalog;
pub mod compile;
pub mod engine;
pub mod lexer;
pub mod parser;
pub mod utils;

pub use catalog::Catalog;

/// A macro that automatically clones variables before they're captured by a closure.
///
/// This macro takes a list of variables to clone and a closure expression, cloning the variables in
/// the list so that the clones can be moved into the closure.
#[macro_export]
macro_rules! capture {
    ([$($var:ident),* $(,)?], $($closure:expr)*) => {
        {
            $(let $var = $var.clone();)*
            $($closure)*
        }
    };
}
