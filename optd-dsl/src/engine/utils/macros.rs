//! Provides utilities for managing variable capture in closures, particularly focused
//! on simplifying the common pattern of cloning variables before capture to avoid ownership
//! issues in complex nested closures and asynchronous contexts.

/// A macro that automatically clones variables before they're captured by a closure.
///
/// This macro takes a list of variables to clone and a closure expression.
/// It clones all the specified variables *before* creating the closure,
/// so the closure captures the clones rather than the originals.
///
/// # Examples
///
/// ```ignore
/// let tag = String::from("tag");
/// let context = Context::new();
///
/// // Without the macro, you would manually clone before the closure:
/// let tag_clone = tag.clone();
/// let context_clone = context.clone();
/// some_function(move |x| {
///     // Use tag_clone and context_clone
///     process(tag_clone, context_clone, x);
/// });
///
/// // With the capture! macro:
/// some_function(capture!([tag, context], move |x| {
///     // The closure receives already-cloned variables
///     process(tag, context, x);
/// }));
/// ```
///
/// This is particularly useful in async code with many nested closures
/// that need to capture the same variables.
#[macro_export]
macro_rules! capture {
    ([$($var:ident),* $(,)?], $($closure:tt)*) => {
        {
            $(let $var = $var.clone();)*
            $($closure)*
        }
    };
}
