//! Definitions and implementations of components related to optimizer state, which we refer to
//! generally as the memo table.
//!
//! TODO(connor): Explain the distinction between the memo table and the other things that the
//! optimizer needs to store / remember (task graph state as well).

mod error;
mod traits;
mod types;

pub use error::*;
pub use traits::*;
pub use types::*;

/// A generic implementation of the Union-Find algorithm.
mod union_find;

/// In-memory implementation of the optimizer state (including the memo table).
mod memory;
