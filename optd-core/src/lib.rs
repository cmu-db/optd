//! TODO Add docs. We will likely want to add a `#![doc = include_str!("../README.md")]` here.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc)]
#![warn(clippy::missing_panics_doc)]
#![warn(clippy::missing_safety_doc)]

pub mod expression;
pub mod memo;
pub mod operator;
pub mod plan;
pub mod rules;
pub mod storage;
