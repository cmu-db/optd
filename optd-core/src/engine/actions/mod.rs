//! Actions for the OPTD optimizer's execution engine.
//!
//! This module defines two key types of actions that can be performed during optimization:
//! - Analyzers: Extract information from plans into user-defined types
//! - Transformers: Transform plans into new equivalent plans
//!
//! Both types of actions use pattern matching and composition, but differ in their outputs.

pub mod analyzers; // Analysis actions that produce user-defined types
pub mod transformers; // Transformation actions that produce new plans

/// A binding for an action's result to a name.
///
/// Used in rule composition to:
/// - Give names to intermediate results
/// - Make results available to subsequent actions
/// - Reference results in output expressions
///
/// The String is the binding name, and T is the type of action being bound
/// (either an analyzer or transformer).
type BindAs<T> = (String, Box<T>);
