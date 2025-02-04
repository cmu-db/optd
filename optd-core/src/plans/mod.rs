//! Plan expression system for constructing partial plans.
//!
//! Provides a generic expression type for building both logical and scalar
//! plans during optimization, with control flow and reference capabilities.

use crate::values::OptdExpr;

pub mod logical;
pub mod scalar;

/// Expression type for constructing partial plans.
///
/// Generic over the type of plan being constructed (logical or scalar)
/// to provide a unified expression system for both plan types.
#[derive(Clone)]
pub enum PartialPlanExpr<Type> {
    /// Conditional plan construction
    IfThenElse {
        /// Condition (must be an OPTD expression)
        cond: Box<OptdExpr>,
        /// Plan to construct if condition is true
        then: Box<PartialPlanExpr<Type>>,
        /// Plan to construct if condition is false
        otherwise: Box<PartialPlanExpr<Type>>,
    },

    /// Reference to a bound plan
    Ref(String),

    /// Direct plan value
    Value(Type),
}
