//! Type representations of scalar operators in query plans and expressions.

pub mod add;
pub mod column_ref;
pub mod constant;

use add::Add;
use column_ref::ColumnRef;
use constant::Constant;

/// A type representing a scalar operator in a query plan.
///
/// Each variant represents a specific kind of scalar operation:
/// - `Add`: Addition operation
/// - `Uint`: Unsigned integer literal
/// - `ColumnRef`: Reference to input column
///
/// This type is generic over `ScalarLink` which specifies what kind of children
/// scalar expressions can have, enabling reuse in different kinds of DAGs.
/// 
/// Valid in [`LogicalPlan`], [`ScalarExpression`], [`PhysicalPlan`], [`PartialLogicalPlan`].
pub enum ScalarOperator<ScalarLink> {
    Add(Add<ScalarLink>),
    Uint(Constant<u64>),
    ColumnRef(ColumnRef),
}
