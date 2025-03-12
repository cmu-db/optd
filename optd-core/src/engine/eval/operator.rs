//! This module provides evaluation functionality for operator expressions.
//!
//! Operators represent computational units that can have various components:
//! - Operator data: Basic parameters needed for the operator's function
//! - Children: Expressions that provide input data to the operator
//!
//! The evaluation process generates all possible combinations of component values
//! (a cartesian product) and constructs operator instances for each combination.
//! This approach handles the non-deterministic nature of expression evaluation
//! in our system, where an expression might evaluate to multiple possible values.

use crate::{
    capture,
    engine::{
        utils::streams::{
            evaluate_all_combinations, propagate_success, stream_from_result, ValueStream,
        },
        Engine, Expander,
    },
    error::Error,
};
use futures::{Stream, StreamExt};
use optd_dsl::analyzer::hir::{
    CoreData, Expr, LogicalOp, Materializable, Operator, PhysicalOp, Value,
};
use std::{pin::Pin, sync::Arc};
use CoreData::{Logical, Physical};
use Materializable::*;

/// A stream of evaluated operators
type OperatorStream = Pin<Box<dyn Stream<Item = Result<Operator<Value>, Error>> + Send>>;

//=============================================================================
// Entry points for different operator types
//=============================================================================

/// Evaluates a logical operator by generating all possible combinations of its components.
pub(super) fn evaluate_logical_operator<E>(
    op: LogicalOp<Arc<Expr>>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    match op.0 {
        UnMaterialized(group_id) => {
            propagate_success(Value(Logical(LogicalOp(UnMaterialized(group_id)))))
        }
        Materialized(op) => explore_operator_data(op.data, op.children, op.tag, engine)
            .map(|result| result.map(|op| Value(Logical(LogicalOp(Materialized(op))))))
            .boxed(),
    }
}

/// Evaluates a physical operator by generating all possible combinations of its components.
pub(super) fn evaluate_physical_operator<E>(
    op: PhysicalOp<Arc<Expr>>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    match op.0 {
        UnMaterialized(physical_goal) => {
            propagate_success(Value(Physical(PhysicalOp(UnMaterialized(physical_goal)))))
        }
        Materialized(op) => explore_operator_data(op.data, op.children, op.tag, engine)
            .map(|result| result.map(|op| Value(Physical(PhysicalOp(Materialized(op))))))
            .boxed(),
    }
}

//=============================================================================
// Shared implementation for operator evaluation
//=============================================================================

/// Evaluates all combinations of operator data values.
fn explore_operator_data<E>(
    op_data_exprs: Vec<Arc<Expr>>,
    children_exprs: Vec<Arc<Expr>>,
    tag: String,
    engine: Engine<E>,
) -> OperatorStream
where
    E: Expander,
{
    evaluate_all_combinations(op_data_exprs.into_iter(), engine.clone())
        .flat_map(move |op_data_result| {
            stream_from_result(
                op_data_result,
                capture!([tag, children_exprs, engine], move |op_data| {
                    explore_children(children_exprs, op_data, tag, engine)
                }),
            )
        })
        .boxed()
}

/// Evaluates all combinations of children values.
fn explore_children<E>(
    children: Vec<Arc<Expr>>,
    op_data: Vec<Value>,
    tag: String,
    engine: Engine<E>,
) -> OperatorStream
where
    E: Expander,
{
    evaluate_all_combinations(children.into_iter(), engine)
        .map(capture!([tag, op_data], move |rel_result| {
            rel_result.map(|children| Operator {
                tag: tag.clone(),
                data: op_data.clone(),
                children,
            })
        }))
        .boxed()
}
