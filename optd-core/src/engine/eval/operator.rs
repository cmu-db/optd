//! This module provides evaluation functionality for operator expressions.
//!
//! Operators represent computational units that can have various components:
//! - Operator data: Basic parameters needed for the operator's function
//! - Scalar children: Expressions that produce scalar values
//! - Relational children: Expressions that produce relations (like table operations)
//!
//! The evaluation process generates all possible combinations of component values
//! (a cartesian product) and constructs operator instances for each combination.
//! This approach handles the non-deterministic nature of expression evaluation
//! in our system, where an expression might evaluate to multiple possible values.

use std::{pin::Pin, sync::Arc};

use crate::{
    capture,
    engine::utils::{
        error::Error,
        streams::{evaluate_all_combinations, propagate_success, stream_from_result, ValueStream},
    },
};
use futures::{Stream, StreamExt};
use optd_dsl::analyzer::{
    context::Context,
    hir::{
        CoreData, Expr, LogicalOp, Materializable, Operator, OperatorKind, PhysicalOp, ScalarOp,
        Value,
    },
};
use CoreData::{Logical, Physical, Scalar};
use Materializable::*;

/// A stream of evaluated operators
type OperatorStream = Pin<Box<dyn Stream<Item = Result<Operator<Value>, Error>> + Send>>;

//=============================================================================
// Entry points for different operator types
//=============================================================================

/// Evaluates a logical operator by generating all possible combinations of its components.
pub(super) fn evaluate_logical_operator(op: LogicalOp<Arc<Expr>>, context: Context) -> ValueStream {
    match op.0 {
        UnMaterialized(group_id) => {
            propagate_success(Value(Logical(LogicalOp(UnMaterialized(group_id)))))
        }
        Materialized(op) => evaluate_operator_components(op, context)
            .map(|result| result.map(|op| Value(Logical(LogicalOp(Materialized(op))))))
            .boxed(),
    }
}

/// Evaluates a scalar operator by generating all possible combinations of its components.
pub(super) fn evaluate_scalar_operator(op: ScalarOp<Arc<Expr>>, context: Context) -> ValueStream {
    match op.0 {
        UnMaterialized(group_id) => {
            propagate_success(Value(Scalar(ScalarOp(UnMaterialized(group_id)))))
        }
        Materialized(op) => evaluate_operator_components(op, context)
            .map(|result| result.map(|op| Value(Scalar(ScalarOp(Materialized(op))))))
            .boxed(),
    }
}

/// Evaluates a physical operator by generating all possible combinations of its components.
pub(super) fn evaluate_physical_operator(
    op: PhysicalOp<Arc<Expr>>,
    context: Context,
) -> ValueStream {
    match op.0 {
        UnMaterialized(physical_goal) => {
            propagate_success(Value(Physical(PhysicalOp(UnMaterialized(physical_goal)))))
        }
        Materialized(op) => evaluate_operator_components(op, context)
            .map(|result| result.map(|op| Value(Physical(PhysicalOp(Materialized(op))))))
            .boxed(),
    }
}

//=============================================================================
// Shared implementation for operator evaluation
//=============================================================================

/// Evaluates the components of an operator (data and children).
///
/// This is a shared implementation that handles the common pattern of
/// evaluating operator data, scalar children, and relational children
/// for any type of operator.
fn evaluate_operator_components(op: Operator<Arc<Expr>>, context: Context) -> OperatorStream {
    let kind = op.kind;

    explore_operator_data(
        op.operator_data,
        op.scalar_children,
        op.relational_children,
        kind,
        op.tag,
        context,
    )
}

/// Evaluates all combinations of operator data values.
fn explore_operator_data(
    op_data_exprs: Vec<Arc<Expr>>,
    scalar_exprs: Vec<Arc<Expr>>,
    rel_exprs: Vec<Arc<Expr>>,
    kind: OperatorKind,
    tag: String,
    context: Context,
) -> OperatorStream {
    evaluate_all_combinations(op_data_exprs.into_iter(), context.clone())
        .flat_map(move |op_data_result| {
            stream_from_result(
                op_data_result,
                capture!(
                    [kind, tag, scalar_exprs, rel_exprs, context],
                    move |op_data| {
                        explore_scalar_children(
                            scalar_exprs,
                            rel_exprs,
                            op_data,
                            kind,
                            tag,
                            context,
                        )
                    }
                ),
            )
        })
        .boxed()
}

/// Evaluates all combinations of scalar children values.
fn explore_scalar_children(
    scalar_exprs: Vec<Arc<Expr>>,
    rel_exprs: Vec<Arc<Expr>>,
    op_data: Vec<Value>,
    kind: OperatorKind,
    tag: String,
    context: Context,
) -> OperatorStream {
    evaluate_all_combinations(scalar_exprs.into_iter(), context.clone())
        .flat_map(capture!(
            [kind, tag, op_data, rel_exprs, context],
            move |scalar_result| {
                stream_from_result(
                    scalar_result,
                    capture!(
                        [kind, tag, op_data, rel_exprs, context],
                        move |scalar_children| {
                            explore_relational_children(
                                rel_exprs,
                                op_data,
                                scalar_children,
                                kind,
                                tag,
                                context,
                            )
                        }
                    ),
                )
            }
        ))
        .boxed()
}

/// Evaluates all combinations of relational children values.
fn explore_relational_children(
    rel_exprs: Vec<Arc<Expr>>,
    op_data: Vec<Value>,
    scalar_children: Vec<Value>,
    kind: OperatorKind,
    tag: String,
    context: Context,
) -> OperatorStream {
    evaluate_all_combinations(rel_exprs.into_iter(), context.clone())
        .map(capture!(
            [kind, tag, op_data, scalar_children],
            move |rel_result| {
                rel_result.map(|rel_children| Operator {
                    kind,
                    tag: tag.clone(),
                    operator_data: op_data.clone(),
                    relational_children: rel_children,
                    scalar_children: scalar_children.clone(),
                })
            }
        ))
        .boxed()
}
