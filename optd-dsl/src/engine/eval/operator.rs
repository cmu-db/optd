use crate::{
    analyzer::hir::{CoreData, Expr, Materializable, Operator, OperatorKind, Value},
    capture,
    engine::utils::streams::{evaluate_all_combinations, stream_from_result, ValueStream},
    utils::context::Context,
};
use futures::StreamExt;

use CoreData::*;
use Materializable::*;

/// Evaluates an operator by generating all possible combinations of its components.
///
/// This function handles three types of components that an operator might have:
/// 1. Operator data - Required base parameters for the operator
/// 2. Relational children - Optional sub-expressions that produce relations (e.g., table operations)
/// 3. Scalar children - Required sub-expressions that produce scalar values
///
/// The function generates a cartesian product of all possible values for each component,
/// then constructs operator instances for each combination.
pub(super) fn evaluate_operator(op: Operator<Expr>, context: Context) -> ValueStream {
    // Start the evaluation process by exploring all operator data combinations
    explore_operator_data(
        op.operator_data,
        op.scalar_children,
        op.relational_children,
        op.kind,
        op.tag,
        context,
    )
}

/// Evaluates all combinations of operator data values.
fn explore_operator_data(
    op_data_exprs: Vec<Expr>,
    scalar_exprs: Vec<Expr>,
    rel_exprs: Vec<Expr>,
    kind: OperatorKind,
    tag: String,
    context: Context,
) -> ValueStream {
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
    scalar_exprs: Vec<Expr>,
    rel_exprs: Vec<Expr>,
    op_data: Vec<Value>,
    kind: OperatorKind,
    tag: String,
    context: Context,
) -> ValueStream {
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
    rel_exprs: Vec<Expr>,
    op_data: Vec<Value>,
    scalar_children: Vec<Value>,
    kind: OperatorKind,
    tag: String,
    context: Context,
) -> ValueStream {
    evaluate_all_combinations(rel_exprs.into_iter(), context.clone())
        .map(capture!(
            [kind, tag, op_data, scalar_children],
            move |rel_result| {
                rel_result.map(|rel_children| {
                    Value(Operator(Data(Operator {
                        kind,
                        tag: tag.clone(),
                        operator_data: op_data.clone(),
                        relational_children: rel_children,
                        scalar_children: scalar_children.clone(),
                    })))
                })
            }
        ))
        .boxed()
}
