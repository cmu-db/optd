use super::{Engine, Generator};
use crate::engine::generator::Continuation;
use crate::engine::utils::evaluate_sequence;
use crate::{capture, engine::utils::UnitFuture};
use optd_dsl::analyzer::hir::{
    CoreData, Expr, LogicalOp, Materializable, Operator, PhysicalOp, Value,
};
use std::sync::Arc;
use CoreData::{Logical, Physical};
use Materializable::*;

/// Specialized continuation type for operator values
type OperatorContinuation = Arc<dyn Fn(Operator<Value>) -> UnitFuture + Send + Sync + 'static>;

/// Evaluates a logical operator by generating all possible combinations of its components.
///
/// # Parameters
/// * `op` - The logical operator to evaluate
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) async fn evaluate_logical_operator<G>(
    op: LogicalOp<Arc<Expr>>,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
{
    match op.0 {
        // For unmaterialized operators, directly call the continuation with the unmaterialized value
        UnMaterialized(group_id) => {
            k(Value(Logical(LogicalOp(UnMaterialized(group_id))))).await;
        }
        // For materialized operators, evaluate all parts and construct the result
        Materialized(op) => {
            evaluate_operator(
                op.data,
                op.children,
                op.tag,
                engine,
                Arc::new(move |constructed_op| {
                    Box::pin(capture!([k], async move {
                        // Wrap the constructed operator in the logical operator structure
                        let result = Value(Logical(LogicalOp(Materialized(constructed_op))));
                        k(result).await;
                    }))
                }),
            )
            .await;
        }
    }
}

/// Evaluates a physical operator by generating all possible combinations of its components.
///
/// # Parameters
/// * `op` - The physical operator to evaluate
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) async fn evaluate_physical_operator<G>(
    op: PhysicalOp<Arc<Expr>>,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
{
    match op.0 {
        // For unmaterialized operators, continue with the unmaterialized value
        UnMaterialized(physical_goal) => {
            k(Value(Physical(PhysicalOp(UnMaterialized(physical_goal))))).await;
        }
        // For materialized operators, evaluate all parts and construct the result
        Materialized(op) => {
            evaluate_operator(
                op.data,
                op.children,
                op.tag,
                engine,
                Arc::new(move |constructed_op| {
                    Box::pin(capture!([k], async move {
                        k(Value(Physical(PhysicalOp(Materialized(constructed_op))))).await;
                    }))
                }),
            )
            .await;
        }
    }
}

/// Evaluates all components of an operator and constructs the final operator value.
///
/// Evaluates both operator data parameters and children expressions, combining
/// them to form complete operator instances.
///
/// # Parameters
/// * `op_data_exprs` - The operator parameter expressions to evaluate
/// * `children_exprs` - The child operator expressions to evaluate
/// * `tag` - The operator type tag
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive the constructed operator
async fn evaluate_operator<G>(
    op_data_exprs: Vec<Arc<Expr>>,
    children_exprs: Vec<Arc<Expr>>,
    tag: String,
    engine: Engine<G>,
    k: OperatorContinuation,
) where
    G: Generator,
{
    // First evaluate all operator data parameters
    evaluate_sequence(
        op_data_exprs,
        engine.clone(),
        Arc::new(move |op_data| {
            Box::pin(capture!([children_exprs, tag, engine, k], async move {
                // Then evaluate all children expressions
                evaluate_sequence(
                    children_exprs,
                    engine,
                    Arc::new(move |children| {
                        Box::pin(capture!([tag, op_data, k], async move {
                            // Construct the complete operator and pass to continuation
                            let operator = Operator {
                                tag,
                                data: op_data,
                                children,
                            };
                            k(operator).await;
                        }))
                    }),
                )
                .await;
            }))
        }),
    )
    .await;
}
