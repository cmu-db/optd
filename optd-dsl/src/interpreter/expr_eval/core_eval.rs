use crate::{
    analyzer::hir::{CoreData, Expr, LogicalOp, Materializable, PhysicalOp, ScalarOp, Value},
    interpreter::Context,
};
use optd_core::cascades::memo::Memoize;
use Materializable::*;

use super::evaluate_all_combinations;

/// Evaluates an operator by generating all possible combinations of its components.
///
/// This function handles three types of components that an operator might have:
/// 1. Operator data - Required base parameters for the operator
/// 2. Relational children - Optional sub-expressions that produce relations (e.g., table operations)
/// 3. Scalar children - Required sub-expressions that produce scalar values
///
/// The function generates a cartesian product of all possible values for each component,
/// then constructs operator instances for each combination using the provided value_constructor.
fn evaluate_operator<M: Memoize, T>(
    tag: &str,
    operator_data: &[Expr],
    relational_children: Option<&[Expr]>,
    scalar_children: &[Expr],
    value_constructor: fn(String, Vec<Value>, Option<Vec<Value>>, Vec<Value>) -> T,
    context: &mut Context,
    memo: &M,
) -> Vec<Value>
where
    T: Clone,
    Value: From<T>,
{
    let operator_data_values = evaluate_all_combinations(operator_data.iter(), context, memo);
    let scalar_values = evaluate_all_combinations(scalar_children.iter(), context, memo);
    let relational_values = relational_children
        .map(|rel_children| evaluate_all_combinations(rel_children.iter(), context, memo));

    operator_data_values
        .into_iter()
        .flat_map(|op_data| match &relational_values {
            Some(rel_combinations) => rel_combinations
                .iter()
                .flat_map(|rel_children| {
                    scalar_values.iter().map(|scalar_children| {
                        Value::from((value_constructor)(
                            tag.to_string(),
                            op_data.clone(),
                            Some(rel_children.clone()),
                            scalar_children.clone(),
                        ))
                    })
                })
                .collect::<Vec<_>>(),
            None => scalar_values
                .iter()
                .map(|scalar_children| {
                    Value::from((value_constructor)(
                        tag.to_string(),
                        op_data.clone(),
                        None,
                        scalar_children.clone(),
                    ))
                })
                .collect(),
        })
        .collect()
}

pub(super) fn evaluate_core_expr<M: Memoize>(
    data: &CoreData<Expr>,
    context: &mut Context,
    memo: &M,
) -> Vec<Value> {
    use CoreData::*;
    match data {
        Literal(lit) => vec![Value(Literal(lit.clone()))],

        Array(items) | Tuple(items) | Struct(_, items) => {
            evaluate_all_combinations(items.iter(), context, memo)
                .into_iter()
                .map(|items| match data {
                    Array(_) => Value(Array(items)),
                    Tuple(_) => Value(Tuple(items)),
                    Struct(name, _) => Value(Struct(name.clone(), items)),
                    _ => unreachable!(),
                })
                .collect()
        }

        Map(items) => {
            let key_comb = evaluate_all_combinations(items.iter().map(|(k, _)| k), context, memo);
            let value_comb = evaluate_all_combinations(items.iter().map(|(_, v)| v), context, memo);

            key_comb
                .into_iter()
                .flat_map(|keys| {
                    value_comb.iter().map(move |values| {
                        Value(Map(keys.clone().into_iter().zip(values.clone()).collect()))
                    })
                })
                .collect()
        }

        Function(fun_type) => vec![Value(Function(fun_type.clone()))],

        Fail(msg) => msg
            .evaluate(context, memo)
            .into_iter()
            .map(|m| Value(Fail(Box::new(m))))
            .collect(),

        Logical(Data(logical_op)) => evaluate_operator(
            &logical_op.tag,
            &logical_op.operator_data,
            Some(&logical_op.relational_children),
            &logical_op.scalar_children,
            |tag, op_data, rel_children, scalar_children| {
                Value(Logical(Data(LogicalOp {
                    tag,
                    operator_data: op_data,
                    relational_children: rel_children.unwrap(),
                    scalar_children,
                })))
            },
            context,
            memo,
        ),

        Scalar(Data(scalar_op)) => evaluate_operator(
            &scalar_op.tag,
            &scalar_op.operator_data,
            None,
            &scalar_op.scalar_children,
            |tag, op_data, _rel_children, scalar_children| {
                Value(Scalar(Data(ScalarOp {
                    tag,
                    operator_data: op_data,
                    scalar_children,
                })))
            },
            context,
            memo,
        ),

        Physical(Data(physical_op)) => evaluate_operator(
            &physical_op.tag,
            &physical_op.operator_data,
            Some(&physical_op.relational_children),
            &physical_op.scalar_children,
            |tag, op_data, rel_children, scalar_children| {
                Value(Physical(Materializable::Data(PhysicalOp {
                    tag,
                    operator_data: op_data,
                    relational_children: rel_children.unwrap(),
                    scalar_children,
                })))
            },
            context,
            memo,
        ),

        // TODO: Memo table call.
        &CoreData::Logical(Materializable::Group(_))
        | &CoreData::Scalar(Materializable::Group(_))
        | &CoreData::Physical(Materializable::Group(_)) => todo!(),
    }
}
