use crate::analyzer::hir::{
    CoreData, Expr, LogicalOp, Materializable, Operator, PhysicalOp, Value,
};
use crate::engine::utils::evaluate_sequence;
use crate::engine::{Continuation, EngineResponse};
use crate::{capture, engine::Engine};
use CoreData::{Logical, Physical};
use Materializable::*;
use std::sync::Arc;

/// Evaluates a logical operator by generating all possible combinations of its components.
///
/// # Parameters
///
/// * `op` - The logical operator to evaluate.
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
pub(crate) async fn evaluate_logical_operator(
    op: LogicalOp<Arc<Expr>>,
    engine: Engine,
    k: Continuation<Value, EngineResponse>,
) -> EngineResponse
where
{
    match op.0 {
        // For unmaterialized operators, directly call the continuation with the unmaterialized
        // value.
        UnMaterialized(group_id) => k(Value(Logical(LogicalOp(UnMaterialized(group_id))))).await,
        // For materialized operators, evaluate all parts and construct the result.
        Materialized(op) => {
            evaluate_operator(
                op.data,
                op.children,
                op.tag,
                engine,
                Arc::new(move |constructed_op| {
                    Box::pin(capture!([k], async move {
                        // Wrap the constructed operator in the logical operator structure.
                        let result = Value(Logical(LogicalOp(Materialized(constructed_op))));
                        k(result).await
                    }))
                }),
            )
            .await
        }
    }
}

/// Evaluates a physical operator by generating all possible combinations of its components.
///
/// # Parameters
///
/// * `op` - The physical operator to evaluate.
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
pub(crate) async fn evaluate_physical_operator(
    op: PhysicalOp<Arc<Expr>>,
    engine: Engine,
    k: Continuation<Value, EngineResponse>,
) -> EngineResponse
where
{
    match op.0 {
        // For unmaterialized operators, continue with the unmaterialized value.
        UnMaterialized(physical_goal) => {
            k(Value(Physical(PhysicalOp(UnMaterialized(physical_goal))))).await
        }
        // For materialized operators, evaluate all parts and construct the result.
        Materialized(op) => {
            evaluate_operator(
                op.data,
                op.children,
                op.tag,
                engine,
                Arc::new(move |constructed_op| {
                    Box::pin(capture!([k], async move {
                        k(Value(Physical(PhysicalOp(Materialized(constructed_op))))).await
                    }))
                }),
            )
            .await
        }
    }
}

/// Evaluates all components of an operator and constructs the final operator value.
///
/// Evaluates both operator data parameters and children expressions, combining them to form
/// complete operator instances.
///
/// # Parameters
///
/// * `op_data_exprs` - The operator parameter expressions to evaluate.
/// * `children_exprs` - The child operator expressions to evaluate.
/// * `tag` - The operator type tag.
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive the constructed operator.
async fn evaluate_operator(
    op_data_exprs: Vec<Arc<Expr>>,
    children_exprs: Vec<Arc<Expr>>,
    tag: String,
    engine: Engine,
    k: Continuation<Operator<Value>, EngineResponse>,
) -> EngineResponse
where
{
    // First evaluate all operator data parameters.
    evaluate_sequence(
        op_data_exprs,
        engine.clone(),
        Arc::new(move |op_data| {
            Box::pin(capture!([children_exprs, tag, engine, k], async move {
                // Then evaluate all children expressions.
                evaluate_sequence(
                    children_exprs,
                    engine,
                    Arc::new(move |children| {
                        Box::pin(capture!([tag, op_data, k], async move {
                            // Construct the complete operator and pass to continuation.
                            let operator = Operator {
                                tag,
                                data: op_data,
                                children,
                            };
                            k(operator).await
                        }))
                    }),
                )
                .await
            }))
        }),
    )
    .await
}

#[cfg(test)]
mod tests {
    use crate::analyzer::{
        context::Context,
        hir::{
            BinOp, CoreData, Expr, Goal, GroupId, Literal, LogicalOp, Materializable, Operator,
            PhysicalOp, Value,
        },
    };
    use crate::engine::{
        Engine,
        test_utils::{
            MockGenerator, create_logical_operator, evaluate_and_collect, int, lit_expr, lit_val,
            string,
        },
    };
    use std::sync::Arc;

    /// Test evaluation of a materialized logical operator
    #[tokio::test]
    async fn test_materialized_logical_operator() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a materialized logical operator expression with nested expressions to evaluate
        let op = LogicalOp(Materializable::Materialized(Operator {
            tag: "Join".to_string(),
            data: vec![
                lit_expr(string("inner")),
                Arc::new(Expr::Binary(
                    lit_expr(int(10)),
                    BinOp::Add,
                    lit_expr(int(5)),
                )),
            ],
            children: vec![
                Arc::new(Expr::CoreExpr(CoreData::Literal(string("orders")))),
                Arc::new(Expr::CoreExpr(CoreData::Literal(string("lineitem")))),
            ],
        }));

        // Create the expression to evaluate
        let logical_op_expr = Arc::new(Expr::CoreExpr(CoreData::Logical(op)));

        // Evaluate the expression
        let results = evaluate_and_collect(logical_op_expr, engine).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Logical(LogicalOp(Materializable::Materialized(op))) => {
                // Check tag
                assert_eq!(op.tag, "Join");

                // Check data - should have "inner" and 15
                assert_eq!(op.data.len(), 2);
                match &op.data[0].0 {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("inner".to_string()));
                    }
                    _ => panic!("Expected string literal"),
                }
                match &op.data[1].0 {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::Int64(15));
                    }
                    _ => panic!("Expected integer literal"),
                }

                // Check children - should have "orders" and "lineitem"
                assert_eq!(op.children.len(), 2);
                match &op.children[0].0 {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("orders".to_string()));
                    }
                    _ => panic!("Expected string literal"),
                }
                match &op.children[1].0 {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("lineitem".to_string()));
                    }
                    _ => panic!("Expected string literal"),
                }
            }
            _ => panic!("Expected materialized logical operator"),
        }
    }

    /// Test evaluation of an unmaterialized logical operator
    #[tokio::test]
    async fn test_unmaterialized_logical_operator() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create an unmaterialized logical operator with a group ID
        let group_id = GroupId(42);
        let op = LogicalOp(Materializable::UnMaterialized(group_id));

        // Create the expression to evaluate
        let logical_op_expr = Arc::new(Expr::CoreExpr(CoreData::Logical(op)));

        // Evaluate the expression
        let results = evaluate_and_collect(logical_op_expr, engine).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Logical(LogicalOp(Materializable::UnMaterialized(id))) => {
                // Check that the group ID is preserved
                assert_eq!(*id, group_id);
            }
            _ => panic!("Expected unmaterialized logical operator"),
        }
    }

    /// Test evaluation of a materialized physical operator
    #[tokio::test]
    async fn test_materialized_physical_operator() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a materialized physical operator expression with nested expressions to evaluate
        let op = PhysicalOp(Materializable::Materialized(Operator {
            tag: "HashJoin".to_string(),
            data: vec![
                lit_expr(string("inner")),
                Arc::new(Expr::Binary(
                    lit_expr(int(20)),
                    BinOp::Mul,
                    lit_expr(int(3)),
                )),
            ],
            children: vec![
                Arc::new(Expr::CoreExpr(CoreData::Literal(string("IndexScan")))),
                Arc::new(Expr::CoreExpr(CoreData::Literal(string("FullScan")))),
            ],
        }));

        // Create the expression to evaluate
        let physical_op_expr = Arc::new(Expr::CoreExpr(CoreData::Physical(op)));

        // Evaluate the expression
        let results = evaluate_and_collect(physical_op_expr, engine).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Physical(PhysicalOp(Materializable::Materialized(op))) => {
                // Check tag
                assert_eq!(op.tag, "HashJoin");

                // Check data - should have "inner" and 60
                assert_eq!(op.data.len(), 2);
                match &op.data[0].0 {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("inner".to_string()));
                    }
                    _ => panic!("Expected string literal"),
                }
                match &op.data[1].0 {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::Int64(60));
                    }
                    _ => panic!("Expected integer literal"),
                }

                // Check children - should have "IndexScan" and "FullScan"
                assert_eq!(op.children.len(), 2);
                match &op.children[0].0 {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("IndexScan".to_string()));
                    }
                    _ => panic!("Expected string literal"),
                }
                match &op.children[1].0 {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("FullScan".to_string()));
                    }
                    _ => panic!("Expected string literal"),
                }
            }
            _ => panic!("Expected materialized physical operator"),
        }
    }

    /// Test evaluation of an unmaterialized physical operator
    #[tokio::test]
    async fn test_unmaterialized_physical_operator() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create an unmaterialized physical operator with a goal
        let goal = Goal {
            group_id: GroupId(42),
            properties: Box::new(Value(CoreData::Literal(Literal::String(
                "sorted".to_string(),
            )))),
        };
        let op = PhysicalOp(Materializable::UnMaterialized(goal.clone()));

        // Create the expression to evaluate
        let physical_op_expr = Arc::new(Expr::CoreExpr(CoreData::Physical(op)));

        // Evaluate the expression
        let results = evaluate_and_collect(physical_op_expr, engine).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Physical(PhysicalOp(Materializable::UnMaterialized(result_goal))) => {
                // Check that the goal is preserved
                assert_eq!(result_goal.group_id, goal.group_id);

                // Check properties
                match &result_goal.properties.0 {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("sorted".to_string()));
                    }
                    _ => panic!("Expected string literal in goal properties"),
                }
            }
            _ => panic!("Expected unmaterialized physical operator"),
        }
    }

    /// Test evaluation of a logical operator with nested operators as children
    #[tokio::test]
    async fn test_nested_logical_operators() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a Join operator with two Scan operators as children
        let scan1 = Arc::new(Expr::CoreVal(create_logical_operator(
            "Scan",
            vec![lit_val(string("orders"))],
            vec![],
        )));

        let scan2 = Arc::new(Expr::CoreVal(create_logical_operator(
            "Scan",
            vec![lit_val(string("lineitem"))],
            vec![],
        )));

        let op = LogicalOp(Materializable::Materialized(Operator {
            tag: "Join".to_string(),
            data: vec![lit_expr(string("inner"))],
            children: vec![scan1, scan2],
        }));

        // Create the expression to evaluate
        let logical_op_expr = Arc::new(Expr::CoreExpr(CoreData::Logical(op)));

        // Evaluate the expression
        let results = evaluate_and_collect(logical_op_expr, engine).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Logical(LogicalOp(Materializable::Materialized(op))) => {
                // Check tag
                assert_eq!(op.tag, "Join");

                // Check data
                assert_eq!(op.data.len(), 1);
                match &op.data[0].0 {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("inner".to_string()));
                    }
                    _ => panic!("Expected string literal"),
                }

                // Check children - should be two logical operators
                assert_eq!(op.children.len(), 2);

                // Check first child
                match &op.children[0].0 {
                    CoreData::Logical(LogicalOp(Materializable::Materialized(child_op))) => {
                        assert_eq!(child_op.tag, "Scan");
                        assert_eq!(child_op.data.len(), 1);
                        match &child_op.data[0].0 {
                            CoreData::Literal(lit) => {
                                assert_eq!(lit, &Literal::String("orders".to_string()));
                            }
                            _ => panic!("Expected string literal"),
                        }
                    }
                    _ => panic!("Expected logical operator as child"),
                }

                // Check second child
                match &op.children[1].0 {
                    CoreData::Logical(LogicalOp(Materializable::Materialized(child_op))) => {
                        assert_eq!(child_op.tag, "Scan");
                        assert_eq!(child_op.data.len(), 1);
                        match &child_op.data[0].0 {
                            CoreData::Literal(lit) => {
                                assert_eq!(lit, &Literal::String("lineitem".to_string()));
                            }
                            _ => panic!("Expected string literal"),
                        }
                    }
                    _ => panic!("Expected logical operator as child"),
                }
            }
            _ => panic!("Expected materialized logical operator"),
        }
    }
}
