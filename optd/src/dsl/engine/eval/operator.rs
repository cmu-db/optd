use crate::capture;
use crate::dsl::analyzer::hir::{
    CoreData, Expr, Goal, GroupId, LogicalOp, Materializable, Operator, PhysicalOp, Value,
};
use crate::dsl::engine::{Continuation, Engine, EngineResponse};
use CoreData::{Logical, Physical};
use Materializable::*;
use std::sync::Arc;

impl<O: Clone + Send + 'static> Engine<O> {
    /// Evaluates a logical operator by generating all possible combinations of its components.
    ///
    /// # Parameters
    ///
    /// * `op` - The logical operator to evaluate.
    /// * `k` - The continuation to receive evaluation results.
    pub(crate) async fn evaluate_logical_operator(
        self,
        op: Materializable<LogicalOp<Arc<Expr>>, GroupId>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        match op {
            // For unmaterialized operators, directly call the continuation.
            UnMaterialized(group_id) => {
                let result = Value::new(Logical(UnMaterialized(group_id)));
                k(result).await
            }
            // For materialized operators, evaluate all parts and construct the result.
            Materialized(log_op) => {
                self.evaluate_operator(
                    log_op.operator.data,
                    log_op.operator.children,
                    log_op.operator.tag,
                    Arc::new(move |constructed_op| {
                        Box::pin(capture!([k], async move {
                            // Wrap the constructed operator in the logical operator structure
                            let log_op = LogicalOp::logical(constructed_op);
                            let result = Value::new(Logical(Materialized(log_op)));
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
    /// * `k` - The continuation to receive evaluation results.
    pub(crate) async fn evaluate_physical_operator(
        self,
        op: Materializable<PhysicalOp<Arc<Expr>>, Goal>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        match op {
            // For unmaterialized operators, continue with the unmaterialized value.
            UnMaterialized(physical_goal) => {
                let result = Value::new(Physical(UnMaterialized(physical_goal)));
                k(result).await
            }
            // For materialized operators, evaluate all parts and construct the result.
            Materialized(phys_op) => {
                self.evaluate_operator(
                    phys_op.operator.data,
                    phys_op.operator.children,
                    phys_op.operator.tag,
                    Arc::new(move |constructed_op| {
                        Box::pin(capture!([k], async move {
                            let phys_op = PhysicalOp::physical(constructed_op);
                            let result = Value::new(Physical(Materialized(phys_op)));
                            k(result).await
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
    /// * `k` - The continuation to receive the constructed operator.
    async fn evaluate_operator(
        self,
        op_data_exprs: Vec<Arc<Expr>>,
        children_exprs: Vec<Arc<Expr>>,
        tag: String,
        k: Continuation<Operator<Value>, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        let engine = self.clone();

        // First evaluate all operator data parameters.
        self.evaluate_sequence(
            op_data_exprs,
            Arc::new(move |op_data| {
                Box::pin(capture!([children_exprs, engine, tag, k], async move {
                    // Then evaluate all children expressions.
                    engine
                        .evaluate_sequence(
                            children_exprs,
                            Arc::new(move |children| {
                                Box::pin(capture!([tag, op_data, k], async move {
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
}

#[cfg(test)]
mod tests {
    use crate::catalog::iceberg::memory_catalog;
    use crate::dsl::analyzer::hir::context::Context;
    use crate::dsl::engine::Engine;
    use crate::dsl::utils::retriever::MockRetriever;
    use crate::dsl::{
        analyzer::hir::{
            BinOp, CoreData, Expr, ExprKind, Goal, GroupId, Literal, LogicalOp, Materializable,
            Operator, PhysicalOp, Value,
        },
        utils::tests::{
            TestHarness, create_logical_operator, evaluate_and_collect, int, lit_expr, lit_val,
            string,
        },
    };
    use ExprKind::*;
    use std::sync::Arc;

    /// Test evaluation of a materialized logical operator
    #[tokio::test]
    async fn test_materialized_logical_operator() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = Arc::new(memory_catalog());
        let retriever = Arc::new(MockRetriever::new());
        let engine = Engine::new(ctx, catalog, retriever);

        // Create a materialized logical operator expression with nested expressions to evaluate
        let log_op = LogicalOp::logical(Operator {
            tag: "Join".to_string(),
            data: vec![
                lit_expr(string("inner")),
                Arc::new(Expr::new(Binary(
                    lit_expr(int(10)),
                    BinOp::Add,
                    lit_expr(int(5)),
                ))),
            ],
            children: vec![
                Arc::new(Expr::new(CoreExpr(CoreData::Literal(string("orders"))))),
                Arc::new(Expr::new(CoreExpr(CoreData::Literal(string("lineitem"))))),
            ],
        });

        // Create the expression to evaluate
        let logical_op_expr = Arc::new(Expr::new(CoreExpr(CoreData::Logical(
            Materializable::Materialized(log_op),
        ))));

        // Evaluate the expression
        let results = evaluate_and_collect(logical_op_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Logical(Materializable::Materialized(log_op)) => {
                // Check tag
                assert_eq!(log_op.operator.tag, "Join");

                // Check data - should have "inner" and 15
                assert_eq!(log_op.operator.data.len(), 2);
                match &log_op.operator.data[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("inner".to_string()));
                    }
                    _ => panic!("Expected string literal"),
                }
                match &log_op.operator.data[1].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::Int64(15));
                    }
                    _ => panic!("Expected integer literal"),
                }

                // Check children - should have "orders" and "lineitem"
                assert_eq!(log_op.operator.children.len(), 2);
                match &log_op.operator.children[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("orders".to_string()));
                    }
                    _ => panic!("Expected string literal"),
                }
                match &log_op.operator.children[1].data {
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
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = Arc::new(memory_catalog());
        let retriever = Arc::new(MockRetriever::new());
        let engine = Engine::new(ctx, catalog, retriever);

        // Create an unmaterialized logical operator with a group ID
        let group_id = GroupId(42);

        // Create the expression to evaluate
        let logical_op_expr = Arc::new(Expr::new(CoreExpr(CoreData::Logical(
            Materializable::UnMaterialized(group_id),
        ))));

        // Evaluate the expression
        let results = evaluate_and_collect(logical_op_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Logical(Materializable::UnMaterialized(id)) => {
                // Check that the group ID is preserved
                assert_eq!(*id, group_id);
            }
            _ => panic!("Expected unmaterialized logical operator"),
        }
    }

    /// Test evaluation of a materialized physical operator
    #[tokio::test]
    async fn test_materialized_physical_operator() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = Arc::new(memory_catalog());
        let retriever = Arc::new(MockRetriever::new());
        let engine = Engine::new(ctx, catalog, retriever);

        // Create a materialized physical operator expression with nested expressions to evaluate
        let phys_op = PhysicalOp::physical(Operator {
            tag: "HashJoin".to_string(),
            data: vec![
                lit_expr(string("inner")),
                Arc::new(Expr::new(Binary(
                    lit_expr(int(20)),
                    BinOp::Mul,
                    lit_expr(int(3)),
                ))),
            ],
            children: vec![
                Arc::new(Expr::new(CoreExpr(CoreData::Literal(string("IndexScan"))))),
                Arc::new(Expr::new(CoreExpr(CoreData::Literal(string("FullScan"))))),
            ],
        });

        // Create the expression to evaluate
        let physical_op_expr = Arc::new(Expr::new(CoreExpr(CoreData::Physical(
            Materializable::Materialized(phys_op),
        ))));

        // Evaluate the expression
        let results = evaluate_and_collect(physical_op_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Physical(Materializable::Materialized(phys_op)) => {
                // Check tag
                assert_eq!(phys_op.operator.tag, "HashJoin");

                // Check data - should have "inner" and 60
                assert_eq!(phys_op.operator.data.len(), 2);
                match &phys_op.operator.data[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("inner".to_string()));
                    }
                    _ => panic!("Expected string literal"),
                }
                match &phys_op.operator.data[1].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::Int64(60));
                    }
                    _ => panic!("Expected integer literal"),
                }

                // Check children - should have "IndexScan" and "FullScan"
                assert_eq!(phys_op.operator.children.len(), 2);
                match &phys_op.operator.children[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("IndexScan".to_string()));
                    }
                    _ => panic!("Expected string literal"),
                }
                match &phys_op.operator.children[1].data {
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
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = Arc::new(memory_catalog());
        let retriever = Arc::new(MockRetriever::new());
        let engine = Engine::new(ctx, catalog, retriever);

        // Create an unmaterialized physical operator with a goal
        let goal = Goal {
            group_id: GroupId(42),
            properties: Box::new(Value::new(CoreData::Literal(Literal::String(
                "sorted".to_string(),
            )))),
        };

        // Create the expression to evaluate
        let physical_op_expr = Arc::new(Expr::new(CoreExpr(CoreData::Physical(
            Materializable::UnMaterialized(goal.clone()),
        ))));

        // Evaluate the expression
        let results = evaluate_and_collect(physical_op_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Physical(Materializable::UnMaterialized(result_goal)) => {
                // Check that the goal is preserved
                assert_eq!(result_goal.group_id, goal.group_id);

                // Check properties
                match &result_goal.properties.data {
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
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = Arc::new(memory_catalog());
        let retriever = Arc::new(MockRetriever::new());
        let engine = Engine::new(ctx, catalog, retriever);

        // Create a Join operator with two Scan operators as children
        let scan1 = Arc::new(Expr::new(CoreVal(create_logical_operator(
            "Scan",
            vec![lit_val(string("orders"))],
            vec![],
        ))));

        let scan2 = Arc::new(Expr::new(CoreVal(create_logical_operator(
            "Scan",
            vec![lit_val(string("lineitem"))],
            vec![],
        ))));

        let log_op = LogicalOp::logical(Operator {
            tag: "Join".to_string(),
            data: vec![lit_expr(string("inner"))],
            children: vec![scan1, scan2],
        });

        // Create the expression to evaluate
        let logical_op_expr = Arc::new(Expr::new(CoreExpr(CoreData::Logical(
            Materializable::Materialized(log_op),
        ))));

        // Evaluate the expression
        let results = evaluate_and_collect(logical_op_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Logical(Materializable::Materialized(log_op)) => {
                // Check tag
                assert_eq!(log_op.operator.tag, "Join");

                // Check data
                assert_eq!(log_op.operator.data.len(), 1);
                match &log_op.operator.data[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("inner".to_string()));
                    }
                    _ => panic!("Expected string literal"),
                }

                // Check children - should be two logical operators
                assert_eq!(log_op.operator.children.len(), 2);

                // Check first child
                match &log_op.operator.children[0].data {
                    CoreData::Logical(Materializable::Materialized(child_log_op)) => {
                        assert_eq!(child_log_op.operator.tag, "Scan");
                        assert_eq!(child_log_op.operator.data.len(), 1);
                        match &child_log_op.operator.data[0].data {
                            CoreData::Literal(lit) => {
                                assert_eq!(lit, &Literal::String("orders".to_string()));
                            }
                            _ => panic!("Expected string literal"),
                        }
                    }
                    _ => panic!("Expected materialized logical operator as child"),
                }

                // Check second child
                match &log_op.operator.children[1].data {
                    CoreData::Logical(Materializable::Materialized(child_log_op)) => {
                        assert_eq!(child_log_op.operator.tag, "Scan");
                        assert_eq!(child_log_op.operator.data.len(), 1);
                        match &child_log_op.operator.data[0].data {
                            CoreData::Literal(lit) => {
                                assert_eq!(lit, &Literal::String("lineitem".to_string()));
                            }
                            _ => panic!("Expected string literal"),
                        }
                    }
                    _ => panic!("Expected materialized logical operator as child"),
                }
            }
            _ => panic!("Expected materialized logical operator"),
        }
    }
}
