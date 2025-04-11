use crate::analyzer::hir::{CoreData, Expr, Value};
use crate::engine::{Continuation, EngineResponse};
use crate::{capture, engine::Engine};
use CoreData::*;
use std::sync::Arc;

impl<O: Clone + Send + 'static> Engine<O> {
    /// Evaluates a core expression.
    ///
    /// # Parameters
    ///
    /// * `data` - The core expression data to evaluate.
    /// * `k` - The continuation to receive evaluation results.
    pub(crate) async fn evaluate_core_expr(
        self,
        data: CoreData<Arc<Expr>>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        match data {
            Literal(lit) => {
                // Directly continue with the literal value.
                k(Value::new(Literal(lit))).await
            }
            Array(items) => self.evaluate_collection(items, Array, k).await,
            Tuple(items) => self.evaluate_collection(items, Tuple, k).await,
            Struct(name, items) => {
                self.evaluate_collection(items, move |values| Struct(name, values), k)
                    .await
            }
            Map(items) => {
                // Directly continue with the map value.
                k(Value::new(Map(items))).await
            }
            Function(fun_type) => {
                // Directly continue with the function value.
                k(Value::new(Function(fun_type))).await
            }
            Fail(msg) => self.evaluate_fail(*msg, k).await,
            Logical(op) => self.evaluate_logical_operator(op, k).await,
            Physical(op) => self.evaluate_physical_operator(op, k).await,
            None => {
                // Directly continue with null value.
                k(Value::new(None)).await
            }
        }
    }

    /// Evaluates a collection expression (Array, Tuple, or Struct).
    ///
    /// # Parameters
    ///
    /// * `items` - The collection items to evaluate.
    /// * `constructor` - Function to construct the appropriate collection type.
    /// * `k` - The continuation to receive evaluation results.
    async fn evaluate_collection<F>(
        self,
        items: Vec<Arc<Expr>>,
        constructor: F,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O>
    where
        F: FnOnce(Vec<Value>) -> CoreData<Value> + Clone + Send + Sync + 'static,
    {
        self.evaluate_sequence(
            items,
            Arc::new(move |values| {
                Box::pin(capture!([constructor, k], async move {
                    let result = Value::new(constructor(values));
                    k(result).await
                }))
            }),
        )
        .await
    }

    /// Evaluates a fail expression.
    ///
    /// # Parameters
    ///
    /// * `msg` - The message expression to evaluate
    /// * `k` - The continuation to receive evaluation results
    async fn evaluate_fail(
        self,
        msg: Arc<Expr>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        self.evaluate(
            msg,
            Arc::new(move |value| {
                Box::pin(capture!([k], async move {
                    k(Value::new(Fail(Box::new(value)))).await
                }))
            }),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::Continuation;
    use crate::utils::tests::{
        TestHarness, evaluate_and_collect, evaluate_and_collect_with_custom_k, int, lit_expr,
        string,
    };
    use crate::{
        analyzer::{
            context::Context,
            hir::{CoreData, Expr, ExprKind, FunKind, Literal, Value},
        },
        engine::Engine,
    };
    use ExprKind::*;
    use std::sync::Arc;

    /// Test evaluation of literal values
    #[tokio::test]
    async fn test_literal_evaluation() {
        let ctx = Context::default();
        let engine = Engine::new(ctx);
        let harness = TestHarness::new();

        // Create a literal expression
        let literal_expr = Arc::new(Expr::new(CoreExpr(CoreData::Literal(int(42)))));
        let results = evaluate_and_collect(literal_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 42);
            }
            _ => panic!("Expected integer literal"),
        }
    }

    /// Test evaluation of array expressions
    #[tokio::test]
    async fn test_array_evaluation() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create an array expression with values to evaluate
        let array_expr = Arc::new(Expr::new(CoreExpr(CoreData::Array(vec![
            lit_expr(int(1)),
            lit_expr(int(2)),
            lit_expr(int(3)),
        ]))));

        let results = evaluate_and_collect(array_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Array(elements) => {
                assert_eq!(elements.len(), 3);
                match &elements[0].data {
                    CoreData::Literal(Literal::Int64(value)) => assert_eq!(*value, 1),
                    _ => panic!("Expected integer literal"),
                }
                match &elements[1].data {
                    CoreData::Literal(Literal::Int64(value)) => assert_eq!(*value, 2),
                    _ => panic!("Expected integer literal"),
                }
                match &elements[2].data {
                    CoreData::Literal(Literal::Int64(value)) => assert_eq!(*value, 3),
                    _ => panic!("Expected integer literal"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    /// Test evaluation of tuple expressions
    #[tokio::test]
    async fn test_tuple_evaluation() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a tuple expression with mixed types
        let tuple_expr = Arc::new(Expr::new(CoreExpr(CoreData::Tuple(vec![
            lit_expr(int(42)),
            lit_expr(string("hello")),
            lit_expr(Literal::Bool(true)),
        ]))));

        let results = evaluate_and_collect(tuple_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Tuple(elements) => {
                assert_eq!(elements.len(), 3);
                match &elements[0].data {
                    CoreData::Literal(Literal::Int64(value)) => assert_eq!(*value, 42),
                    _ => panic!("Expected integer literal"),
                }
                match &elements[1].data {
                    CoreData::Literal(Literal::String(value)) => assert_eq!(value, "hello"),
                    _ => panic!("Expected string literal"),
                }
                match &elements[2].data {
                    CoreData::Literal(Literal::Bool(value)) => assert!(*value),
                    _ => panic!("Expected boolean literal"),
                }
            }
            _ => panic!("Expected tuple"),
        }
    }

    /// Test evaluation of struct expressions
    #[tokio::test]
    async fn test_struct_evaluation() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a struct expression
        let struct_expr = Arc::new(Expr::new(CoreExpr(CoreData::Struct(
            "Point".to_string(),
            vec![lit_expr(int(10)), lit_expr(int(20))],
        ))));

        let results = evaluate_and_collect(struct_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Struct(name, fields) => {
                assert_eq!(name, "Point");
                assert_eq!(fields.len(), 2);
                match &fields[0].data {
                    CoreData::Literal(Literal::Int64(value)) => assert_eq!(*value, 10),
                    _ => panic!("Expected integer literal"),
                }
                match &fields[1].data {
                    CoreData::Literal(Literal::Int64(value)) => assert_eq!(*value, 20),
                    _ => panic!("Expected integer literal"),
                }
            }
            _ => panic!("Expected struct"),
        }
    }

    /// Test evaluation of function expressions
    #[tokio::test]
    async fn test_function_evaluation() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a function expression (just a simple closure)
        let fn_expr = Arc::new(Expr::new(CoreExpr(CoreData::Function(FunKind::Closure(
            vec!["x".to_string()],
            lit_expr(int(42)), // Just returns 42 regardless of argument
        )))));

        let results = evaluate_and_collect(fn_expr, engine, harness).await;

        // Check that we got a function value
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Function(_) => {
                // Successfully evaluated to a function
            }
            _ => panic!("Expected function"),
        }
    }

    /// Test evaluation of null expressions
    #[tokio::test]
    async fn test_null_evaluation() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a null expression
        let null_expr = Arc::new(Expr::new(CoreExpr(CoreData::None)));

        let results = evaluate_and_collect(null_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::None => {
                // Successfully evaluated to null
            }
            _ => panic!("Expected null"),
        }
    }

    /// Test evaluation of fail expressions
    #[tokio::test]
    async fn test_fail_evaluation() {
        let return_k: Continuation<Value, Result<Value, String>> = Arc::new(move |value| {
            Box::pin(async move {
                match value.data {
                    CoreData::Fail(boxed_value) => match boxed_value.data {
                        CoreData::Literal(Literal::String(msg)) => Err(msg),
                        _ => panic!("Expected string message in fail"),
                    },
                    _ => Ok(value),
                }
            })
        });

        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a fail expression with a message
        let fail_expr = Arc::new(Expr::new(CoreExpr(CoreData::Fail(Box::new(Arc::new(
            Expr::new(CoreVal(Value::new(CoreData::Literal(string(
                "error message",
            ))))),
        ))))));

        let results =
            evaluate_and_collect_with_custom_k(fail_expr, engine, harness, return_k).await;

        // Check result
        assert_eq!(results.len(), 1);
        let error_msg = results[0].as_ref().unwrap_err().as_str();
        assert_eq!(error_msg, "error message");
    }
}
