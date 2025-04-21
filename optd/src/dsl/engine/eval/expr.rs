use super::{binary::eval_binary_op, unary::eval_unary_op};
use crate::capture;
use crate::dsl::analyzer::hir::{
    BinOp, CoreData, Expr, ExprKind, FunKind, Goal, GroupId, Identifier, LetBinding, Literal,
    LogicalOp, Materializable, PhysicalOp, Udf, UnaryOp, Value,
};
use crate::dsl::analyzer::map::Map;
use crate::dsl::engine::{Continuation, Engine, EngineResponse};
use ExprKind::*;
use std::sync::Arc;

impl<O: Clone + Send + 'static> Engine<O> {
    /// Evaluates an if-then-else expression.
    ///
    /// First evaluates the condition, then either the 'then' branch if the condition is true, or the
    /// 'else' branch if the condition is false, passing results to the continuation.
    ///
    /// # Parameters
    ///
    /// * `cond` - The condition expression.
    /// * `then_expr` - The expression to evaluate if condition is true.
    /// * `else_expr` - The expression to evaluate if condition is false.
    /// * `k` - The continuation to receive evaluation results.
    pub(crate) async fn evaluate_if_then_else(
        self,
        cond: Arc<Expr>,
        then_expr: Arc<Expr>,
        else_expr: Arc<Expr>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        let engine = self.clone();

        self.evaluate(
            cond,
            Arc::new(move |value| {
                Box::pin(capture!([then_expr, engine, else_expr, k], async move {
                    match value.data {
                        CoreData::Literal(Literal::Bool(b)) => {
                            if b {
                                engine.evaluate(then_expr, k).await
                            } else {
                                engine.evaluate(else_expr, k).await
                            }
                        }
                        _ => panic!("Expected boolean in condition"),
                    }
                }))
            }),
        )
        .await
    }

    /// Evaluates a let binding expression.
    ///
    /// Binds the result of evaluating the assignee to the identifier in the context, then evaluates the
    /// 'after' expression in the updated context, passing results to the continuation.
    ///
    /// # Parameters
    ///
    /// * `binding` - The binding to evaluate and bind to the context.
    /// * `after` - The expression to evaluate in the updated context.
    /// * `k` - The continuation to receive evaluation results.
    pub(crate) async fn evaluate_let_binding(
        self,
        binding: LetBinding,
        after: Arc<Expr>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        let engine = self.clone();
        let LetBinding { name, expr, .. } = binding;

        self.evaluate(
            expr,
            Arc::new(move |value| {
                Box::pin(capture!([name, engine, after, k], async move {
                    // Create updated context with the new binding.
                    let mut new_ctx = engine.context.clone();
                    new_ctx.bind(name, value);

                    // Evaluate the after expression in the updated context.
                    engine.with_new_context(new_ctx).evaluate(after, k).await
                }))
            }),
        )
        .await
    }

    /// Evaluates a new scope expression.
    ///
    /// Creates a new context, pushes a new scope onto it, and evaluates the expression in that
    /// context.
    ///
    /// # Parameters
    /// * `expr` - The expression to evaluate.
    /// * `k` - The continuation to receive evaluation results.
    pub(crate) async fn evaluate_new_scope(
        self,
        expr: Arc<Expr>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        let mut new_ctx = self.context.clone();
        new_ctx.push_scope();
        self.with_new_context(new_ctx)
            .evaluate(expr.clone(), k)
            .await
    }

    /// Evaluates a binary expression.
    ///
    /// Handles different binary operations, with special cases for logical operators to enable
    /// short-circuit evaluation.
    ///
    /// # Parameters
    /// * `left` - The left operand
    /// * `op` - The binary operator
    /// * `right` - The right operand
    /// * `k` - The continuation to receive evaluation results
    pub(crate) async fn evaluate_binary_expr(
        self,
        left: Arc<Expr>,
        op: BinOp,
        right: Arc<Expr>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        match op {
            // Special case for logical operators that implement short-circuit evaluation.
            BinOp::And => self.evaluate_and(left, right, k).await,
            BinOp::Or => self.evaluate_or(left, right, k).await,
            // For all other operators, use the generic non-short-circuit evaluation.
            _ => {
                let engine = self.clone();
                self.evaluate(
                    left,
                    Arc::new(move |left_val| {
                        Box::pin(capture!([right, op, engine, k], async move {
                            engine.evaluate_right(left_val, right, op, k).await
                        }))
                    }),
                )
                .await
            }
        }
    }

    /// Helper function to evaluate the right operand after the left is evaluated.
    async fn evaluate_right(
        self,
        left_val: Value,
        right: Arc<Expr>,
        op: BinOp,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        self.evaluate(
            right,
            Arc::new(move |right_val| {
                Box::pin(capture!([left_val, op, k], async move {
                    let result = eval_binary_op(left_val, &op, right_val);
                    k(result).await
                }))
            }),
        )
        .await
    }

    /// Evaluates a unary expression.
    ///
    /// Evaluates the operand, then applies the unary operation, passing the result to the continuation.
    ///
    /// # Parameters
    ///
    /// * `op` - The unary operator.
    /// * `expr` - The operand expression.
    /// * `k` - The continuation to receive evaluation results.
    pub(crate) async fn evaluate_unary_expr(
        self,
        op: UnaryOp,
        expr: Arc<Expr>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        self.evaluate(
            expr,
            Arc::new(move |value| {
                Box::pin(capture!([op, k], async move {
                    let result = eval_unary_op(&op, value);
                    k(result).await
                }))
            }),
        )
        .await
    }

    /// Evaluates a call expression.
    ///
    /// First evaluates the called expression, then the arguments, and finally applies the call to
    /// the arguments, passing results to the continuation.
    ///
    /// Extended to support indexing into collections (Array, Tuple, Struct, Map, Logical, Physical)
    /// when the called expression evaluates to one of these types and a single argument is provided.
    ///
    /// # Parameters
    ///
    /// * `called` - The called expression to evaluate.
    /// * `args` - The argument expressions to evaluate.
    /// * `k` - The continuation to receive evaluation results.
    pub(crate) async fn evaluate_call(
        self,
        called: Arc<Expr>,
        args: Vec<Arc<Expr>>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        let engine = self.clone();

        self.evaluate(
            called,
            Arc::new(move |called_value| {
                Box::pin(capture!([args, engine, k], async move {
                    match called_value.data {
                        // Handle function calls.
                        CoreData::Function(FunKind::Closure(params, body)) => {
                            engine.evaluate_closure_call(params, body, args, k).await
                        }
                        CoreData::Function(FunKind::Udf(udf)) => {
                            engine.evaluate_rust_udf_call(udf, args, k).await
                        }

                        // Handle collection indexing.
                        CoreData::Array(_) | CoreData::Tuple(_) | CoreData::Struct(_, _) => {
                            engine.evaluate_indexed_access(called_value, args, k).await
                        }
                        CoreData::Map(_) => engine.evaluate_map_lookup(called_value, args, k).await,

                        // Handle operator field accesses.
                        CoreData::Logical(op) => {
                            engine.evaluate_logical_operator_access(op, args, k).await
                        }
                        CoreData::Physical(op) => {
                            engine.evaluate_physical_operator_access(op, args, k).await
                        }

                        // Value must be a function or indexable collection/operator.
                        _ => panic!(
                            "Expected function or indexable value, got: {:?}",
                            called_value
                        ),
                    }
                }))
            }),
        )
        .await
    }

    /// Evaluates access to a logical operator.
    ///
    /// Handles both materialized and unmaterialized logical operators.
    ///
    /// # Parameters
    ///
    /// * `op` - The logical operator (materialized or unmaterialized).
    /// * `args` - The argument expressions (should be a single index).
    /// * `k` - The continuation to receive evaluation results.
    fn evaluate_logical_operator_access(
        self,
        op: Materializable<LogicalOp<Value>, GroupId>,
        args: Vec<Arc<Expr>>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> impl Future<Output = EngineResponse<O>> + Send {
        Box::pin(async move {
            self.validate_single_index_arg(&args);

            match op {
                // For unmaterialized logical operators, yield the group and continue when it's expanded.
                Materializable::UnMaterialized(group_id) => EngineResponse::YieldGroup(
                    group_id,
                    Arc::new(move |expanded_value| {
                        let engine = self.clone();
                        Box::pin(capture!([args, k], async move {
                            engine
                                .evaluate_call(Expr::new(CoreVal(expanded_value)).into(), args, k)
                                .await
                        }))
                    }),
                ),
                // For materialized logical operators, access the data or children directly.
                Materializable::Materialized(log_op) => {
                    self.evaluate_index_on_materialized_operator(
                        args[0].clone(),
                        log_op.operator.data,
                        log_op.operator.children,
                        k,
                    )
                    .await
                }
            }
        })
    }

    /// Evaluates access to a physical operator.
    ///
    /// Handles both materialized and unmaterialized physical operators.
    ///
    /// # Parameters
    ///
    /// * `op` - The physical operator (materialized or unmaterialized).
    /// * `args` - The argument expressions (should be a single index).
    /// * `k` - The continuation to receive evaluation results.
    fn evaluate_physical_operator_access(
        self,
        op: Materializable<PhysicalOp<Value>, Goal>,
        args: Vec<Arc<Expr>>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> impl Future<Output = EngineResponse<O>> + Send {
        Box::pin(async move {
            self.validate_single_index_arg(&args);

            match op {
                // For unmaterialized physical operators, yield the goal and continue when it's expanded.
                Materializable::UnMaterialized(goal) => EngineResponse::YieldGoal(
                    goal,
                    Arc::new(move |expanded_value| {
                        let engine = self.clone();
                        Box::pin(capture!([args, k], async move {
                            engine
                                .evaluate_call(Expr::new(CoreVal(expanded_value)).into(), args, k)
                                .await
                        }))
                    }),
                ),
                // For materialized physical operators, access the data or children directly.
                Materializable::Materialized(phys_op) => {
                    self.evaluate_index_on_materialized_operator(
                        args[0].clone(),
                        phys_op.operator.data,
                        phys_op.operator.children,
                        k,
                    )
                    .await
                }
            }
        })
    }

    /// Validates that exactly one index argument is provided.
    ///
    /// # Parameters
    ///
    /// * `args` - The argument expressions to validate.
    fn validate_single_index_arg(&self, args: &[Arc<Expr>]) {
        if args.len() != 1 {
            panic!("Operator access requires exactly one index argument");
        }
    }

    /// Evaluates an index expression on a materialized operator.
    ///
    /// Treats the operator's data and children as a concatenated vector and accesses by index.
    ///
    /// # Parameters
    ///
    /// * `index_expr` - The index expression to evaluate.
    /// * `data` - The operator's data fields.
    /// * `children` - The operator's children.
    /// * `k` - The continuation to receive evaluation results.
    async fn evaluate_index_on_materialized_operator(
        self,
        index_expr: Arc<Expr>,
        data: Vec<Value>,
        children: Vec<Value>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        let engine = self.clone();

        self.evaluate(
            index_expr,
            Arc::new(move |index_value| {
                Box::pin(capture!([data, engine, children, k], async move {
                    let index = engine.extract_index(&index_value);
                    let result = engine.access_operator_field(index, &data, &children);

                    k(result).await
                }))
            }),
        )
        .await
    }

    /// Extracts an integer index from a value.
    ///
    /// # Parameters
    ///
    /// * `index_value` - The value containing the index.
    ///
    /// # Returns
    ///
    /// The extracted integer index.
    fn extract_index(&self, index_value: &Value) -> usize {
        match &index_value.data {
            CoreData::Literal(Literal::Int64(i)) => *i as usize,
            _ => panic!("Index must be an integer, got: {:?}", index_value),
        }
    }

    /// Accesses a field in an operator by index.
    ///
    /// Treats data and children as a concatenated vector and accesses by index.
    ///
    /// # Parameters
    ///
    /// * `index` - The index to access.
    /// * `data` - The operator's data fields.
    /// * `children` - The operator's children.
    ///
    /// # Returns
    ///
    /// The value at the specified index.
    fn access_operator_field(&self, index: usize, data: &[Value], children: &[Value]) -> Value {
        let data_len = data.len();
        let total_len = data_len + children.len();

        if index >= total_len {
            panic!("index out of bounds: {} >= {}", index, total_len);
        }

        if index < data_len {
            data[index].clone()
        } else {
            children[index - data_len].clone()
        }
    }

    /// Evaluates indexing into a collection (Array, Tuple, or Struct).
    ///
    /// # Parameters
    ///
    /// * `collection` - The collection value to index into.
    /// * `args` - The argument expressions (should be a single index).
    /// * `k` - The continuation to receive evaluation results.
    async fn evaluate_indexed_access(
        self,
        collection: Value,
        args: Vec<Arc<Expr>>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        self.validate_single_index_arg(&args);
        let engine = self.clone();

        self.evaluate(
            args[0].clone(),
            Arc::new(move |index_value| {
                Box::pin(capture!([collection, engine, k], async move {
                    let index = engine.extract_index(&index_value);

                    let result = match &collection.data {
                        CoreData::Array(items) => engine.get_indexed_item(items, index),
                        CoreData::Tuple(items) => engine.get_indexed_item(items, index),
                        CoreData::Struct(_, fields) => engine.get_indexed_item(fields, index),
                        _ => {
                            panic!("Attempted to index a non-indexable value: {:?}", collection)
                        }
                    };

                    k(result).await
                }))
            }),
        )
        .await
    }

    /// Gets an item from a collection at the specified index.
    ///
    /// # Parameters
    ///
    /// * `items` - The collection items.
    /// * `index` - The index to access.
    ///
    /// # Returns
    ///
    /// The value at the specified index.
    fn get_indexed_item(&self, items: &[Value], index: usize) -> Value {
        if index < items.len() {
            items[index].clone()
        } else {
            Value::new(CoreData::None)
        }
    }

    /// Evaluates a map lookup.
    ///
    /// # Parameters
    ///
    /// * `map_value` - The map value to look up in.
    /// * `args` - The argument expressions (should be a single key).
    /// * `k` - The continuation to receive evaluation results.
    async fn evaluate_map_lookup(
        self,
        map_value: Value,
        args: Vec<Arc<Expr>>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        self.validate_single_index_arg(&args);

        self.evaluate(
            args[0].clone(),
            Arc::new(move |key_value| {
                Box::pin(capture!([map_value, k], async move {
                    match &map_value.data {
                        CoreData::Map(map) => {
                            let result = map.get(&key_value);
                            k(result).await
                        }
                        _ => panic!(
                            "Attempted to perform map lookup on non-map value: {:?}",
                            map_value
                        ),
                    }
                }))
            }),
        )
        .await
    }

    pub(crate) async fn evaluate_closure_call(
        self,
        params: Vec<Identifier>,
        body: Arc<Expr>,
        args: Vec<Arc<Expr>>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        let engine = self.clone();

        engine
            .clone()
            .evaluate_sequence(
                args,
                Arc::new(move |arg_values| {
                    Box::pin(capture!([params, body, engine, k], async move {
                        // Create context with bound parameters.
                        let mut new_ctx = engine.context.clone();
                        new_ctx.push_scope();
                        params.iter().zip(arg_values).for_each(|(p, a)| {
                            new_ctx.bind(p.clone(), a);
                        });

                        // Create return continuation.
                        // We verify the return value is a failure and if so,
                        // we propagate it to the caller.
                        let return_k = Arc::new(capture!([k, engine], move |value: Value| {
                            if let CoreData::Fail(_) = &value.data {
                                if let Some(return_k) = &engine.fun_return {
                                    return return_k(value);
                                }
                            }
                            k(value)
                        }));

                        // Execute function body with appropriate continuations.
                        engine
                            .with_new_return(return_k)
                            .with_new_context(new_ctx)
                            .evaluate(body, k)
                            .await
                    }))
                }),
            )
            .await
    }

    /// Evaluates a call to a Rust UDF.
    ///
    /// Evaluates the arguments, then calls the Rust function with those arguments, passing the result
    /// to the continuation.
    ///
    /// # Parameters
    ///
    /// * `udf` - The Rust function to call
    /// * `args` - The argument expressions to evaluate
    /// * `k` - The continuation to receive evaluation results
    pub(crate) async fn evaluate_rust_udf_call(
        self,
        udf: Udf,
        args: Vec<Arc<Expr>>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        let catalog = Arc::clone(&self.catalog);

        self.evaluate_sequence(
            args,
            Arc::new(move |arg_values| {
                Box::pin(capture!([udf, catalog, k], async move {
                    // Call the UDF with the argument values.
                    let result = udf.call(&arg_values, catalog.as_ref());

                    // Pass the result to the continuation.
                    k(result).await
                }))
            }),
        )
        .await
    }

    /// Evaluates a map expression.
    ///
    /// # Parameters
    ///
    /// * `items` - The key-value pairs to evaluate.
    /// * `k` - The continuation to receive evaluation results.
    pub(crate) async fn evaluate_map(
        self,
        items: Vec<(Arc<Expr>, Arc<Expr>)>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        let (keys, values): (Vec<Arc<Expr>>, Vec<Arc<Expr>>) = items.into_iter().unzip();
        let engine = self.clone();

        self.evaluate_sequence(
            keys,
            Arc::new(move |keys_values| {
                Box::pin(capture!([values, engine, k], async move {
                    // Then evaluate all value expressions.
                    engine
                        .evaluate_sequence(
                            values,
                            Arc::new(move |values_values| {
                                Box::pin(capture!([keys_values, k], async move {
                                    // Create a map from keys and values.
                                    let map_items =
                                        keys_values.into_iter().zip(values_values).collect();
                                    k(Value::new(CoreData::Map(Map::from_pairs(map_items)))).await
                                }))
                            }),
                        )
                        .await
                }))
            }),
        )
        .await
    }

    /// Evaluates a reference to a variable.
    ///
    /// Looks up the variable in the context and passes its value to the continuation.
    ///
    /// # Parameters
    ///
    /// * `ident` - The identifier to look up
    /// * `k` - The continuation to receive the variable value
    pub(crate) async fn evaluate_reference(
        self,
        ident: String,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O> {
        let value = self
            .context
            .lookup(&ident)
            .unwrap_or_else(|| panic!("Variable not found: {}", ident))
            .clone();

        k(value).await
    }

    /// Evaluates a return expression.
    ///
    /// Pops the most recent continuation from the function return stack and uses it to
    /// evaluate the return expression. This implements early returns from functions.
    ///
    /// # Parameters
    ///
    /// * `expr` - The expression to evaluate and return.
    pub(crate) async fn evaluate_return(self, expr: Arc<Expr>) -> EngineResponse<O> {
        let return_k = self.fun_return.clone().unwrap();
        self.evaluate(expr, return_k).await
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::iceberg::memory_catalog;
    use crate::dsl::analyzer::hir::{Goal, GroupId, LetBinding, Materializable, Udf};
    use crate::dsl::engine::Engine;
    use crate::dsl::utils::tests::{
        array_val, assert_values_equal, create_logical_operator, create_physical_operator,
        ref_expr, struct_val,
    };
    use crate::dsl::{
        analyzer::{
            context::Context,
            hir::{BinOp, CoreData, Expr, ExprKind, FunKind, Literal, Value},
        },
        utils::tests::{
            TestHarness, boolean, evaluate_and_collect, int, lit_expr, lit_val, string,
        },
    };
    use ExprKind::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_if_then_else() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = Arc::new(memory_catalog());
        let engine = Engine::new(ctx, catalog.clone());

        // if true then "yes" else "no"
        let true_condition = Arc::new(Expr::new(IfThenElse(
            lit_expr(boolean(true)),
            lit_expr(string("yes")),
            lit_expr(string("no")),
        )));
        let true_results =
            evaluate_and_collect(true_condition, engine.clone(), harness.clone()).await;

        // if false then "yes" else "no"
        let false_condition = Arc::new(Expr::new(IfThenElse(
            lit_expr(boolean(false)),
            lit_expr(string("yes")),
            lit_expr(string("no")),
        )));
        let false_results =
            evaluate_and_collect(false_condition, engine.clone(), harness.clone()).await;

        // Let's create a more complex condition: if x > 10 then x * 2 else x / 2
        let mut ctx = Context::default();
        ctx.bind("x".to_string(), lit_val(int(20)));
        let engine_with_x = Engine::new(ctx, catalog);

        let complex_condition = Arc::new(Expr::new(IfThenElse(
            Arc::new(Expr::new(Binary(
                ref_expr("x"),
                BinOp::Lt,
                lit_expr(int(10)),
            ))),
            Arc::new(Expr::new(Binary(
                ref_expr("x"),
                BinOp::Div,
                lit_expr(int(2)),
            ))),
            Arc::new(Expr::new(Binary(
                ref_expr("x"),
                BinOp::Mul,
                lit_expr(int(2)),
            ))),
        )));

        let complex_results = evaluate_and_collect(complex_condition, engine_with_x, harness).await;

        // Check results
        match &true_results[0].data {
            CoreData::Literal(Literal::String(value)) => {
                assert_eq!(value, "yes"); // true condition should select "yes"
            }
            _ => panic!("Expected string value"),
        }

        match &false_results[0].data {
            CoreData::Literal(Literal::String(value)) => {
                assert_eq!(value, "no"); // false condition should select "no"
            }
            _ => panic!("Expected string value"),
        }

        match &complex_results[0].data {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 40); // 20 * 2 = 40 (since x > 10)
            }
            _ => panic!("Expected integer value"),
        }
    }

    #[tokio::test]
    async fn test_let_binding() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // let x = 10 in x + 5
        let let_expr = Arc::new(Expr::new(Let(
            LetBinding::new("x".to_string(), lit_expr(int(10))),
            Arc::new(Expr::new(Binary(
                ref_expr("x"),
                BinOp::Add,
                lit_expr(int(5)),
            ))),
        )));

        let results = evaluate_and_collect(let_expr, engine, harness).await;

        // Check result
        match &results[0].data {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 15); // 10 + 5 = 15
            }
            _ => panic!("Expected integer value"),
        }
    }

    #[tokio::test]
    async fn test_new_scope_and_shadowing() {
        let harness = TestHarness::new();
        let mut ctx = Context::default();
        // Bind a variable in the outer scope
        ctx.bind("x".to_string(), lit_val(int(10)));
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Create an inner let binding to shadow x
        let inner_let = Arc::new(Expr::new(Let(
            LetBinding::new("x".to_string(), lit_expr(int(20))), // Shadow x in new scope
            ref_expr("x"),                                       // Return the shadowed value
        )));

        // Create a new scope with the inner let
        let new_scope = Arc::new(Expr::new(NewScope(inner_let)));

        // Create the outer let binding for inner_value
        let test_expr = Arc::new(Expr::new(Let(
            LetBinding::new("inner_value".to_string(), new_scope),
            // Create a tuple with both the inner value and the outer value of x
            Arc::new(Expr::new(CoreExpr(CoreData::Tuple(vec![
                ref_expr("inner_value"), // Value from inner scope
                ref_expr("x"),           // Value from outer scope after leaving inner scope
            ])))),
        )));

        let results = evaluate_and_collect(test_expr, engine, harness).await;

        // Check results - should be a tuple (20, 10)
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Tuple(elements) => {
                assert_eq!(elements.len(), 2);
                // First element should be from shadowed variable in inner scope
                match &elements[0].data {
                    CoreData::Literal(Literal::Int64(value)) => {
                        assert_eq!(*value, 20);
                    }
                    _ => panic!("Expected integer value"),
                }
                // Second element should be from outer scope after leaving inner scope
                match &elements[1].data {
                    CoreData::Literal(Literal::Int64(value)) => {
                        assert_eq!(*value, 10);
                    }
                    _ => panic!("Expected integer value"),
                }
            }
            _ => panic!("Expected tuple result"),
        }
    }

    #[tokio::test]
    async fn test_evaluate_binary_logical() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Test direct binary expression evaluation for AND operator
        // true && true = true
        let true_and_true = Arc::new(Expr::new(Binary(
            lit_expr(boolean(true)),
            BinOp::And,
            lit_expr(boolean(true)),
        )));
        let true_and_true_result =
            evaluate_and_collect(true_and_true, engine.clone(), harness.clone()).await;

        // true && false = false
        let true_and_false = Arc::new(Expr::new(Binary(
            lit_expr(boolean(true)),
            BinOp::And,
            lit_expr(boolean(false)),
        )));
        let true_and_false_result =
            evaluate_and_collect(true_and_false, engine.clone(), harness.clone()).await;

        // false && true = false
        let false_and_true = Arc::new(Expr::new(Binary(
            lit_expr(boolean(false)),
            BinOp::And,
            lit_expr(boolean(true)),
        )));
        let false_and_true_result =
            evaluate_and_collect(false_and_true, engine.clone(), harness.clone()).await;

        // false && false = false
        let false_and_false = Arc::new(Expr::new(Binary(
            lit_expr(boolean(false)),
            BinOp::And,
            lit_expr(boolean(false)),
        )));
        let false_and_false_result =
            evaluate_and_collect(false_and_false, engine.clone(), harness.clone()).await;

        // Test direct binary expression evaluation for OR operator
        // true || true = true
        let true_or_true = Arc::new(Expr::new(Binary(
            lit_expr(boolean(true)),
            BinOp::Or,
            lit_expr(boolean(true)),
        )));
        let true_or_true_result =
            evaluate_and_collect(true_or_true, engine.clone(), harness.clone()).await;

        // true || false = true
        let true_or_false = Arc::new(Expr::new(Binary(
            lit_expr(boolean(true)),
            BinOp::Or,
            lit_expr(boolean(false)),
        )));
        let true_or_false_result =
            evaluate_and_collect(true_or_false, engine.clone(), harness.clone()).await;

        // false || true = true
        let false_or_true = Arc::new(Expr::new(Binary(
            lit_expr(boolean(false)),
            BinOp::Or,
            lit_expr(boolean(true)),
        )));
        let false_or_true_result =
            evaluate_and_collect(false_or_true, engine.clone(), harness.clone()).await;

        // false || false = false
        let false_or_false = Arc::new(Expr::new(Binary(
            lit_expr(boolean(false)),
            BinOp::Or,
            lit_expr(boolean(false)),
        )));
        let false_or_false_result = evaluate_and_collect(false_or_false, engine, harness).await;

        // Verify all AND results
        match &true_and_true_result[0].data {
            CoreData::Literal(Literal::Bool(value)) => {
                assert!(*value, "true && true should be true");
            }
            _ => panic!("Expected boolean value from true && true"),
        }

        match &true_and_false_result[0].data {
            CoreData::Literal(Literal::Bool(value)) => {
                assert!(!(*value), "true && false should be false");
            }
            _ => panic!("Expected boolean value from true && false"),
        }

        match &false_and_true_result[0].data {
            CoreData::Literal(Literal::Bool(value)) => {
                assert!(!(*value), "false && true should be false");
            }
            _ => panic!("Expected boolean value from false && true"),
        }

        match &false_and_false_result[0].data {
            CoreData::Literal(Literal::Bool(value)) => {
                assert!(!(*value), "false && false should be false");
            }
            _ => panic!("Expected boolean value from false && false"),
        }

        // Verify all OR results
        match &true_or_true_result[0].data {
            CoreData::Literal(Literal::Bool(value)) => {
                assert!(*value, "true || true should be true");
            }
            _ => panic!("Expected boolean value from true || true"),
        }

        match &true_or_false_result[0].data {
            CoreData::Literal(Literal::Bool(value)) => {
                assert!(*value, "true || false should be true");
            }
            _ => panic!("Expected boolean value from true || false"),
        }

        match &false_or_true_result[0].data {
            CoreData::Literal(Literal::Bool(value)) => {
                assert!(*value, "false || true should be true");
            }
            _ => panic!("Expected boolean value from false || true"),
        }

        match &false_or_false_result[0].data {
            CoreData::Literal(Literal::Bool(value)) => {
                assert!(!(*value), "false || false should be false");
            }
            _ => panic!("Expected boolean value from false || false"),
        }
    }

    #[tokio::test]
    async fn test_return_expression_breaks_control_flow() {
        let harness = TestHarness::new();
        let mut ctx = Context::default();

        // Define a function:
        // fn test_return(x) => {
        //   let result = if x < 10 {
        //     return "too small";
        //   } else {
        //     "big enough"
        //   };
        //   "result is: " + result;
        // }
        let test_return_function = Value::new(CoreData::Function(FunKind::Closure(
            vec!["x".to_string()],
            Arc::new(Expr::new(Let(
                LetBinding::new(
                    "result".to_string(),
                    Arc::new(Expr::new(IfThenElse(
                        Arc::new(Expr::new(Binary(
                            ref_expr("x"),
                            BinOp::Lt,
                            lit_expr(int(10)),
                        ))),
                        Arc::new(Expr::new(Return(lit_expr(string("too small"))))),
                        lit_expr(string("big enough")),
                    ))),
                ),
                // This part should never execute when x < 10
                Arc::new(Expr::new(Binary(
                    lit_expr(string("result is: ")),
                    BinOp::Concat,
                    ref_expr("result"),
                ))),
            ))),
        )));

        ctx.bind("test_return".to_string(), test_return_function);
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Call with x = 5, should return "too small" directly, skipping the string concatenation
        let small_call = Arc::new(Expr::new(Call(
            ref_expr("test_return"),
            vec![lit_expr(int(5))],
        )));
        let small_result = evaluate_and_collect(small_call, engine.clone(), harness.clone()).await;

        // Call with x = 15, should proceed normally
        let big_call = Arc::new(Expr::new(Call(
            ref_expr("test_return"),
            vec![lit_expr(int(15))],
        )));
        let big_result = evaluate_and_collect(big_call, engine, harness).await;

        // Check results
        assert_eq!(small_result.len(), 1);
        match &small_result[0].data {
            CoreData::Literal(Literal::String(value)) => {
                assert_eq!(value, "too small");
            }
            _ => panic!("Expected string value"),
        }

        assert_eq!(big_result.len(), 1);
        match &big_result[0].data {
            CoreData::Literal(Literal::String(value)) => {
                assert_eq!(value, "result is: big enough");
            }
            _ => panic!("Expected string value"),
        }
    }

    /// Test nested let bindings
    #[tokio::test]
    async fn test_nested_let_bindings() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // let x = 10 in
        // let y = x * 2 in
        // x + y
        let nested_let_expr = Arc::new(Expr::new(Let(
            LetBinding::new("x".to_string(), lit_expr(int(10))),
            Arc::new(Expr::new(Let(
                LetBinding::new(
                    "y".to_string(),
                    Arc::new(Expr::new(Binary(
                        ref_expr("x"),
                        BinOp::Mul,
                        lit_expr(int(2)),
                    ))),
                ),
                Arc::new(Expr::new(Binary(ref_expr("x"), BinOp::Add, ref_expr("y")))),
            ))),
        )));

        let results = evaluate_and_collect(nested_let_expr, engine, harness).await;

        // Check result
        match &results[0].data {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 30); // 10 + (10 * 2) = 30
            }
            _ => panic!("Expected integer value"),
        }
    }

    #[tokio::test]
    async fn test_function_call_closure() {
        let harness = TestHarness::new();
        let mut ctx = Context::default();

        // Define a function: fn(x, y) => x + y
        let add_function = Value::new(CoreData::Function(FunKind::Closure(
            vec!["x".to_string(), "y".to_string()],
            Arc::new(Expr::new(Binary(ref_expr("x"), BinOp::Add, ref_expr("y")))),
        )));

        ctx.bind("add".to_string(), add_function);
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Call the function: add(10, 20)
        let call_expr = Arc::new(Expr::new(Call(
            ref_expr("add"),
            vec![lit_expr(int(10)), lit_expr(int(20))],
        )));

        let results = evaluate_and_collect(call_expr, engine, harness).await;

        // Check result
        match &results[0].data {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 30); // 10 + 20 = 30
            }
            _ => panic!("Expected integer value"),
        }
    }

    #[tokio::test]
    async fn test_function_call_rust_udf() {
        let harness = TestHarness::new();
        let mut ctx = Context::default();

        // Define a Rust UDF that calculates the sum of array elements
        let sum_function = Value::new(CoreData::Function(FunKind::Udf(Udf {
            func: |args, _catalog| match &args[0].data {
                CoreData::Array(elements) => {
                    let mut sum = 0;
                    for elem in elements {
                        if let CoreData::Literal(Literal::Int64(value)) = &elem.data {
                            sum += value;
                        }
                    }
                    Value::new(CoreData::Literal(Literal::Int64(sum)))
                }
                _ => panic!("Expected array argument"),
            },
        })));

        ctx.bind("sum".to_string(), sum_function);
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Call the function: sum([1, 2, 3, 4, 5])
        let call_expr = Arc::new(Expr::new(Call(
            ref_expr("sum"),
            vec![Arc::new(Expr::new(CoreVal(array_val(vec![
                lit_val(int(1)),
                lit_val(int(2)),
                lit_val(int(3)),
                lit_val(int(4)),
                lit_val(int(5)),
            ]))))],
        )));

        let results = evaluate_and_collect(call_expr, engine, harness).await;

        // Check result
        match &results[0].data {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 15); // 1 + 2 + 3 + 4 + 5 = 15
            }
            _ => panic!("Expected integer value"),
        }
    }

    #[tokio::test]
    async fn test_map_creation() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Create a map with key-value pairs: { "a": 1, "b": 2, "c": 3 }
        let map_expr = Arc::new(Expr::new(Map(vec![
            (lit_expr(string("a")), lit_expr(int(1))),
            (lit_expr(string("b")), lit_expr(int(2))),
            (lit_expr(string("c")), lit_expr(int(3))),
        ])));

        // Evaluate the map expression
        let results = evaluate_and_collect(map_expr, engine.clone(), harness.clone()).await;

        // Check that we got a Map value
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Map(map) => {
                // Check that map has the correct key-value pairs
                assert_values_equal(&map.get(&lit_val(string("a"))), &lit_val(int(1)));
                assert_values_equal(&map.get(&lit_val(string("b"))), &lit_val(int(2)));
                assert_values_equal(&map.get(&lit_val(string("c"))), &lit_val(int(3)));

                // Check that non-existent key returns None value
                assert_values_equal(&map.get(&lit_val(string("d"))), &Value::new(CoreData::None));
            }
            _ => panic!("Expected Map value"),
        }

        // Test map with expressions that need evaluation as keys and values
        // Map: { "x" + "y": 10 + 5, "a" + "b": 20 * 2 }
        let complex_map_expr = Arc::new(Expr::new(Map(vec![
            (
                Arc::new(Expr::new(Binary(
                    lit_expr(string("x")),
                    BinOp::Concat,
                    lit_expr(string("y")),
                ))),
                Arc::new(Expr::new(Binary(
                    lit_expr(int(10)),
                    BinOp::Add,
                    lit_expr(int(5)),
                ))),
            ),
            (
                Arc::new(Expr::new(Binary(
                    lit_expr(string("a")),
                    BinOp::Concat,
                    lit_expr(string("b")),
                ))),
                Arc::new(Expr::new(Binary(
                    lit_expr(int(20)),
                    BinOp::Mul,
                    lit_expr(int(2)),
                ))),
            ),
        ])));

        // Evaluate the complex map expression
        let complex_results = evaluate_and_collect(complex_map_expr, engine, harness).await;

        // Check that we got a Map value with correctly evaluated keys and values
        assert_eq!(complex_results.len(), 1);
        match &complex_results[0].data {
            CoreData::Map(map) => {
                // Check that map has the correct key-value pairs after evaluation
                assert_values_equal(&map.get(&lit_val(string("xy"))), &lit_val(int(15)));
                assert_values_equal(&map.get(&lit_val(string("ab"))), &lit_val(int(40)));
            }
            _ => panic!("Expected Map value"),
        }
    }

    #[tokio::test]
    async fn test_map_nested_and_lookup() {
        let harness = TestHarness::new();
        let mut ctx = Context::default();

        // Add a map lookup function
        ctx.bind(
            "get".to_string(),
            Value::new(CoreData::Function(FunKind::Udf(Udf {
                func: |args, _catalog| {
                    if args.len() != 2 {
                        panic!("get function requires 2 arguments");
                    }

                    match &args[0].data {
                        CoreData::Map(map) => map.get(&args[1]),
                        _ => panic!("First argument must be a map"),
                    }
                },
            }))),
        );

        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Create a nested map:
        // {
        //   "user": {
        //     "name": "Alice",
        //     "age": 30,
        //     "address": {
        //       "city": "San Francisco",
        //       "zip": 94105
        //     }
        //   },
        //   "settings": {
        //     "theme": "dark",
        //     "notifications": true
        //   }
        // }

        // First, create the address map
        let address_map = Arc::new(Expr::new(Map(vec![
            (lit_expr(string("city")), lit_expr(string("San Francisco"))),
            (lit_expr(string("zip")), lit_expr(int(94105))),
        ])));

        // Then, create the user map with the nested address map
        let user_map = Arc::new(Expr::new(Map(vec![
            (lit_expr(string("name")), lit_expr(string("Alice"))),
            (lit_expr(string("age")), lit_expr(int(30))),
            (lit_expr(string("address")), address_map),
        ])));

        // Create the settings map
        let settings_map = Arc::new(Expr::new(Map(vec![
            (lit_expr(string("theme")), lit_expr(string("dark"))),
            (lit_expr(string("notifications")), lit_expr(boolean(true))),
        ])));

        // Finally, create the top-level map
        let nested_map_expr = Arc::new(Expr::new(Map(vec![
            (lit_expr(string("user")), user_map),
            (lit_expr(string("settings")), settings_map),
        ])));

        // Create the nested data lookup expressions step by step
        let get_user = Arc::new(Expr::new(Call(
            ref_expr("get"),
            vec![ref_expr("data"), lit_expr(string("user"))],
        )));

        let get_address = Arc::new(Expr::new(Call(
            ref_expr("get"),
            vec![get_user, lit_expr(string("address"))],
        )));

        let get_city = Arc::new(Expr::new(Call(
            ref_expr("get"),
            vec![get_address, lit_expr(string("city"))],
        )));

        // First, evaluate the nested map to bind it to a variable
        let program = Arc::new(Expr::new(Let(
            LetBinding::new("data".to_string(), nested_map_expr),
            // Extract user.address.city using get function
            get_city,
        )));

        // Evaluate the program
        let results = evaluate_and_collect(program, engine, harness).await;

        // Check that we got the correct value from the nested lookup
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(Literal::String(value)) => {
                assert_eq!(value, "San Francisco");
            }
            _ => panic!("Expected string value"),
        }
    }

    #[tokio::test]
    async fn test_complex_program() {
        let harness = TestHarness::new();
        let mut ctx = Context::default();

        // Define a function to compute factorial: fn(n) => if n <= 1 then 1 else n * factorial(n-1)
        let factorial_function = Value::new(CoreData::Function(FunKind::Closure(
            vec!["n".to_string()],
            Arc::new(Expr::new(IfThenElse(
                Arc::new(Expr::new(Binary(
                    ref_expr("n"),
                    BinOp::Lt,
                    lit_expr(int(2)), // n < 2
                ))),
                lit_expr(int(1)), // then 1
                Arc::new(Expr::new(Binary(
                    ref_expr("n"),
                    BinOp::Mul,
                    Arc::new(Expr::new(Call(
                        ref_expr("factorial"),
                        vec![Arc::new(Expr::new(Binary(
                            ref_expr("n"),
                            BinOp::Sub,
                            lit_expr(int(1)),
                        )))],
                    ))),
                ))), // else n * factorial(n-1)
            ))),
        )));

        ctx.bind("factorial".to_string(), factorial_function);
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Create a program that:
        // 1. Defines variables for different values
        // 2. Calls factorial on one of them
        // 3. Performs some arithmetic on the result
        let program = Arc::new(Expr::new(Let(
            LetBinding::new("a".to_string(), lit_expr(int(5))), // a = 5
            Arc::new(Expr::new(Let(
                LetBinding::new("b".to_string(), lit_expr(int(3))), // b = 3
                Arc::new(Expr::new(Let(
                    LetBinding::new(
                        "fact_a".to_string(),
                        Arc::new(Expr::new(Call(ref_expr("factorial"), vec![ref_expr("a")]))), // fact_a = factorial(a)
                    ),
                    Arc::new(Expr::new(Binary(
                        ref_expr("fact_a"),
                        BinOp::Div,
                        ref_expr("b"),
                    ))), // fact_a / b
                ))),
            ))),
        )));

        let results = evaluate_and_collect(program, engine, harness).await;

        // Check result: factorial(5) / 3 = 120 / 3 = 40
        match &results[0].data {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 40);
            }
            _ => panic!("Expected integer value"),
        }
    }

    #[tokio::test]
    async fn test_array_indexing() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Create an array [10, 20, 30, 40, 50]
        let array_expr = Arc::new(Expr::new(CoreVal(array_val(vec![
            lit_val(int(10)),
            lit_val(int(20)),
            lit_val(int(30)),
            lit_val(int(40)),
            lit_val(int(50)),
        ]))));

        // Access array[2] which should be 30
        let index_expr = Arc::new(Expr::new(Call(array_expr.clone(), vec![lit_expr(int(2))])));
        let results = evaluate_and_collect(index_expr, engine.clone(), harness.clone()).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::Int64(30));
            }
            _ => panic!("Expected integer literal"),
        }

        // Test out-of-bounds access: array[10] which should return None
        let out_of_bounds_expr = Arc::new(Expr::new(Call(array_expr, vec![lit_expr(int(10))])));
        let out_of_bounds_results = evaluate_and_collect(out_of_bounds_expr, engine, harness).await;

        // Check that out-of-bounds access returns None
        assert_eq!(out_of_bounds_results.len(), 1);
        match &out_of_bounds_results[0].data {
            CoreData::None => {
                // Expected None value
            }
            other => panic!("Expected None, got: {:?}", other),
        }
    }

    /// Test tuple indexing with call syntax
    #[tokio::test]
    async fn test_tuple_indexing() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Create a tuple (10, "hello", true)
        let tuple_expr = Arc::new(Expr::new(CoreVal(Value::new(CoreData::Tuple(vec![
            lit_val(int(10)),
            lit_val(string("hello")),
            lit_val(Literal::Bool(true)),
        ])))));

        // Access tuple[1] which should be "hello"
        let index_expr = Arc::new(Expr::new(Call(tuple_expr, vec![lit_expr(int(1))])));

        let results = evaluate_and_collect(index_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::String("hello".to_string()));
            }
            _ => panic!("Expected string literal"),
        }
    }

    /// Test struct field access with call syntax
    #[tokio::test]
    async fn test_struct_indexing() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Create a struct Point { x: 10, y: 20 }
        let struct_expr = Arc::new(Expr::new(CoreVal(struct_val(
            "Point",
            vec![lit_val(int(10)), lit_val(int(20))],
        ))));

        // Access struct[1] which should be 20 (the y field)
        let index_expr = Arc::new(Expr::new(Call(struct_expr, vec![lit_expr(int(1))])));

        let results = evaluate_and_collect(index_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::Int64(20));
            }
            _ => panic!("Expected integer literal"),
        }
    }

    #[tokio::test]
    async fn test_map_lookup() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Create a map with key-value pairs: { "a": 1, "b": 2, "c": 3 }
        // Use a let expression to bind the map and do lookups directly
        let test_expr = Arc::new(Expr::new(Let(
            LetBinding::new(
                "map".to_string(),
                Arc::new(Expr::new(Map(vec![
                    (lit_expr(string("a")), lit_expr(int(1))),
                    (lit_expr(string("b")), lit_expr(int(2))),
                    (lit_expr(string("c")), lit_expr(int(3))),
                ]))),
            ),
            // Create a tuple of map["b"] and map["d"] to test both existing and missing keys
            Arc::new(Expr::new(CoreExpr(CoreData::Tuple(vec![
                // map["b"] - should be 2
                Arc::new(Expr::new(Call(
                    ref_expr("map"),
                    vec![lit_expr(string("b"))],
                ))),
                // map["d"] - should be None
                Arc::new(Expr::new(Call(
                    ref_expr("map"),
                    vec![lit_expr(string("d"))],
                ))),
            ])))),
        )));

        let results = evaluate_and_collect(test_expr, engine, harness).await;

        // Check result - should be a tuple (2, None)
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Tuple(elements) => {
                assert_eq!(elements.len(), 2);
                // Check first element: map["b"] should be 2
                match &elements[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::Int64(2));
                    }
                    _ => panic!("Expected integer literal for existing key lookup"),
                }
                // Check second element: map["d"] should be None
                match &elements[1].data {
                    CoreData::None => {}
                    _ => panic!("Expected None for missing key lookup"),
                }
            }
            _ => panic!("Expected tuple result"),
        }
    }

    #[tokio::test]
    async fn test_complex_collection_and_index() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Create a let expression that binds an array and then accesses it
        // let arr = [10, 20, 30, 40, 50] in
        // let idx = 2 + 1 in
        // arr[idx] // should be 40
        let complex_expr = Arc::new(Expr::new(Let(
            LetBinding::new(
                "arr".to_string(),
                Arc::new(Expr::new(CoreExpr(CoreData::Array(vec![
                    lit_expr(int(10)),
                    lit_expr(int(20)),
                    lit_expr(int(30)),
                    lit_expr(int(40)),
                    lit_expr(int(50)),
                ])))),
            ),
            Arc::new(Expr::new(Let(
                LetBinding::new(
                    "idx".to_string(),
                    Arc::new(Expr::new(Binary(
                        lit_expr(int(2)),
                        BinOp::Add,
                        lit_expr(int(1)),
                    ))),
                ),
                Arc::new(Expr::new(Call(ref_expr("arr"), vec![ref_expr("idx")]))),
            ))),
        )));

        let results = evaluate_and_collect(complex_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::Int64(40));
            }
            _ => panic!("Expected integer literal"),
        }
    }

    #[tokio::test]
    async fn test_logical_operator_indexing() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Create a logical operator: LogicalJoin { joinType: "inner", condition: "x = y" } [TableScan("orders"), TableScan("lineitem")]
        let join_op = create_logical_operator(
            "LogicalJoin",
            vec![lit_val(string("inner")), lit_val(string("x = y"))],
            vec![
                create_logical_operator("TableScan", vec![lit_val(string("orders"))], vec![]),
                create_logical_operator("TableScan", vec![lit_val(string("lineitem"))], vec![]),
            ],
        );

        let logical_op_expr = Arc::new(Expr::new(CoreVal(join_op)));

        // Access join_type using indexing - should be "inner"
        let join_type_expr = Arc::new(Expr::new(Call(
            logical_op_expr.clone(),
            vec![lit_expr(int(0))],
        )));
        let join_type_results =
            evaluate_and_collect(join_type_expr, engine.clone(), harness.clone()).await;

        // Access condition using indexing - should be "x = y"
        let condition_expr = Arc::new(Expr::new(Call(
            logical_op_expr.clone(),
            vec![lit_expr(int(1))],
        )));
        let condition_results =
            evaluate_and_collect(condition_expr, engine.clone(), harness.clone()).await;

        // Access first child (orders table scan) using indexing
        let first_child_expr = Arc::new(Expr::new(Call(
            logical_op_expr.clone(),
            vec![lit_expr(int(2))],
        )));
        let first_child_results =
            evaluate_and_collect(first_child_expr, engine.clone(), harness.clone()).await;

        // Access second child (lineitem table scan) using indexing
        let second_child_expr = Arc::new(Expr::new(Call(logical_op_expr, vec![lit_expr(int(3))])));
        let second_child_results = evaluate_and_collect(second_child_expr, engine, harness).await;

        // Check join_type result
        assert_eq!(join_type_results.len(), 1);
        match &join_type_results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::String("inner".to_string()));
            }
            _ => panic!("Expected string literal for join type"),
        }

        // Check condition result
        assert_eq!(condition_results.len(), 1);
        match &condition_results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::String("x = y".to_string()));
            }
            _ => panic!("Expected string literal for condition"),
        }

        // Check first child result (orders table scan)
        assert_eq!(first_child_results.len(), 1);
        match &first_child_results[0].data {
            CoreData::Logical(Materializable::Materialized(log_op)) => {
                assert_eq!(log_op.operator.tag, "TableScan");
                assert_eq!(log_op.operator.data.len(), 1);
                match &log_op.operator.data[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("orders".to_string()));
                    }
                    _ => panic!("Expected string literal for table name"),
                }
            }
            _ => panic!("Expected logical operator for first child"),
        }

        // Check second child result (lineitem table scan)
        assert_eq!(second_child_results.len(), 1);
        match &second_child_results[0].data {
            CoreData::Logical(Materializable::Materialized(log_op)) => {
                assert_eq!(log_op.operator.tag, "TableScan");
                assert_eq!(log_op.operator.data.len(), 1);
                match &log_op.operator.data[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("lineitem".to_string()));
                    }
                    _ => panic!("Expected string literal for table name"),
                }
            }
            _ => panic!("Expected logical operator for second child"),
        }
    }

    #[tokio::test]
    async fn test_physical_operator_indexing() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Create a physical operator: HashJoin { method: "hash", condition: "id = id" } [IndexScan("customers"), ParallelScan("orders")]
        let join_op = create_physical_operator(
            "HashJoin",
            vec![lit_val(string("hash")), lit_val(string("id = id"))],
            vec![
                create_physical_operator("IndexScan", vec![lit_val(string("customers"))], vec![]),
                create_physical_operator("ParallelScan", vec![lit_val(string("orders"))], vec![]),
            ],
        );

        let physical_op_expr = Arc::new(Expr::new(CoreVal(join_op)));

        // Access join method using indexing - should be "hash"
        let method_expr = Arc::new(Expr::new(Call(
            physical_op_expr.clone(),
            vec![lit_expr(int(0))],
        )));
        let method_results =
            evaluate_and_collect(method_expr, engine.clone(), harness.clone()).await;

        // Access condition using indexing - should be "id = id"
        let condition_expr = Arc::new(Expr::new(Call(
            physical_op_expr.clone(),
            vec![lit_expr(int(1))],
        )));
        let condition_results =
            evaluate_and_collect(condition_expr, engine.clone(), harness.clone()).await;

        // Access first child (customers index scan) using indexing
        let first_child_expr = Arc::new(Expr::new(Call(
            physical_op_expr.clone(),
            vec![lit_expr(int(2))],
        )));
        let first_child_results =
            evaluate_and_collect(first_child_expr, engine.clone(), harness.clone()).await;

        // Access second child (orders parallel scan) using indexing
        let second_child_expr = Arc::new(Expr::new(Call(physical_op_expr, vec![lit_expr(int(3))])));
        let second_child_results = evaluate_and_collect(second_child_expr, engine, harness).await;

        // Check join method result
        assert_eq!(method_results.len(), 1);
        match &method_results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::String("hash".to_string()));
            }
            _ => panic!("Expected string literal for join method"),
        }

        // Check condition result
        assert_eq!(condition_results.len(), 1);
        match &condition_results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::String("id = id".to_string()));
            }
            _ => panic!("Expected string literal for condition"),
        }

        // Check first child result (customers index scan)
        assert_eq!(first_child_results.len(), 1);
        match &first_child_results[0].data {
            CoreData::Physical(Materializable::Materialized(phys_op)) => {
                assert_eq!(phys_op.operator.tag, "IndexScan");
                assert_eq!(phys_op.operator.data.len(), 1);
                match &phys_op.operator.data[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("customers".to_string()));
                    }
                    _ => panic!("Expected string literal for table name"),
                }
            }
            _ => panic!("Expected physical operator for first child"),
        }

        // Check second child result (orders parallel scan)
        assert_eq!(second_child_results.len(), 1);
        match &second_child_results[0].data {
            CoreData::Physical(Materializable::Materialized(phys_op)) => {
                assert_eq!(phys_op.operator.tag, "ParallelScan");
                assert_eq!(phys_op.operator.data.len(), 1);
                match &phys_op.operator.data[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("orders".to_string()));
                    }
                    _ => panic!("Expected string literal for table name"),
                }
            }
            _ => panic!("Expected physical operator for second child"),
        }
    }

    #[tokio::test]
    async fn test_unmaterialized_logical_operator_indexing() {
        let harness = TestHarness::new();
        let test_group_id = GroupId(1);

        // Register a logical operator in the test harness
        let materialized_join = create_logical_operator(
            "LogicalJoin",
            vec![
                lit_val(string("inner")),
                lit_val(string("customer.id = order.id")),
            ],
            vec![
                create_logical_operator("TableScan", vec![lit_val(string("customers"))], vec![]),
                create_logical_operator("TableScan", vec![lit_val(string("orders"))], vec![]),
            ],
        );

        harness.register_group(test_group_id, materialized_join);

        // Create an unmaterialized logical operator
        let unmaterialized_expr = Arc::new(Expr::new(CoreVal(Value::new(CoreData::Logical(
            Materializable::UnMaterialized(test_group_id),
        )))));

        // Access join type using indexing - should materialize and return "inner"
        let join_type_expr = Arc::new(Expr::new(Call(
            unmaterialized_expr.clone(),
            vec![lit_expr(int(0))],
        )));

        // Access condition using indexing - should materialize and return "customer.id = order.id"
        let condition_expr = Arc::new(Expr::new(Call(
            unmaterialized_expr.clone(),
            vec![lit_expr(int(1))],
        )));

        // Access first child (customers table scan) using indexing
        let first_child_expr = Arc::new(Expr::new(Call(
            unmaterialized_expr.clone(),
            vec![lit_expr(int(2))],
        )));

        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Evaluate the expressions
        let join_type_results =
            evaluate_and_collect(join_type_expr, engine.clone(), harness.clone()).await;
        let condition_results =
            evaluate_and_collect(condition_expr, engine.clone(), harness.clone()).await;
        let first_child_results = evaluate_and_collect(first_child_expr, engine, harness).await;

        // Check join type result
        assert_eq!(join_type_results.len(), 1);
        match &join_type_results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::String("inner".to_string()));
            }
            _ => panic!("Expected string literal for join type"),
        }

        // Check condition result
        assert_eq!(condition_results.len(), 1);
        match &condition_results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::String("customer.id = order.id".to_string()));
            }
            _ => panic!("Expected string literal for condition"),
        }

        // Check first child result (customers table scan)
        assert_eq!(first_child_results.len(), 1);
        match &first_child_results[0].data {
            CoreData::Logical(Materializable::Materialized(log_op)) => {
                assert_eq!(log_op.operator.tag, "TableScan");
                assert_eq!(log_op.operator.data.len(), 1);
                match &log_op.operator.data[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::String("customers".to_string()));
                    }
                    _ => panic!("Expected string literal for table name"),
                }
            }
            _ => panic!("Expected logical operator for first child"),
        }
    }

    #[tokio::test]
    async fn test_unmaterialized_physical_operator_indexing() {
        let harness = TestHarness::new();

        // Create a physical goal
        let test_group_id = GroupId(2);
        let properties = Box::new(Value::new(CoreData::Literal(string("sorted"))));
        let test_goal = Goal {
            group_id: test_group_id,
            properties,
        };

        // Register a physical operator to be returned when the goal is expanded
        let materialized_join = create_physical_operator(
            "MergeJoin",
            vec![
                lit_val(string("merge")),
                lit_val(string("customer.id = order.id")),
            ],
            vec![
                create_physical_operator("SortedScan", vec![lit_val(string("customers"))], vec![]),
                create_physical_operator("SortedScan", vec![lit_val(string("orders"))], vec![]),
            ],
        );

        harness.register_goal(&test_goal, materialized_join);

        // Create an unmaterialized physical operator
        let unmaterialized_expr = Arc::new(Expr::new(CoreVal(Value::new(CoreData::Physical(
            Materializable::UnMaterialized(test_goal),
        )))));

        // Access join method using indexing - should materialize and return "merge"
        let method_expr = Arc::new(Expr::new(Call(
            unmaterialized_expr.clone(),
            vec![lit_expr(int(0))],
        )));

        // Access condition using indexing - should materialize and return "customer.id = order.id"
        let condition_expr = Arc::new(Expr::new(Call(
            unmaterialized_expr.clone(),
            vec![lit_expr(int(1))],
        )));

        // Access first child (customers scan) using indexing
        let first_child_expr = Arc::new(Expr::new(Call(
            unmaterialized_expr.clone(),
            vec![lit_expr(int(2))],
        )));

        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Evaluate the expressions
        let method_results =
            evaluate_and_collect(method_expr, engine.clone(), harness.clone()).await;
        let condition_results =
            evaluate_and_collect(condition_expr, engine.clone(), harness.clone()).await;
        let first_child_results = evaluate_and_collect(first_child_expr, engine, harness).await;

        // Check join method result
        assert_eq!(method_results.len(), 1);
        match &method_results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::String("merge".to_string()));
            }
            _ => panic!("Expected string literal for join method"),
        }

        // Check condition result
        assert_eq!(condition_results.len(), 1);
        match &condition_results[0].data {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::String("customer.id = order.id".to_string()));
            }
            _ => panic!("Expected string literal for condition"),
        }

        // Check first child result (customers scan)
        assert_eq!(first_child_results.len(), 1);
        match &first_child_results[0].data {
            CoreData::Physical(Materializable::Materialized(phys_op)) => {
                assert_eq!(phys_op.operator.tag, "SortedScan");
                assert_eq!(phys_op.operator.data.len(), 1);
                match &phys_op.operator.data[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(*lit, Literal::String("customers".to_string()));
                    }
                    _ => panic!("Expected string literal for table name"),
                }
            }
            _ => panic!("Expected physical operator for first child"),
        }
    }

    #[tokio::test]
    async fn test_nested_operator_indexing() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Create a nested operator:
        // Project [col1, col2] (
        //   Filter ("age > 30") (
        //     Join ("inner", "t1.id = t2.id") (
        //       TableScan ("customers"),
        //       TableScan ("orders")
        //     )
        //   )
        // )
        let nested_op = create_logical_operator(
            "Project",
            vec![array_val(vec![
                lit_val(string("col1")),
                lit_val(string("col2")),
            ])],
            vec![create_logical_operator(
                "Filter",
                vec![lit_val(string("age > 30"))],
                vec![create_logical_operator(
                    "Join",
                    vec![lit_val(string("inner")), lit_val(string("t1.id = t2.id"))],
                    vec![
                        create_logical_operator(
                            "TableScan",
                            vec![lit_val(string("customers"))],
                            vec![],
                        ),
                        create_logical_operator(
                            "TableScan",
                            vec![lit_val(string("orders"))],
                            vec![],
                        ),
                    ],
                )],
            )],
        );

        let nested_op_expr = Arc::new(Expr::new(CoreVal(nested_op)));

        // First, access the Filter child of Project (index 1)
        let filter_expr = Arc::new(Expr::new(Call(nested_op_expr, vec![lit_expr(int(1))])));
        let filter_results =
            evaluate_and_collect(filter_expr, engine.clone(), harness.clone()).await;

        // Now, from the Filter, access its Join child (index 1)
        let filter_value = filter_results[0].clone();
        let join_expr = Arc::new(Expr::new(Call(
            Arc::new(Expr::new(CoreVal(filter_value))),
            vec![lit_expr(int(1))],
        )));
        let join_results = evaluate_and_collect(join_expr, engine.clone(), harness.clone()).await;

        // Finally, from the Join, access its first TableScan child (index 2)
        let join_value = join_results[0].clone();
        let table_scan_expr = Arc::new(Expr::new(Call(
            Arc::new(Expr::new(CoreVal(join_value))),
            vec![lit_expr(int(2))],
        )));
        let table_scan_results = evaluate_and_collect(table_scan_expr, engine, harness).await;

        // Verify the final result is the "customers" TableScan
        assert_eq!(table_scan_results.len(), 1);
        match &table_scan_results[0].data {
            CoreData::Logical(Materializable::Materialized(log_op)) => {
                assert_eq!(log_op.operator.tag, "TableScan");
                assert_eq!(log_op.operator.data.len(), 1);
                match &log_op.operator.data[0].data {
                    CoreData::Literal(lit) => {
                        assert_eq!(*lit, Literal::String("customers".to_string()));
                    }
                    _ => panic!("Expected string literal for table name"),
                }
            }
            _ => panic!("Expected logical operator for table scan"),
        }
    }

    #[tokio::test]
    async fn test_variable_references() {
        let harness = TestHarness::new();

        // Test that variables from outer scope are visible in inner scope
        let mut ctx = Context::default();
        ctx.bind("outer_var".to_string(), lit_val(int(100)));
        ctx.push_scope();
        ctx.bind("inner_var".to_string(), lit_val(int(200)));

        let catalog = memory_catalog();
        let engine = Engine::new(ctx, Arc::new(catalog));

        // Reference to a variable in the current (inner) scope
        let inner_ref = Arc::new(Expr::new(Ref("inner_var".to_string())));
        let inner_results = evaluate_and_collect(inner_ref, engine.clone(), harness.clone()).await;

        // Reference to a variable in the outer scope
        let outer_ref = Arc::new(Expr::new(Ref("outer_var".to_string())));
        let outer_results = evaluate_and_collect(outer_ref, engine.clone(), harness).await;

        // Check results
        match &inner_results[0].data {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 200);
            }
            _ => panic!("Expected integer value"),
        }

        match &outer_results[0].data {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 100);
            }
            _ => panic!("Expected integer value"),
        }
    }
}
