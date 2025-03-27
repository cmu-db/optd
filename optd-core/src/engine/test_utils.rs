use crate::engine::{Continuation, Engine, Evaluate, generator::Generator};
use optd_dsl::analyzer::hir::{
    CoreData, Expr, Goal, GroupId, Literal, LogicalOp, MatchArm, Materializable, Operator, Pattern,
    PhysicalOp, Value,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A configurable mock generator for testing the evaluation engine.
#[derive(Clone)]
pub struct MockGenerator {
    /// Maps group IDs to their materialized values.
    group_mappings: Arc<Mutex<HashMap<String, Vec<Value>>>>,

    /// Maps goals to their implementations.
    goal_mappings: Arc<Mutex<HashMap<String, Vec<Value>>>>,
}

impl MockGenerator {
    /// Creates a new empty mock generator.
    pub fn new() -> Self {
        Self {
            group_mappings: Arc::new(Mutex::new(HashMap::new())),
            goal_mappings: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Registers a logical operator value to be returned when a specific group is requested.
    pub fn register_group(&self, group_id: GroupId, value: Value) {
        let key = format!("{:?}", group_id);
        let mut mappings = self.group_mappings.lock().unwrap();
        mappings.entry(key).or_default().push(value);
    }

    /// Registers a physical operator value to be returned when a specific goal is requested.
    pub fn register_goal(&self, goal: &Goal, value: Value) {
        let key = format!("{:?}:{:?}", goal.group_id, goal.properties);
        let mut mappings = self.goal_mappings.lock().unwrap();
        mappings.entry(key).or_default().push(value);
    }
}

impl Generator for MockGenerator {
    async fn yield_group(&self, group_id: GroupId, k: Continuation<Value>) {
        let key = format!("{:?}", group_id);
        let values = {
            let mappings = self.group_mappings.lock().unwrap();
            mappings.get(&key).cloned().unwrap_or_default()
        };

        for value in values {
            k(value.clone()).await;
        }
    }

    async fn yield_goal(&self, physical_goal: &Goal, k: Continuation<Value>) {
        let key = format!(
            "{:?}:{:?}",
            physical_goal.group_id, physical_goal.properties
        );

        let values = {
            let mappings = self.goal_mappings.lock().unwrap();
            mappings.get(&key).cloned().unwrap_or_default()
        };

        for value in values {
            k(value.clone()).await;
        }
    }
}

/// Helper to create a literal expression.
pub fn lit_expr(literal: Literal) -> Arc<Expr> {
    Arc::new(Expr::CoreExpr(CoreData::Literal(literal)))
}

/// Helper to create a literal value.
pub fn lit_val(literal: Literal) -> Value {
    Value(CoreData::Literal(literal))
}

/// Helper to create an integer literal.
pub fn int(i: i64) -> Literal {
    Literal::Int64(i)
}

/// Helper to create a string literal.
pub fn string(s: &str) -> Literal {
    Literal::String(s.to_string())
}

/// Helper to create a boolean literal.
pub fn boolean(b: bool) -> Literal {
    Literal::Bool(b)
}

/// Helper to create a reference expression.
pub fn ref_expr(name: &str) -> Arc<Expr> {
    Arc::new(Expr::Ref(name.to_string()))
}

/// Helper to create a pattern match arm.
pub fn match_arm(pattern: Pattern, expr: Arc<Expr>) -> MatchArm {
    MatchArm { pattern, expr }
}

/// Helper to create an array value.
pub fn array_val(items: Vec<Value>) -> Value {
    Value(CoreData::Array(items))
}

/// Helper to create a struct value.
pub fn struct_val(name: &str, fields: Vec<Value>) -> Value {
    Value(CoreData::Struct(name.to_string(), fields))
}

/// Helper to create a pattern matching expression.
pub fn pattern_match_expr(expr: Arc<Expr>, arms: Vec<MatchArm>) -> Arc<Expr> {
    Arc::new(Expr::PatternMatch(expr, arms))
}

/// Helper to create a bind pattern.
pub fn bind_pattern(name: &str, inner: Pattern) -> Pattern {
    Pattern::Bind(name.to_string(), Box::new(inner))
}

/// Helper to create a wildcard pattern.
pub fn wildcard_pattern() -> Pattern {
    Pattern::Wildcard
}

/// Helper to create a literal pattern.
pub fn literal_pattern(lit: Literal) -> Pattern {
    Pattern::Literal(lit)
}

/// Helper to create a struct pattern.
pub fn struct_pattern(name: &str, fields: Vec<Pattern>) -> Pattern {
    Pattern::Struct(name.to_string(), fields)
}

/// Helper to create an array decomposition pattern.
pub fn array_decomp_pattern(head: Pattern, tail: Pattern) -> Pattern {
    Pattern::ArrayDecomp(Box::new(head), Box::new(tail))
}

/// Helper to create an operator pattern.
pub fn operator_pattern(tag: &str, data: Vec<Pattern>, children: Vec<Pattern>) -> Pattern {
    Pattern::Operator(Operator {
        tag: tag.to_string(),
        data,
        children,
    })
}

/// Helper to create a simple logical operator value.
pub fn create_logical_operator(tag: &str, data: Vec<Value>, children: Vec<Value>) -> Value {
    let op = Operator {
        tag: tag.to_string(),
        data,
        children,
    };

    Value(CoreData::Logical(LogicalOp(Materializable::Materialized(
        op,
    ))))
}

/// Helper to create a simple physical operator value.
pub fn create_physical_operator(tag: &str, data: Vec<Value>, children: Vec<Value>) -> Value {
    let op = Operator {
        tag: tag.to_string(),
        data,
        children,
    };

    Value(CoreData::Physical(PhysicalOp(
        Materializable::Materialized(op),
    )))
}

/// Runs a test by evaluating the expression and collecting all results.
pub async fn evaluate_and_collect<G>(expr: Arc<Expr>, engine: Engine<G>) -> Vec<Value>
where
    G: Generator,
{
    let results = Arc::new(Mutex::new(Vec::new()));
    let results_clone = results.clone();

    expr.evaluate(
        engine,
        Arc::new(move |value| {
            let results = results_clone.clone();
            Box::pin(async move {
                let mut results_guard = results.lock().unwrap();
                results_guard.push(value);
            })
        }),
    )
    .await;

    Arc::try_unwrap(results).unwrap().into_inner().unwrap()
}
