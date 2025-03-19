use optd_dsl::analyzer::{
    context::Context,
    hir::{Expr::*, *},
};
use std::{collections::HashMap, sync::Arc};

mod rules;

pub fn generate_simple_commute_hir() -> HIR {
    let join_commute_rule = Arc::new(rules::join_commute());

    // Define the pattern for matching a Join operator
    let join_pattern = Pattern::Operator(Operator {
        tag: "Join".to_string(),
        data: vec![
            Pattern::Bind("left".to_string(), Box::new(Pattern::Wildcard)),
            Pattern::Bind("right".to_string(), Box::new(Pattern::Wildcard)),
            Pattern::Literal(Literal::String("Inner".to_string())),
            Pattern::Bind("cond".to_string(), Box::new(Pattern::Wildcard)),
        ],
        children: vec![],
    });

    // Create the match arms
    let match_arm = MatchArm {
        expr: join_commute_rule,
        pattern: join_pattern,
    };

    // Default case - return error
    let default_arm = MatchArm {
        pattern: Pattern::Wildcard,
        expr: CoreExpr(CoreData::Fail(Box::new(
            CoreExpr(CoreData::Literal(Literal::String(
                "could not apply rule".to_string(),
            )))
            .into(),
        )))
        .into(),
    };

    // Create initial bindings for the context
    let mut initial_bindings = HashMap::new();

    // Helper functions needed by the rule
    initial_bindings.insert(
        "schema_len".to_string(),
        Value(CoreData::Function(FunKind::RustUDF(|_args| {
            // Implementation would extract schema length from relation
            Value(CoreData::Literal(Literal::Int64(8))) // Placeholder
        }))),
    );

    initial_bindings.insert(
        "to_map".to_string(),
        Value(CoreData::Function(FunKind::RustUDF(|_args| {
            // Implementation would convert a list of tuples to a map
            Value(CoreData::Map(vec![])) // Placeholder
        }))),
    );

    initial_bindings.insert(
        "remap".to_string(),
        Value(CoreData::Function(FunKind::RustUDF(|args| {
            // Implementation would rewrite column references using the mapping
            args[0].clone() // Placeholder that just returns the input
        }))),
    );

    initial_bindings.insert(
        "to_array".to_string(),
        Value(CoreData::Function(FunKind::RustUDF(|_args| {
            // Implementation would convert to an array
            Value(CoreData::Array(vec![])) // Placeholder
        }))),
    );

    // Add the rule function to the context
    initial_bindings.insert(
        "join_commute".to_string(),
        Value(CoreData::Function(FunKind::Closure(
            vec!["expr".to_string()],
            PatternMatch(Ref("expr".to_string()).into(), vec![match_arm, default_arm]).into(),
        ))),
    );

    // Add the join_commute rule to HIR context with annotation
    let mut annotations = HashMap::new();
    annotations.insert("join_commute".to_string(), vec!["rule".to_string()]);

    // Create the context using the Context implementation
    let context = Context::new(initial_bindings);

    // Create the HIR program
    HIR {
        context,
        annotations,
    }
}
