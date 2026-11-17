//! Conservative SQL template extraction. Templates group history, never cache plans.
mod tracking;
use std::ops::ControlFlow;
pub use tracking::{CapturePolicy, HistoryIssue, QueryHistoryOptions, QueryHistoryReport};
pub(crate) use tracking::{QueryHistory, RequestHistory};

use datafusion::{
    error::Result,
    execution::session_state::SessionState,
    sql::{
        parser::Statement as DfStatement,
        sqlparser::ast::{
            BinaryOperator, Expr, Query, SetExpr, Statement, UnaryOperator, Value, Visit, VisitMut,
            Visitor, VisitorMut,
        },
    },
};
use optd_catalog::query_history::QueryTemplate;
use serde_json::json;

/// Normalizer/serializer identity. Bump when extraction or canonical SQL changes,
/// including incompatible changes to the pinned sqlparser 0.61 Display serializer.
pub const NORMALIZATION_VERSION: i32 = 1;

/// Values extracted from a copy of the request. Callers must explicitly opt in to
/// persisting these strings; template-only history must discard them.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedSql {
    pub template: QueryTemplate,
    pub parameters: Vec<String>,
}

/// Uses the same dialect and parser limits as DataFusion. Non-query statements and
/// existing bind parameters are conservatively unhandled in v1, not rewritten.
/// Syntax errors are returned for classification; never replace the actual query's
/// error or attempt to execute this template.
pub fn normalize_sql(
    state: &SessionState,
    sql: &str,
    resolution_scope: serde_json::Value,
) -> Result<Option<NormalizedSql>> {
    let dialect = &state.config_options().sql_parser.dialect;
    let DfStatement::Statement(mut statement) = state.sql_to_statement(sql, dialect)? else {
        return Ok(None);
    };
    if !matches!(statement.as_ref(), Statement::Query(_)) {
        return Ok(None);
    }
    // Internal slots cannot collide with caller bind names. Supporting bindings
    // later requires a separate canonical identity preserving their sharing.
    if Visit::visit(statement.as_ref(), &mut FindBinding).is_break() {
        return Ok(None);
    }
    let mut extractor = Extractor::default();
    let _ = VisitMut::visit(statement.as_mut(), &mut extractor);
    Ok(Some(NormalizedSql {
        template: QueryTemplate {
            normalization_version: NORMALIZATION_VERSION,
            dialect: dialect.to_string(),
            resolution_scope,
            template: statement.to_string(),
            parameter_shape: json!(extractor.kinds),
        },
        parameters: extractor.parameters,
    }))
}

struct FindBinding;
impl Visitor for FindBinding {
    type Break = ();
    fn pre_visit_value(&mut self, value: &Value) -> ControlFlow<Self::Break> {
        if matches!(value, Value::Placeholder(_)) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}

#[derive(Default)]
struct Extractor {
    parameters: Vec<String>,
    kinds: Vec<&'static str>,
}
impl VisitorMut for Extractor {
    type Break = ();
    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        self.query_body(&mut query.body);
        ControlFlow::Continue(())
    }
}
impl Extractor {
    fn query_body(&mut self, body: &mut SetExpr) {
        match body {
            SetExpr::Select(select) => {
                for expr in [&mut select.selection, &mut select.having]
                    .into_iter()
                    .flatten()
                {
                    self.predicate(expr);
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.query_body(left);
                self.query_body(right);
            }
            // Nested queries/CTEs are visited by VisitorMut, without treating
            // their projections, ordinals or function arguments as predicates.
            _ => {}
        }
    }
    fn predicate(&mut self, expr: &mut Expr) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And | BinaryOperator::Or,
                right,
            } => {
                self.predicate(left);
                self.predicate(right);
            }
            Expr::BinaryOp {
                left,
                op:
                    BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq,
                right,
            } => {
                self.literal(left);
                self.literal(right);
            }
            Expr::Nested(inner)
            | Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: inner,
            } => {
                self.predicate(inner);
            }
            _ => {}
        }
    }
    fn literal(&mut self, expr: &mut Expr) {
        let Expr::Value(value) = expr else {
            return;
        };
        let kind = match &value.value {
            Value::Number(number, false) if number.bytes().all(|b| b.is_ascii_digit()) => {
                "integer_literal"
            }
            Value::Number(_, false) => "number_literal",
            Value::SingleQuotedString(_) => "string_literal",
            Value::Boolean(_) => "boolean_literal",
            _ => return,
        };
        self.parameters.push(value.value.to_string());
        self.kinds.push(kind);
        // Caller placeholders were rejected before mutation; identifiers and string
        // contents are represented differently by SQL AST Display.
        value.value = Value::Placeholder(format!("$optd_literal_{}", self.parameters.len()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    fn normalize(sql: &str) -> Option<NormalizedSql> {
        normalize_sql(
            &SessionContext::new().state(),
            sql,
            json!({"catalog_id": 0, "schema_id": 1}),
        )
        .unwrap()
    }

    #[test]
    fn predicate_values_group_without_affecting_the_original() {
        let sql = "select * from t where a = 1 and b > 'old'";
        let a = normalize(sql).unwrap();
        let b = normalize("-- comment\nSELECT * FROM t WHERE a=2 AND b>'new'").unwrap();
        assert_eq!(a.template, b.template);
        assert_eq!(
            a.template.fingerprint().unwrap(),
            b.template.fingerprint().unwrap()
        );
        assert_ne!(a.parameters, b.parameters);
        assert_eq!(sql, "select * from t where a = 1 and b > 'old'");
        assert_eq!(
            a.template.parameter_shape,
            json!(["integer_literal", "string_literal"])
        );
    }

    #[test]
    fn occurrences_do_not_deduplicate_by_equal_values() {
        let a = normalize("SELECT * FROM t WHERE a = 1 AND b = 1").unwrap();
        let b = normalize("SELECT * FROM t WHERE a = 1 AND b = 2").unwrap();
        assert_eq!(a.template, b.template);
        assert_eq!(a.parameters, vec!["1", "1"]);
        assert_eq!(b.parameters, vec!["1", "2"]);
    }

    #[test]
    fn structural_values_and_unhandled_expressions_are_retained() {
        let a = normalize("SELECT a = 1 FROM t WHERE a = 42 ORDER BY 1 LIMIT 10").unwrap();
        assert!(a.template.template.contains("SELECT a = 1"));
        assert!(a.template.template.contains("ORDER BY 1"));
        assert!(a.template.template.contains("LIMIT 10"));
        assert_eq!(a.parameters, vec!["42"]);
        let b = normalize("SELECT CAST(a AS DECIMAL(10, 2)) FROM t WHERE a IS NULL").unwrap();
        assert!(b.parameters.is_empty());
        assert!(b.template.template.contains("DECIMAL(10,2)"));
        assert!(b.template.template.contains("IS NULL"));
        let c = normalize("SELECT * FROM t WHERE substring(a, 1, 2) = 'x'").unwrap();
        assert_eq!(c.parameters, vec!["'x'"]);
        assert!(c.template.template.contains("1"));
        assert!(c.template.template.contains("2"));
    }

    #[test]
    fn nested_queries_having_and_unions_have_stable_slots() {
        let a = normalize("SELECT * FROM (SELECT a FROM t WHERE a > 1) q WHERE a < 2 UNION SELECT a FROM t GROUP BY a HAVING a > 3").unwrap();
        let b = normalize("SELECT * FROM (SELECT a FROM t WHERE a > 4) q WHERE a < 5 UNION SELECT a FROM t GROUP BY a HAVING a > 6").unwrap();
        assert_eq!(a.template, b.template);
        assert_eq!(a.parameters.len(), 3);
        assert_eq!(a.template.parameter_shape.as_array().unwrap().len(), 3);
    }

    #[test]
    fn scopes_types_quoting_and_structural_values_separate_templates() {
        let a = normalize("SELECT * FROM t WHERE a = 1").unwrap();
        let b = normalize("SELECT * FROM t WHERE a = '1'").unwrap();
        assert_ne!(
            a.template.fingerprint().unwrap(),
            b.template.fingerprint().unwrap()
        );
        let quoted = normalize("SELECT * FROM \"T\" WHERE a = 1").unwrap();
        assert_ne!(a.template, quoted.template);
        let a = normalize("SELECT * FROM t LIMIT 1").unwrap();
        let b = normalize("SELECT * FROM t LIMIT 2").unwrap();
        assert_ne!(a.template, b.template);
        let other = normalize_sql(
            &SessionContext::new().state(),
            "SELECT * FROM t LIMIT 1",
            json!({"catalog_id": 1, "schema_id": 1}),
        )
        .unwrap()
        .unwrap();
        assert_ne!(
            a.template.fingerprint().unwrap(),
            other.template.fingerprint().unwrap()
        );
    }

    #[test]
    fn unsupported_bindings_and_nonqueries_are_not_reinterpreted() {
        assert!(normalize("SELECT * FROM t WHERE a = $1 AND b = $1").is_none());
        assert!(normalize("SELECT * FROM t WHERE a = $optd_literal_1").is_none());
        assert!(normalize("CREATE TABLE t (a INT)").is_none());
        assert!(normalize("SELECT '$optd_literal_1' FROM t WHERE a = 1").is_some());
        assert!(normalize_sql(&SessionContext::new().state(), "SELECT FROM", json!({})).is_err());
    }
}
