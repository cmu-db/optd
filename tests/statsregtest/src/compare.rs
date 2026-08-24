use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::artifact::QueryArtifact;

const Q_ERROR_ABS_EPSILON: f64 = 5e-4;
const Q_ERROR_REL_EPSILON: f64 = 1e-12;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunReport {
    pub missing_baselines: Vec<String>,
    pub comparisons: Vec<QueryComparison>,
    pub markdown: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryComparison {
    pub query_id: String,
    pub root: RootComparison,
    pub matched_nodes: usize,
    pub added_nodes: usize,
    pub removed_nodes: usize,
    pub operator_summaries: Vec<OperatorSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RootComparison {
    pub baseline_row_q_error: Option<f64>,
    pub current_row_q_error: Option<f64>,
    pub delta: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorSummary {
    pub operator_kind: String,
    pub matched_nodes: usize,
    pub improved_row_q_error: usize,
    pub regressed_row_q_error: usize,
    pub baseline_mean_row_q_error: Option<f64>,
    pub current_mean_row_q_error: Option<f64>,
    pub baseline_mean_distinct_q_error: Option<f64>,
    pub current_mean_distinct_q_error: Option<f64>,
}

pub fn compare_artifacts(baseline: &QueryArtifact, current: &QueryArtifact) -> QueryComparison {
    let matches = match_nodes(baseline, current);
    let root = RootComparison {
        baseline_row_q_error: baseline
            .nodes
            .first()
            .and_then(|node| node.error.row_count_q_error),
        current_row_q_error: current
            .nodes
            .first()
            .and_then(|node| node.error.row_count_q_error),
        delta: match (
            baseline
                .nodes
                .first()
                .and_then(|node| node.error.row_count_q_error),
            current
                .nodes
                .first()
                .and_then(|node| node.error.row_count_q_error),
        ) {
            (Some(baseline), Some(current)) => {
                let delta = current - baseline;
                Some(if q_error_delta_is_noise(baseline, current) {
                    0.0
                } else {
                    delta
                })
            }
            _ => None,
        },
    };

    let mut per_operator = BTreeMap::<String, Vec<MatchedNode>>::new();
    for matched in &matches.matched {
        per_operator
            .entry(matched.current.operator_kind.clone())
            .or_default()
            .push(matched.clone());
    }

    let operator_summaries = per_operator
        .into_iter()
        .map(|(operator_kind, nodes)| summarize_operator(operator_kind, &nodes))
        .collect();

    QueryComparison {
        query_id: current.query_id.clone(),
        root,
        matched_nodes: matches.matched.len(),
        added_nodes: matches.current_unmatched,
        removed_nodes: matches.baseline_unmatched,
        operator_summaries,
    }
}

pub fn render_markdown(missing_baselines: &[String], comparisons: &[QueryComparison]) -> String {
    let mut out = String::new();
    out.push_str("# Statistics Regression Report\n\n");
    if !missing_baselines.is_empty() {
        out.push_str("## Missing Baselines\n\n");
        for query_id in missing_baselines {
            out.push_str(&format!("- {}\n", query_id));
        }
        out.push('\n');
    }
    for comparison in comparisons {
        out.push_str(&format!("## {}\n\n", comparison.query_id));
        out.push_str(&format!(
            "- Root row q-error: {} -> {}{}\n",
            format_optional(comparison.root.baseline_row_q_error),
            format_optional(comparison.root.current_row_q_error),
            comparison
                .root
                .delta
                .map(|delta| format!(" (delta {delta:+.3})"))
                .unwrap_or_default()
        ));
        out.push_str(&format!(
            "- Matched nodes: {}, added: {}, removed: {}\n",
            comparison.matched_nodes, comparison.added_nodes, comparison.removed_nodes
        ));

        if comparison.operator_summaries.is_empty() {
            out.push_str("- No uniquely matched operator fingerprints.\n\n");
            continue;
        }

        out.push_str(
            "| Operator | Matched | Improved | Regressed | Row q-error | Distinct q-error |\n",
        );
        out.push_str("| --- | ---: | ---: | ---: | --- | --- |\n");
        for summary in &comparison.operator_summaries {
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} -> {} | {} -> {} |\n",
                summary.operator_kind,
                summary.matched_nodes,
                summary.improved_row_q_error,
                summary.regressed_row_q_error,
                format_optional(summary.baseline_mean_row_q_error),
                format_optional(summary.current_mean_row_q_error),
                format_optional(summary.baseline_mean_distinct_q_error),
                format_optional(summary.current_mean_distinct_q_error),
            ));
        }
        out.push('\n');
    }
    out
}

fn summarize_operator(operator_kind: String, nodes: &[MatchedNode]) -> OperatorSummary {
    let mut improved_row_q_error = 0;
    let mut regressed_row_q_error = 0;
    let mut baseline_row_values = Vec::new();
    let mut current_row_values = Vec::new();
    let mut baseline_distinct_values = Vec::new();
    let mut current_distinct_values = Vec::new();

    for node in nodes {
        if let (Some(baseline), Some(current)) = (
            node.baseline.error.row_count_q_error,
            node.current.error.row_count_q_error,
        ) {
            baseline_row_values.push(baseline);
            current_row_values.push(current);
            let delta = current - baseline;
            if delta < 0.0 && !q_error_delta_is_noise(baseline, current) {
                improved_row_q_error += 1;
            } else if delta > 0.0 && !q_error_delta_is_noise(baseline, current) {
                regressed_row_q_error += 1;
            }
        }

        baseline_distinct_values.extend(
            node.baseline
                .error
                .columns
                .iter()
                .filter_map(|column| column.distinct_count_q_error),
        );
        current_distinct_values.extend(
            node.current
                .error
                .columns
                .iter()
                .filter_map(|column| column.distinct_count_q_error),
        );
    }

    OperatorSummary {
        operator_kind,
        matched_nodes: nodes.len(),
        improved_row_q_error,
        regressed_row_q_error,
        baseline_mean_row_q_error: mean(&baseline_row_values),
        current_mean_row_q_error: mean(&current_row_values),
        baseline_mean_distinct_q_error: mean(&baseline_distinct_values),
        current_mean_distinct_q_error: mean(&current_distinct_values),
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn q_error_delta_is_noise(baseline: f64, current: f64) -> bool {
    let scale = baseline.abs().max(current.abs());
    let tolerance = Q_ERROR_ABS_EPSILON.max(scale * Q_ERROR_REL_EPSILON);
    (current - baseline).abs() <= tolerance
}

fn format_optional(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.3}"))
        .unwrap_or_else(|| "n/a".to_string())
}

#[derive(Debug, Clone)]
struct MatchResults<'a> {
    matched: Vec<MatchedNode<'a>>,
    baseline_unmatched: usize,
    current_unmatched: usize,
}

#[derive(Debug, Clone)]
struct MatchedNode<'a> {
    baseline: &'a crate::artifact::NodeArtifact,
    current: &'a crate::artifact::NodeArtifact,
}

fn match_nodes<'a>(baseline: &'a QueryArtifact, current: &'a QueryArtifact) -> MatchResults<'a> {
    let mut baseline_by_key =
        HashMap::<(String, String), Vec<&crate::artifact::NodeArtifact>>::new();
    let mut current_by_key =
        HashMap::<(String, String), Vec<&crate::artifact::NodeArtifact>>::new();

    for node in &baseline.nodes {
        baseline_by_key
            .entry((node.operator_kind.clone(), node.subtree_fingerprint.clone()))
            .or_default()
            .push(node);
    }
    for node in &current.nodes {
        current_by_key
            .entry((node.operator_kind.clone(), node.subtree_fingerprint.clone()))
            .or_default()
            .push(node);
    }

    let mut matched = Vec::new();
    let mut baseline_unmatched = 0;
    let mut current_unmatched = 0;

    for (key, baseline_nodes) in &baseline_by_key {
        match current_by_key.get(key) {
            Some(current_nodes) if baseline_nodes.len() == 1 && current_nodes.len() == 1 => {
                matched.push(MatchedNode {
                    baseline: baseline_nodes[0],
                    current: current_nodes[0],
                });
            }
            Some(current_nodes) => {
                baseline_unmatched += baseline_nodes.len();
                current_unmatched += current_nodes.len();
            }
            None => baseline_unmatched += baseline_nodes.len(),
        }
    }

    for (key, current_nodes) in &current_by_key {
        if !baseline_by_key.contains_key(key) {
            current_unmatched += current_nodes.len();
        }
    }

    MatchResults {
        matched,
        baseline_unmatched,
        current_unmatched,
    }
}

#[cfg(test)]
mod tests {
    use crate::artifact::{
        ColumnErrorMetrics, EstimatedColumnStats, EstimatedNodeStats, ExactColumnStats,
        ExactNodeStats, NodeArtifact, NodeErrorMetrics, OutputColumn, QueryArtifact,
    };

    use super::compare_artifacts;

    fn make_artifact(
        query_id: &str,
        operator_kind: &str,
        fingerprint: &str,
        row_q_error: Option<f64>,
    ) -> QueryArtifact {
        QueryArtifact {
            suite: "tpch".to_string(),
            query_id: query_id.to_string(),
            desc: None,
            sql_hash: "sql".to_string(),
            dataset_fingerprint: "data".to_string(),
            plan_hash: "plan".to_string(),
            nodes: vec![NodeArtifact {
                node_path: "0".to_string(),
                operator_kind: operator_kind.to_string(),
                short_explain: operator_kind.to_string(),
                subtree_fingerprint: fingerprint.to_string(),
                output_schema: vec![OutputColumn {
                    ordinal: 0,
                    relation: "t".to_string(),
                    name: "c".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: true,
                }],
                estimated: EstimatedNodeStats {
                    row_count: Some(10.0),
                    columns: vec![EstimatedColumnStats {
                        null_count: None,
                        distinct_count: None,
                        min_value: None,
                        max_value: None,
                    }],
                },
                exact: ExactNodeStats {
                    row_count: Some(10),
                    columns: vec![ExactColumnStats {
                        null_count: None,
                        distinct_count: None,
                        min_value: None,
                        max_value: None,
                    }],
                },
                error: NodeErrorMetrics {
                    row_count_q_error: row_q_error,
                    columns: vec![ColumnErrorMetrics {
                        distinct_count_q_error: None,
                        null_fraction_abs_error: None,
                        min_value_match: None,
                        max_value_match: None,
                    }],
                },
            }],
        }
    }

    #[test]
    fn compares_unique_fingerprint_matches() {
        let baseline = make_artifact("q1", "Join", "abc", Some(4.0));
        let current = make_artifact("q1", "Join", "abc", Some(2.0));

        let comparison = compare_artifacts(&baseline, &current);
        assert_eq!(comparison.matched_nodes, 1);
        assert_eq!(comparison.added_nodes, 0);
        assert_eq!(comparison.removed_nodes, 0);
        assert_eq!(comparison.operator_summaries[0].improved_row_q_error, 1);
    }

    #[test]
    fn ignores_tiny_q_error_deltas() {
        let baseline = make_artifact("q1", "Join", "abc", Some(2.0));
        let current = make_artifact("q1", "Join", "abc", Some(2.0001));

        let comparison = compare_artifacts(&baseline, &current);
        assert_eq!(comparison.root.delta, Some(0.0));
        assert_eq!(comparison.operator_summaries[0].improved_row_q_error, 0);
        assert_eq!(comparison.operator_summaries[0].regressed_row_q_error, 0);
    }

    #[test]
    fn marks_added_and_removed_when_fingerprints_change() {
        let baseline = make_artifact("q1", "Join", "abc", Some(4.0));
        let current = make_artifact("q1", "Join", "xyz", Some(2.0));

        let comparison = compare_artifacts(&baseline, &current);
        assert_eq!(comparison.matched_nodes, 0);
        assert_eq!(comparison.added_nodes, 1);
        assert_eq!(comparison.removed_nodes, 1);
    }
}
