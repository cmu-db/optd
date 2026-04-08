use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use tokio::task::JoinSet;

use crate::{
    artifact::{QueryArtifact, collect_query_artifact, read_artifact, write_artifact},
    compare::{RunReport, compare_artifacts, render_markdown},
    manifest::load_tpch_queries,
};

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub workspace_root: PathBuf,
    pub suite: String,
    pub selections: Vec<String>,
    pub baseline_root: PathBuf,
    pub output_root: PathBuf,
}

impl RunConfig {
    pub fn new(
        workspace_root: impl Into<PathBuf>,
        suite: impl Into<String>,
        selections: Vec<String>,
    ) -> Self {
        let workspace_root = workspace_root.into();
        Self {
            baseline_root: workspace_root.join("tests/statsregtest/baselines"),
            output_root: workspace_root.join("target/statsregtest"),
            workspace_root,
            suite: suite.into(),
            selections,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplySummary {
    pub written: Vec<PathBuf>,
}

pub async fn apply_baselines(config: &RunConfig) -> Result<ApplySummary> {
    let artifacts = collect_suite_artifacts(config).await?;
    let mut written = Vec::new();

    for artifact in artifacts {
        let path = baseline_path(config, &artifact.query_id);
        write_artifact(&path, &artifact)?;
        written.push(path);
    }

    written.sort();
    Ok(ApplySummary { written })
}

pub async fn run_against_baselines(config: &RunConfig) -> Result<RunReport> {
    let artifacts = collect_suite_artifacts(config).await?;
    let mut comparisons = Vec::new();
    let mut missing_baselines = Vec::new();

    for artifact in artifacts {
        let baseline_path = baseline_path(config, &artifact.query_id);
        if baseline_path.exists() {
            let baseline = read_artifact(&baseline_path)?;
            comparisons.push(compare_artifacts(&baseline, &artifact));
        } else {
            missing_baselines.push(artifact.query_id.clone());
        }
    }

    comparisons.sort_by(|left, right| left.query_id.cmp(&right.query_id));
    missing_baselines.sort();

    Ok(RunReport {
        markdown: render_markdown(&missing_baselines, &comparisons),
        missing_baselines,
        comparisons,
    })
}

async fn collect_suite_artifacts(config: &RunConfig) -> Result<Vec<QueryArtifact>> {
    if config.suite != "tpch" {
        anyhow::bail!("unsupported suite {}", config.suite);
    }

    let queries = load_tpch_queries(&config.workspace_root, &config.selections)?;
    fs::create_dir_all(config.output_root.join(&config.suite))
        .with_context(|| format!("failed to create {}", config.output_root.display()))?;

    let mut join_set = JoinSet::new();
    for query in queries {
        let workspace_root = config.workspace_root.clone();
        join_set.spawn(async move { collect_query_artifact(&query, &workspace_root).await });
    }

    let mut artifacts = Vec::new();
    while let Some(result) = join_set.join_next().await {
        let artifact = result.context("stats collection task failed")??;
        let raw_output_path = output_path(config, &artifact.query_id);
        write_artifact(&raw_output_path, &artifact)?;
        artifacts.push(artifact);
    }

    artifacts.sort_by(|left, right| left.query_id.cmp(&right.query_id));
    Ok(artifacts)
}

fn baseline_path(config: &RunConfig, query_id: &str) -> PathBuf {
    config
        .baseline_root
        .join(&config.suite)
        .join(format!("{query_id}.json"))
}

fn output_path(config: &RunConfig, query_id: &str) -> PathBuf {
    config
        .output_root
        .join(&config.suite)
        .join(format!("{query_id}.json"))
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use anyhow::Result;
    use tempfile::tempdir;

    use crate::artifact::QueryArtifact;

    use super::RunConfig;

    #[test]
    fn config_uses_expected_default_roots() -> Result<()> {
        let dir = tempdir()?;
        let config = RunConfig::new(dir.path(), "tpch", vec!["q1".to_string()]);
        assert!(
            config
                .baseline_root
                .ends_with(Path::new("tests/statsregtest/baselines"))
        );
        assert!(
            config
                .output_root
                .ends_with(Path::new("target/statsregtest"))
        );
        Ok(())
    }

    #[test]
    fn artifact_round_trip_paths_are_deterministic() -> Result<()> {
        let dir = tempdir()?;
        let config = RunConfig::new(dir.path(), "tpch", vec![]);
        let suite_dir = config.output_root.join("tpch");
        fs::create_dir_all(&suite_dir)?;
        let path = super::output_path(&config, "q1");
        assert_eq!(path, suite_dir.join("q1.json"));
        let _: Option<QueryArtifact> = None;
        Ok(())
    }
}
