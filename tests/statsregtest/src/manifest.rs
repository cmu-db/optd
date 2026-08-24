use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupSql {
    pub source: String,
    pub sql: String,
}

#[derive(Debug, Clone)]
pub struct BenchmarkQuery {
    pub suite: String,
    pub query_id: String,
    pub desc: Option<String>,
    pub sql: String,
    pub before: Vec<SetupSql>,
    pub manifest_path: PathBuf,
}

impl BenchmarkQuery {
    pub fn harness_setup_sql(&self, workspace_root: &Path) -> Result<Vec<SetupSql>> {
        let mut setup = Vec::new();
        let mut schema_sql = None;
        for item in &self.before {
            if item.source.ends_with("schema.sql") {
                schema_sql = Some(item.sql.clone());
            } else {
                setup.push(item.clone());
            }
        }

        if let Some(schema_sql) = schema_sql {
            setup.extend(externalize_tpch_schema(workspace_root, &schema_sql)?);
        }
        Ok(setup)
    }
}

#[derive(Debug, Deserialize)]
struct RawCase {
    sql: String,
    desc: Option<String>,
    before: Option<Vec<String>>,
}

pub fn load_tpch_queries(
    workspace_root: &Path,
    selections: &[String],
) -> Result<Vec<BenchmarkQuery>> {
    let manifest_root = workspace_root.join("tests/sqlplannertest/tests/tpch");
    let mut paths = fs::read_dir(&manifest_root)
        .with_context(|| format!("failed to read {}", manifest_root.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("yml"))
        .collect::<Vec<_>>();
    paths.sort();

    let selected = selections.iter().cloned().collect::<HashSet<_>>();
    let mut queries = Vec::new();
    for path in paths {
        let query_id = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .context("missing file stem")?
            .to_string();
        if !selected.is_empty() && !selected.contains(&query_id) {
            continue;
        }
        queries.push(load_query_manifest(&path, "tpch")?);
    }

    if queries.is_empty() {
        bail!("no TPC-H queries matched selections {selections:?}");
    }

    Ok(queries)
}

pub fn split_sql_statements(sql: &str) -> Vec<String> {
    sql.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(|statement| statement.to_string())
        .collect()
}

fn externalize_tpch_schema(workspace_root: &Path, schema_sql: &str) -> Result<Vec<SetupSql>> {
    let data_root = workspace_root.join("data/tpch");
    split_sql_statements(schema_sql)
        .into_iter()
        .map(|statement| {
            let create_prefix = "CREATE TABLE";
            let trimmed = statement.trim();
            let upper = trimmed.to_ascii_uppercase();
            if !upper.starts_with(create_prefix) {
                bail!("unsupported schema statement: {trimmed}");
            }

            let remainder = trimmed[create_prefix.len()..].trim_start();
            let open_paren = remainder
                .find('(')
                .with_context(|| format!("missing column list in {trimmed}"))?;
            let table_name = remainder[..open_paren].trim().to_ascii_lowercase();
            let columns = remainder[open_paren..].trim();
            let location = data_root.join(format!("{table_name}.parquet"));

            Ok(SetupSql {
                source: format!("externalized:{table_name}"),
                sql: format!(
                    "CREATE EXTERNAL TABLE {table_name} {columns} STORED AS PARQUET LOCATION '{}'",
                    location.display()
                ),
            })
        })
        .collect()
}

fn load_query_manifest(path: &Path, suite: &str) -> Result<BenchmarkQuery> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let cases = serde_yaml::from_str::<Vec<RawCase>>(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    let first = cases
        .into_iter()
        .next()
        .with_context(|| format!("no cases found in {}", path.display()))?;

    let before = first
        .before
        .unwrap_or_default()
        .into_iter()
        .map(|entry| expand_before_entry(path.parent().unwrap_or_else(|| Path::new(".")), entry))
        .collect::<Result<Vec<_>>>()?;

    Ok(BenchmarkQuery {
        suite: suite.to_string(),
        query_id: path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .context("missing file stem")?
            .to_string(),
        desc: first.desc,
        sql: first.sql,
        before,
        manifest_path: path.to_path_buf(),
    })
}

fn expand_before_entry(manifest_dir: &Path, entry: String) -> Result<SetupSql> {
    if let Some(relative) = entry.strip_prefix("include_sql:") {
        let include_path = manifest_dir.join(relative);
        let sql = fs::read_to_string(&include_path)
            .with_context(|| format!("failed to read {}", include_path.display()))?;
        return Ok(SetupSql {
            source: include_path.display().to_string(),
            sql,
        });
    }

    Ok(SetupSql {
        source: "inline".to_string(),
        sql: entry,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;
    use tempfile::tempdir;

    use super::{SetupSql, load_query_manifest, split_sql_statements};

    #[test]
    fn expands_include_sql_before_entries() -> Result<()> {
        let dir = tempdir()?;
        let include_path = dir.path().join("strict.sql");
        let manifest_path = dir.path().join("q1.yml");
        fs::write(&include_path, "set optd.optd_strict_mode = true;")?;
        fs::write(
            &manifest_path,
            r#"
- sql: |
    SELECT 1;
  desc: test
  before: ["include_sql:strict.sql", "set x = 1;"]
"#,
        )?;

        let query = load_query_manifest(&manifest_path, "tpch")?;
        assert_eq!(query.before.len(), 2);
        assert_eq!(
            query.before[0],
            SetupSql {
                source: include_path.display().to_string(),
                sql: "set optd.optd_strict_mode = true;".to_string(),
            }
        );
        assert_eq!(query.before[1].source, "inline");
        assert_eq!(query.before[1].sql, "set x = 1;");
        Ok(())
    }

    #[test]
    fn harness_setup_drops_schema_include() -> Result<()> {
        let dir = tempdir()?;
        fs::create_dir_all(dir.path().join("data/tpch"))?;
        fs::write(dir.path().join("data/tpch/lineitem.parquet"), [])?;

        let query = super::BenchmarkQuery {
            suite: "tpch".to_string(),
            query_id: "q1".to_string(),
            desc: None,
            sql: "select 1".to_string(),
            before: vec![
                SetupSql {
                    source: dir.path().join("schema.sql").display().to_string(),
                    sql: "create table lineitem(a int);".to_string(),
                },
                SetupSql {
                    source: dir.path().join("strict.sql").display().to_string(),
                    sql: "set optd.optd_strict_mode = true;".to_string(),
                },
            ],
            manifest_path: dir.path().join("q1.yml"),
        };

        let setup = query.harness_setup_sql(dir.path())?;
        assert_eq!(setup.len(), 2);
        assert!(setup[0].source.ends_with("strict.sql"));
        assert_eq!(setup[1].source, "externalized:lineitem");
        assert!(setup[1].sql.contains("CREATE EXTERNAL TABLE lineitem"));
        Ok(())
    }

    #[test]
    fn splits_multi_statement_sql() {
        let statements = split_sql_statements("set x = 1; create table t(a int);");
        assert_eq!(statements, vec!["set x = 1", "create table t(a int)"]);
    }
}
