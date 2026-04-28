use anyhow::{Result, bail};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use optd_datafusion::DataFusionDB;

pub struct PlannerTestDB(pub DataFusionDB);

pub enum PlannerTestTask {
    /// Execute the query,
    Execute,
    /// Displays the query plan.
    Explain,
}

fn parse_task(task: &str) -> Result<PlannerTestTask> {
    match task {
        "execute" => Ok(PlannerTestTask::Execute),
        "explain" => Ok(PlannerTestTask::Explain),
        _ => bail!("Unknown task: {}", task),
    }
}

impl PlannerTestDB {
    pub async fn execute(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        let batches = self.0.execute(sql).await?;
        let options = FormatOptions::default().with_null("NULL");
        let mut result = Vec::with_capacity(batches.len());
        for batch in batches {
            let converters = batch
                .columns()
                .iter()
                .map(|a| ArrayFormatter::try_new(a.as_ref(), &options))
                .collect::<Result<Vec<_>, _>>()?;
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::with_capacity(batch.num_columns());
                for converter in converters.iter() {
                    let mut buffer = String::with_capacity(8);
                    converter.value(row_idx).write(&mut buffer)?;
                    row.push(buffer);
                }
                result.push(row);
            }
        }
        Ok(result)
    }
}

#[async_trait::async_trait]
impl sqlplannertest::PlannerTestRunner for PlannerTestDB {
    async fn run(&mut self, test_case: &sqlplannertest::ParsedTestCase) -> Result<String> {
        use itertools::Itertools;
        use std::fmt::Write;

        let mut result = String::new();
        let r = &mut result;
        for sql in &test_case.before_sql {
            // We drop output of before statements
            self.0.execute(sql).await?;
        }

        for task_str in &test_case.tasks {
            let task = parse_task(task_str)?;
            match task {
                PlannerTestTask::Execute => {
                    let result = self.execute(&test_case.sql).await?;
                    writeln!(r, "{}", result.into_iter().map(|x| x.join(" ")).join("\n"))?;
                    writeln!(r)?;
                }
                PlannerTestTask::Explain => {
                    // Handle the Explain task here
                    let explained_sql = format!("EXPLAIN verbose {}", test_case.sql);
                    let result = self.execute(&explained_sql).await?;
                    let explained_output = result
                        .into_iter()
                        .filter_map(|row| {
                            let label = row[0].as_str();
                            (label.starts_with("logical_plan after optd-")
                                || label.starts_with("physical_plan after optd-"))
                                .then(|| row[0..2].join(":\n"))
                        })
                        .join("\n\n");
                    writeln!(r, "{}\n", explained_output)?;
                }
            }
        }

        Ok(result)
    }
}
