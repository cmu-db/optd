use std::{error::Error, fs, path::PathBuf, sync::Arc};

use clap::Parser;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_core::{CardinalityEstimationV1, FreeColumns, QueryFormatConfig};
use optd_datafusion::config::OptdExtensionConfig;
use optd_datafusion::explain_udfs::ExplainStep;
use optd_datafusion::runner::{OptdRunner, RunnerOutput};
use optd_datafusion::setup::{
    register_job_tables, register_tpch_tables, session_context_with_information_schema,
};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

#[derive(Debug, Parser)]
#[command(
    name = "optd-cli",
    about = "Run SQL through the optd DataFusion bridge"
)]
struct Args {
    /// Execute SQL and exit. May be passed more than once.
    #[arg(short = 'c', long = "command")]
    commands: Vec<String>,

    /// Execute SQL from a file and exit. May be passed more than once.
    #[arg(short = 'f', long = "file")]
    files: Vec<PathBuf>,

    /// Register local TPC-H parquet tables before executing SQL.
    #[arg(long)]
    tpch: bool,

    /// Register local JOB parquet tables before executing SQL.
    #[arg(long)]
    job: bool,

    /// Execute optd-optimized IR through direct DataFusion physical planning.
    #[arg(long)]
    physical: bool,

    /// Print the direct optd -> DataFusion physical plan instead of executing SQL.
    #[arg(long)]
    direct_physical_plan: bool,

    /// Print the DataFusion physical plan after optd -> DataFusion logical conversion.
    #[arg(long)]
    logical_physical_plan: bool,

    /// Override DataFusion target partitions before registering benchmark tables.
    #[arg(long)]
    target_partitions: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let session = if args.physical || args.target_partitions.is_some() {
        let mut optd_config = OptdExtensionConfig::default();
        optd_config.physical_planning = args.physical;
        let mut config = SessionConfig::new()
            .with_information_schema(true)
            .with_option_extension(optd_config);
        if let Some(target_partitions) = args.target_partitions {
            config = config.with_target_partitions(target_partitions);
        }
        SessionContext::new_with_config(config)
    } else {
        session_context_with_information_schema()
    };
    if args.tpch {
        register_tpch_tables(&session).await?;
    }
    if args.job {
        register_job_tables(&session).await?;
    }
    let runner = OptdRunner::new(session);

    if args.commands.is_empty() && args.files.is_empty() {
        repl(&runner).await?;
        return Ok(());
    }

    for sql in args.commands {
        if args.logical_physical_plan {
            print_logical_physical_plan(&runner, &sql).await?;
        } else if args.direct_physical_plan {
            print_direct_physical_plan(&runner, &sql).await?;
        } else {
            execute_script(&runner, &sql).await?;
        }
    }

    for path in args.files {
        let sql = fs::read_to_string(&path)?;
        if args.logical_physical_plan {
            print_logical_physical_plan(&runner, &sql).await?;
        } else if args.direct_physical_plan {
            print_direct_physical_plan(&runner, &sql).await?;
        } else {
            execute_script(&runner, &sql).await?;
        }
    }

    Ok(())
}

async fn repl(runner: &OptdRunner) -> Result<(), Box<dyn Error>> {
    let mut editor = DefaultEditor::new()?;
    let mut buffer = String::new();

    loop {
        let prompt = if buffer.trim().is_empty() {
            "optd> "
        } else {
            "   -> "
        };

        match editor.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if buffer.trim().is_empty()
                    && matches!(
                        trimmed.to_ascii_lowercase().as_str(),
                        "\\q" | "quit" | "exit"
                    )
                {
                    break;
                }

                if !trimmed.is_empty() {
                    let _ = editor.add_history_entry(line.as_str());
                }

                buffer.push_str(&line);
                buffer.push('\n');

                let (statements, trailing) = split_complete_statements(&buffer);
                for statement in statements {
                    if let Err(err) = execute_statement(runner, &statement).await {
                        eprintln!("{err}");
                    }
                }
                buffer = trailing;
            }
            Err(ReadlineError::Interrupted) => {
                buffer.clear();
                eprintln!("^C");
            }
            Err(ReadlineError::Eof) => break,
            Err(err) => return Err(Box::new(err)),
        }
    }

    Ok(())
}

async fn execute_script(runner: &OptdRunner, sql: &str) -> Result<(), Box<dyn Error>> {
    for statement in split_statements(sql) {
        execute_statement(runner, &statement).await?;
    }
    Ok(())
}

async fn print_direct_physical_plan(runner: &OptdRunner, sql: &str) -> Result<(), Box<dyn Error>> {
    for statement in split_statements(sql) {
        println!("{}", runner.explain_direct_physical_plan(&statement).await?);
    }
    Ok(())
}

async fn print_logical_physical_plan(runner: &OptdRunner, sql: &str) -> Result<(), Box<dyn Error>> {
    for statement in split_statements(sql) {
        println!(
            "{}",
            runner.explain_logical_physical_plan(&statement).await?
        );
    }
    Ok(())
}

async fn execute_statement(runner: &OptdRunner, statement: &str) -> Result<(), Box<dyn Error>> {
    if runner.log_explain_steps_enabled()
        && let Err(err) = log_explain_steps(runner, statement)
    {
        eprintln!("{err}");
    }

    match runner.execute_sql(statement).await? {
        RunnerOutput::StatementComplete => println!("OK"),
        RunnerOutput::Rows { schema, batches } => {
            let batches = if batches.is_empty() {
                vec![RecordBatch::new_empty(schema)]
            } else {
                batches
            };
            println!("{}", pretty_format_batches(&batches)?);
        }
    }
    Ok(())
}

fn log_explain_steps(runner: &OptdRunner, statement: &str) -> Result<(), Box<dyn Error>> {
    let steps = runner.explain_steps_box_with_config(
        statement,
        QueryFormatConfig::new()
            .with_analysis::<CardinalityEstimationV1>()
            .with_analysis::<FreeColumns>(),
    )?;
    for step in &steps {
        println!("-- explain_steps step={} pass={}", step.step, step.pass);
        println!("{}", step.plan);
    }
    log_explain_step_metrics(&steps)?;
    Ok(())
}

fn log_explain_step_metrics(steps: &[ExplainStep]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int64, false),
        Field::new("iteration", DataType::Int64, true),
        Field::new("pass_index", DataType::Int64, true),
        Field::new("pass", DataType::Utf8, false),
        Field::new("result", DataType::Utf8, false),
        Field::new("duration_ms", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                steps.iter().map(|step| step.step).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                steps.iter().map(|step| step.iteration).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                steps.iter().map(|step| step.pass_index).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                steps
                    .iter()
                    .map(|step| step.pass.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                steps
                    .iter()
                    .map(|step| step.result.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float64Array::from(
                steps
                    .iter()
                    .map(|step| step.duration_ms)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )?;

    println!("-- explain_steps metrics");
    println!("{}", pretty_format_batches(&[batch])?);
    println!("-- explain_steps end\n");
    Ok(())
}

fn split_statements(sql: &str) -> Vec<String> {
    let (mut statements, trailing) = split_complete_statements(sql);
    let trailing = trailing.trim();
    if !trailing.is_empty() {
        statements.push(trailing.to_string());
    }
    statements
}

fn split_complete_statements(sql: &str) -> (Vec<String>, String) {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        current.push(ch);
        match ch {
            '\'' if in_single_quote && chars.peek() == Some(&'\'') => {
                current.push(chars.next().unwrap());
            }
            '\'' => in_single_quote = !in_single_quote,
            ';' if !in_single_quote => {
                let statement = current.trim().trim_end_matches(';').trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                current.clear();
            }
            _ => {}
        }
    }

    (statements, current)
}

#[cfg(test)]
mod tests {
    use super::{split_complete_statements, split_statements};

    #[test]
    fn splits_semicolon_terminated_statements() {
        assert_eq!(
            split_statements("SELECT 1; SELECT 2;"),
            vec!["SELECT 1", "SELECT 2"]
        );
    }

    #[test]
    fn preserves_semicolons_inside_single_quoted_strings() {
        assert_eq!(
            split_statements("SELECT ';'; SELECT 'x'';y';"),
            vec!["SELECT ';'", "SELECT 'x'';y'"]
        );
    }

    #[test]
    fn returns_incomplete_trailing_statement_for_repl() {
        let (statements, trailing) = split_complete_statements("SELECT 1; SELECT 2");
        assert_eq!(statements, vec!["SELECT 1"]);
        assert_eq!(trailing.trim(), "SELECT 2");
    }
}
