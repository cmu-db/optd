use std::{error::Error, fs, path::PathBuf};

use clap::Parser;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use optd::FreeColumns;
use optd::QueryFormatConfig;
use optd_datafusion::runner::{RunnerOutput, OptdRunner};
use optd_datafusion::setup::{session_context_with_information_schema, setup_tpch_session};

#[derive(Debug, Parser)]
#[command(
    name = "optd",
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

    /// Print box-formatted optd optimizer steps before executing each statement.
    #[arg(long)]
    log_explain_steps: bool,
}

#[derive(Clone, Copy)]
struct ExecuteOptions {
    log_explain_steps: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let session = if args.tpch {
        setup_tpch_session().await?
    } else {
        session_context_with_information_schema()
    };
    let runner = OptdRunner::new(session);
    let options = ExecuteOptions {
        log_explain_steps: args.log_explain_steps,
    };

    if args.commands.is_empty() && args.files.is_empty() {
        repl(&runner, options).await?;
        return Ok(());
    }

    for sql in args.commands {
        execute_script(&runner, &sql, options).await?;
    }

    for path in args.files {
        let sql = fs::read_to_string(&path)?;
        execute_script(&runner, &sql, options).await?;
    }

    Ok(())
}

async fn repl(runner: &OptdRunner, options: ExecuteOptions) -> Result<(), Box<dyn Error>> {
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
                    if let Err(err) = execute_statement(runner, &statement, options).await {
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

async fn execute_script(
    runner: &OptdRunner,
    sql: &str,
    options: ExecuteOptions,
) -> Result<(), Box<dyn Error>> {
    for statement in split_statements(sql) {
        execute_statement(runner, &statement, options).await?;
    }
    Ok(())
}

async fn execute_statement(
    runner: &OptdRunner,
    statement: &str,
    options: ExecuteOptions,
) -> Result<(), Box<dyn Error>> {
    if options.log_explain_steps {
        log_explain_steps(runner, statement)?;
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
        QueryFormatConfig::new().with_analysis::<FreeColumns>(),
    )?;
    for step in steps {
        println!(
            "-- explain_steps step={} pass={} result={}",
            step.step, step.pass, step.result
        );
        if let Some(iteration) = step.iteration {
            println!("-- iteration={iteration}");
        }
        if let Some(pass_index) = step.pass_index {
            println!("-- pass_index={pass_index}");
        }
        if let Some(duration_ms) = step.duration_ms {
            println!("-- duration_ms={duration_ms:.3}");
        }
        println!("{}", step.plan);
    }
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
