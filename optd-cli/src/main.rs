//! CLI tool for the Optimizer DSL
//!
//! This tool provides a command-line interface for the Optimizer DSL compiler.
//!
//! # Usage
//!
//! ```
//! # Compile a DSL file (parse and analyze):
//! optd-cli compile path/to/file.opt
//!
//! # Compile with verbose output:
//! optd-cli compile path/to/file.opt --verbose
//!
//! # Print intermediate representations:
//! optd-cli compile path/to/file.opt --verbose --show-ast --show-typedspan-hir --show-hir
//!
//! # Get help:
//! optd-cli --help
//! optd-cli compile --help
//!
//! # Run functions marked with [run] annotation:
//! optd-cli run-functions path/to/file.opt
//! ```
//!
//! When developing, you can run through cargo:
//!
//! ```
//! cargo run --bin optd-cli -- compile path/to/example.opt
//! cargo run --bin optd-cli -- compile path/to/example.opt --verbose
//! cargo run --bin optd-cli -- compile path/to/example.opt --verbose --show-ast --show-hir
//! cargo run --bin optd-cli -- compile path/to/example.opt --mock-udfs hello get_schema world
//! cargo run --bin optd-cli -- run-functions path/to/example.opt
//! ```

use clap::{Parser, Subcommand};
use colored::Colorize;
use optd::catalog::iceberg::memory_catalog;
use optd::dsl::analyzer::hir::{CoreData, HIR, Udf, Value};
use optd::dsl::compile::{Config, compile_hir};
use optd::dsl::engine::{Continuation, Engine, EngineResponse};
use optd::dsl::utils::errors::{CompileError, Diagnose};
use optd::dsl::utils::retriever::MockRetriever;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tracing::Instrument;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser)]
#[command(
    name = "optd",
    about = "Optimizer DSL compiler and toolchain",
    version,
    author
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Compile a DSL file (parse and analyze).
    Compile(Config),
    /// Run functions annotated with [run].
    RunFunctions(Config),
}

fn main() -> Result<(), Vec<CompileError>> {
    // Initialize tracing subscriber
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("optd=info,optd_cli=info"));
    fmt()
        .with_env_filter(filter)
        .pretty()
        .with_ansi(true)
        .init();

    tracing::info!(target: "optd_cli", "optd-cli starting up");
    let cli = Cli::parse();

    let mut udfs = HashMap::new();
    let unimplemented_udf = Udf {
        func: Arc::new(|_, _, _| {
            Box::pin(async move {
                println!("This user-defined function is unimplemented!");
                Value::new(CoreData::<Value>::None)
            })
        }),
    };
    udfs.insert("unimplemented_udf".to_string(), unimplemented_udf.clone());

    match cli.command {
        Commands::Compile(config) => {
            let compile_span =
                tracing::info_span!(target: "optd_cli", "compile_file", path = %config.path_str());
            let _guard = compile_span.enter();
            tracing::info!("Starting compilation");
            for mock_udf in config.mock_udfs() {
                udfs.insert(mock_udf.to_string(), unimplemented_udf.clone());
            }

            let _ = compile_hir(config, udfs).unwrap_or_else(|errors| handle_errors(&errors));
            tracing::info!("Compilation completed successfully");
            Ok(())
        }
        Commands::RunFunctions(config) => {
            let run_span = tracing::info_span!(target: "optd_cli", "run_dsl_functions", path = %config.path_str());
            let _guard = run_span.enter();
            tracing::info!("Starting function execution");
            for mock_udf in config.mock_udfs() {
                udfs.insert(mock_udf.to_string(), unimplemented_udf.clone());
            }

            let hir = compile_hir(config, udfs).unwrap_or_else(|errors| handle_errors(&errors));
            tracing::info!("Compilation successful, proceeding to run functions");

            run_all_functions(&hir)
        }
    }
}

/// Result of running a function.
struct FunctionResult {
    name: String,
    result: EngineResponse<Value>,
}

/// Run all functions found in the HIR, marked with [run].
fn run_all_functions(hir: &HIR) -> Result<(), Vec<CompileError>> {
    println!("\n{} {}\n", "•".green(), "Running functions...".green());

    let functions_to_run = find_functions(hir);
    tracing::info!(target: "optd_cli", num_functions = functions_to_run.len(), "Found functions to run");

    if functions_to_run.is_empty() {
        println!("No functions found annotated with [run]");
        tracing::warn!(target: "optd_cli", "No functions found annotated with [run]");
        return Ok(());
    }

    println!("Found {} functions to run", functions_to_run.len());

    // Create a multi-threaded runtime for parallel execution.
    // TODO: We increase the stack size by x64 to avoid stack overflow
    // given the lack of tail recursion in the engine (yet...)
    let runtime = Builder::new_multi_thread()
        .thread_stack_size(128 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    tracing::debug!(target: "optd_cli", "Tokio runtime initialized for function execution");
    let function_results = runtime.block_on(run_functions_in_parallel(hir, functions_to_run));

    // Process and display function results.
    let success_count = process_function_results(function_results);

    println!(
        "\n{} {}",
        "Execution Results:".yellow(),
        format!("{} functions executed", success_count).yellow()
    );
    tracing::info!(target: "optd_cli", success_count, "Function execution finished");

    Ok(())
}

async fn run_functions_in_parallel(hir: &HIR, functions: Vec<String>) -> Vec<FunctionResult> {
    let catalog = Arc::new(memory_catalog());
    let retriever = Arc::new(MockRetriever::new());
    let mut set = JoinSet::new();

    tracing::debug!(
        target: "optd_cli",
        num_functions = functions.len(),
        "Spawning functions for parallel execution"
    );
    for function_name in functions {
        let engine = Engine::new(hir.context.clone(), catalog.clone(), retriever.clone());
        let name = function_name.clone();

        set.spawn(
            async move {
                // Create a continuation that returns itself.
                let result_handler: Continuation<Value, Value> =
                    Arc::new(|value| Box::pin(async move { value }));

                tracing::debug!(target: "optd_cli", function_name = %name, "Launching function");
                // Launch the function with an empty vector of arguments.
                let result = engine.launch(&name, vec![], result_handler).await;
                tracing::debug!(target: "optd_cli", function_name = %name, "Function launch completed");
                FunctionResult { name, result }
            }
            .instrument(tracing::info_span!(target: "optd_cli", "run_dsl_function", function_name = %function_name)),
        );
    }

    // Collect all function results.
    let mut results = Vec::new();
    while let Some(result) = set.join_next().await {
        if let Ok(function_result) = result {
            tracing::debug!(target: "optd_cli", function_name = %function_result.name, "Function task completed");
            results.push(function_result);
        }
    }

    results
}

/// Process function results and display them.
fn process_function_results(function_results: Vec<FunctionResult>) -> usize {
    let mut success_count = 0;

    for function_result in function_results {
        println!("\n{} {}", "Function:".blue(), function_result.name);

        match function_result.result {
            EngineResponse::Return(value, _) => {
                tracing::debug!(target: "optd_cli", function_name = %function_result.name, "Function returned a value");
                // Check if the result is a failure.
                if matches!(value.data, CoreData::Fail(_)) {
                    tracing::warn!(target: "optd_cli", function_name = %function_result.name, "Function failed: {}", value);
                    println!("  {}: Function failed: {}", "Error".red(), value);
                } else {
                    println!("  {}: {}", "Result".green(), value);
                    success_count += 1;
                    tracing::info!(target: "optd_cli", function_name = %function_result.name, "Function succeeded. Result: {}", value);
                }
            }
            _ => unreachable!(), // For now, unless we add a special UDF that builds a group / goal.
        }
    }

    success_count
}

/// Find functions with the [run] annotation.
fn find_functions(hir: &HIR) -> Vec<String> {
    let mut functions = Vec::new();

    for (name, _) in hir.context.get_all_bindings() {
        if let Some(annotations) = hir.annotations.get(name) {
            if annotations.iter().any(|a| a == "run") {
                functions.push(name.clone());
            }
        }
    }

    functions
}

/// Display error details and exit the program.
fn handle_errors(errors: &[CompileError]) -> ! {
    tracing::error!(target: "optd_cli", num_errors = errors.len(), "Compilation failed");
    eprintln!(
        "\n{} {}\n",
        "•".yellow(),
        format!("{} error(s) encountered:", errors.len()).yellow()
    );

    for error in errors {
        error
            .print(std::io::stderr())
            .expect("Failed to print error");
    }
    std::process::exit(1);
}
