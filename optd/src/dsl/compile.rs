use super::analyzer::{from_ast::from_ast, into_hir::into_hir};
use crate::dsl::{
    analyzer::{
        errors::AnalyzerError,
        hir::{HIR, TypedSpan, Udf},
        semantic_checks::adt_check,
        type_checks::registry::TypeRegistry,
    },
    lexer::lex::lex,
    parser::{ast::Module, module::parse_module},
    utils::errors::CompileError,
};
use clap::{Args, Parser};
use colored::Colorize;
use std::{borrow::Cow, collections::HashMap};
use std::{fs, path::PathBuf};

/// Compilation configuration and options.
#[derive(Parser)]
#[command(
    name = "optd",
    about = "Optimizer DSL compiler and toolchain",
    version,
    author
)]
pub struct Config {
    /// Input file to compile.
    path: PathBuf,
    /// The verbosity settings.
    #[command(flatten)]
    verbosity: Verbosity,
    /// Mock UDFs (user-defined functions) to load.
    #[arg(long, value_delimiter = ' ', num_args = 1..)]
    mock_udfs: Vec<String>,
}

impl Config {
    /// Creates a new Config instance with the given path.
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            verbosity: Default::default(),
            mock_udfs: Default::default(),
        }
    }

    /// A helper method to get the verbosity.
    pub fn verbosity(&self) -> bool {
        self.verbosity.verbose
    }

    /// A helper method to get the path as a string.
    pub fn path_str(&self) -> Cow<'_, str> {
        self.path.to_string_lossy()
    }

    pub fn mock_udfs(&self) -> &[String] {
        self.mock_udfs.as_ref()
    }
}

/// Verbosity settings for compilation.
#[derive(Args, Default)]
pub struct Verbosity {
    /// Print detailed processing information.
    #[arg(long)]
    verbose: bool,
    /// Print the AST in a readable format (must enable --verbose).
    #[arg(long)]
    show_ast: bool,
    /// Print the typed-span HIR in a readable format (must enable --verbose).
    #[arg(long)]
    show_typedspan_hir: bool,
    /// Print the final HIR in a readable format (must enable --verbose).
    #[arg(long)]
    show_hir: bool,
}

/// Compiles a file into the [`HIR`].
#[tracing::instrument(level = "info", skip(config, udfs), fields(
    source_path = %config.path_str(),
    udf_count = udfs.len()
), target = "optd::dsl::compile")]
pub fn compile_hir(config: Config, udfs: HashMap<String, Udf>) -> Result<HIR, Vec<CompileError>> {
    let source_path = config.path_str();
    tracing::info!(target: "optd::dsl::compile", "Starting DSL compilation");

    // If we cannot find the file we can't compile anything, so exit immediately.
    let source = fs::read_to_string(&config.path).unwrap_or_else(|e| {
        tracing::error!(target: "optd::dsl::compile",
            error = %e,
            "Failed to read source file"
        );
        if e.kind() == std::io::ErrorKind::NotFound {
            eprintln!(
                "{} {}",
                "✘".red().bold(),
                format!("File not found: {}", source_path).red()
            );
            eprintln!("  Please check the path and file permissions.\n");
        } else {
            eprintln!(
                "{} {}",
                "✘".red().bold(),
                format!("Error reading file: {}", e).red()
            );
        }
        std::process::exit(1);
    });

    tracing::debug!(target: "optd::dsl::compile",
        source_length = source.len(),
        "Source file loaded successfully"
    );

    // Step 1: Parse.
    if config.verbosity() {
        println!("{} Compiling file: {}", "⏳".blue(), config.path_str());
        println!("{} Parsing source code...", "→".cyan());
    }

    tracing::debug!(target: "optd::dsl::compile", "Starting parsing phase");
    let ast = parse(&source, &config)?;
    tracing::info!(target: "optd::dsl::compile", "Parsing completed successfully");

    if config.verbosity() {
        println!("{}", "Parse successful".green());

        if config.verbosity.show_ast {
            println!("\nAST Structure:\n{:#?}", ast);
        }
    }

    // Step 2: AST to HIR<TypedSpan>.
    if config.verbosity() {
        println!("{} Converting AST to HIR and TypeRegistry...", "→".cyan());
    }

    tracing::debug!(target: "optd::dsl::compile", "Starting AST to HIR conversion");
    let (typed_hir, mut type_registry) = ast_to_hir(&source, ast, udfs).map_err(|e| {
        tracing::error!(target: "optd::dsl::compile",
            error = ?e,
            "AST to HIR conversion failed"
        );
        vec![e]
    })?;
    tracing::info!(target: "optd::dsl::compile", "AST to HIR conversion completed successfully");

    if config.verbosity() {
        println!("{}", "AST to HIR conversion successful".green());

        if config.verbosity.show_typedspan_hir {
            println!("\nTyped-Span HIR Structure:\n{:#?}", typed_hir);
        }
    }

    // Step 3: Semantic checks.
    if config.verbosity() {
        println!("{} Checking TypeRegistry...", "→".cyan());
    }

    tracing::debug!(target: "optd::dsl::compile", "Starting semantic checks");
    registry_check(&source, &source_path, &type_registry).map_err(|e| {
        tracing::error!(target: "optd::dsl::compile",
            error = ?e,
            "Semantic checks failed"
        );
        vec![e]
    })?;
    tracing::info!(target: "optd::dsl::compile", "Semantic checks completed successfully");

    if config.verbosity() {
        println!("{}", "TypeRegistry check successful".green());
    }

    // Step 4: Type checks & inference.
    if config.verbosity() {
        println!("{} Performing type inference...", "→".cyan());
    }

    tracing::debug!(target: "optd::dsl::compile", "Starting type inference");
    let hir = infer(&source, typed_hir, &mut type_registry).map_err(|e| {
        tracing::error!(target: "optd::dsl::compile",
            error = ?e,
            "Type inference failed"
        );
        vec![e]
    })?;
    tracing::info!(target: "optd::dsl::compile", "Type inference completed successfully");

    if config.verbosity() {
        println!("{}", "Type inference successful".green());

        if config.verbosity.show_hir {
            println!("\nHIR Structure:\n{:#?}", hir);
        }

        println!("\n{}", "Compilation completed successfully!".green().bold());
    }

    tracing::info!(target: "optd::dsl::compile", "DSL compilation completed successfully");
    Ok(hir)
}

/// Parse DSL source code to AST.
///
/// This function performs lexing and parsing stages of compilation,
/// returning either the parsed AST Module or collected errors.
#[tracing::instrument(level = "info", skip(source, config), fields(
    source_length = source.len(),
    source_path = %config.path_str()
), target = "optd::dsl::compile")]
pub fn parse(source: &str, config: &Config) -> Result<Module, Vec<CompileError>> {
    let mut errors = Vec::new();

    // Step 1: Lexing
    tracing::debug!(target: "optd::dsl::compile", "Starting lexical analysis");
    let (tokens_opt, lex_errors) = lex(source, &config.path_str());
    let lex_error_count = lex_errors.len();
    errors.extend(lex_errors);

    if lex_error_count > 0 {
        tracing::warn!(target: "optd::dsl::compile",
            error_count = lex_error_count,
            "Lexical analysis completed with errors"
        );
    } else {
        tracing::debug!(target: "optd::dsl::compile", "Lexical analysis completed successfully");
    }

    match tokens_opt {
        Some(tokens) => {
            tracing::debug!(target: "optd::dsl::compile",
                token_count = tokens.len(),
                "Starting syntax analysis"
            );
            // Step 2: Parsing
            let (ast_opt, parse_errors) = parse_module(tokens, source, &config.path_str());
            let parse_error_count = parse_errors.len();
            errors.extend(parse_errors);

            if parse_error_count > 0 {
                tracing::warn!(target: "optd::dsl::compile",
                    error_count = parse_error_count,
                    "Syntax analysis completed with errors"
                );
            } else {
                tracing::debug!(target: "optd::dsl::compile", "Syntax analysis completed successfully");
            }

            match ast_opt {
                Some(ast) if errors.is_empty() => {
                    tracing::debug!(target: "optd::dsl::compile", "Parse phase completed successfully");
                    Ok(ast)
                }
                _ => {
                    tracing::error!(target: "optd::dsl::compile",
                        total_errors = errors.len(),
                        "Parse phase failed with errors"
                    );
                    Err(errors)
                }
            }
        }
        None => {
            tracing::error!(target: "optd::dsl::compile",
                error_count = errors.len(),
                "Lexical analysis failed, no tokens produced"
            );
            Err(errors)
        }
    }
}

/// Convert AST to typed HIR.
///
/// This function performs semantic analysis on the AST and converts it
/// to a typed High-level Intermediate Representation (HIR).
#[tracing::instrument(level = "info", skip(source, ast, udfs), fields(
    udf_count = udfs.len()
), target = "optd::dsl::compile")]
pub fn ast_to_hir(
    source: &str,
    ast: Module,
    udfs: HashMap<String, Udf>,
) -> Result<(HIR<TypedSpan>, TypeRegistry), CompileError> {
    tracing::debug!(target: "optd::dsl::compile", "Converting AST to typed HIR");

    from_ast(&ast, udfs).map_err(|err_kind| {
        tracing::error!(target: "optd::dsl::compile",
            error = ?err_kind,
            "AST to HIR conversion failed"
        );
        CompileError::AnalyzerError(AnalyzerError::new(source.to_string(), *err_kind))
    })
}

/// Checks the type registry.
///
/// This function verifies that all ADT definitions are valid:
/// - Checks for circular ADT definitions that would cause infinite recursion
/// - Checks for duplicate field names within product types
/// - Verifies that all referenced types exist in the registry
pub fn registry_check(
    source: &str,
    source_path: &str,
    registry: &TypeRegistry,
) -> Result<(), CompileError> {
    adt_check(registry, source_path).map_err(|err_kind| {
        CompileError::AnalyzerError(AnalyzerError::new(source.to_string(), *err_kind))
    })
}

/// Performs type inference on the typed HIR.
///
/// This function is responsible for:
/// 1. Performing scope checking to verify all identifiers are properly defined.
/// 2. Building type constraints based on the annotated TypedSpan nodes.
/// 3. Resolving these constraints to infer concrete types.
/// 4. Transforming the typed HIR into its final form.
#[tracing::instrument(level = "info", skip(source, hir, registry), target = "optd::dsl::compile")]
pub fn infer(
    source: &str,
    hir: HIR<TypedSpan>,
    registry: &mut TypeRegistry,
) -> Result<HIR, CompileError> {
    tracing::debug!(target: "optd::dsl::compile", "Starting type inference");

    // Step 1 & 2: Perform scope checking and generate type constraints
    // This traverses the HIR, verifies scopes, and creates constraints for all expressions
    tracing::trace!(target: "optd::dsl::compile", "Generating type constraints");
    registry.generate_constraints(&hir).map_err(|err_kind| {
        tracing::error!(target: "optd::dsl::compile",
            error = ?err_kind,
            "Type constraint generation failed"
        );
        CompileError::AnalyzerError(AnalyzerError::new(source.to_string(), *err_kind))
    })?;

    // Step 3: Resolve constraints.
    tracing::trace!(target: "optd::dsl::compile", "Resolving type constraints");
    registry.resolve().map_err(|err_kind| {
        tracing::error!(target: "optd::dsl::compile",
            error = ?err_kind,
            "Type constraint resolution failed"
        );
        CompileError::AnalyzerError(AnalyzerError::new(source.to_string(), *err_kind))
    })?;

    // Step 4: Transform HIR
    // After type inference, transform the HIR into its final form with complete type information.
    tracing::trace!(target: "optd::dsl::compile", "Transforming HIR to final form");
    into_hir(hir, registry).map_err(|err_kind| {
        tracing::error!(target: "optd::dsl::compile",
            error = ?err_kind,
            "HIR transformation failed"
        );
        CompileError::AnalyzerError(AnalyzerError::new(source.to_string(), *err_kind))
    })
}
