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
}

impl Config {
    /// Creates a new Config instance with the given path.
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            verbosity: Default::default(),
        }
    }

    /// A helper method to get the verbosity.
    fn verbose(&self) -> bool {
        self.verbosity.verbose
    }

    /// A helper method to get the path as a string.
    fn path_str(&self) -> Cow<'_, str> {
        self.path.to_string_lossy()
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
pub fn compile_hir(config: Config, udfs: HashMap<String, Udf>) -> Result<HIR, Vec<CompileError>> {
    let source_path = config.path_str();

    // If we cannot find the file we can't compile anything, so exit immediately.
    let source = fs::read_to_string(&config.path).unwrap_or_else(|e| {
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

    // Step 1: Parse.
    if config.verbose() {
        println!("{} Compiling file: {}", "⏳".blue(), config.path_str());
        println!("{} Parsing source code...", "→".cyan());
    }

    let ast = parse(&source, &config)?;

    if config.verbose() {
        println!("{}", "Parse successful".green());

        if config.verbosity.show_ast {
            println!("\nAST Structure:\n{:#?}", ast);
        }
    }

    // Step 2: AST to HIR<TypedSpan>.
    if config.verbose() {
        println!("{} Converting AST to HIR and TypeRegistry...", "→".cyan());
    }

    let (typed_hir, mut type_registry) = ast_to_hir(&source, ast, udfs).map_err(|e| vec![e])?;

    if config.verbose() {
        println!("{}", "AST to HIR conversion successful".green());

        if config.verbosity.show_typedspan_hir {
            println!("\nTyped-Span HIR Structure:\n{:#?}", typed_hir);
        }
    }

    // Step 3: Semantic checks.
    if config.verbose() {
        println!("{} Checking TypeRegistry...", "→".cyan());
    }

    registry_check(&source, &source_path, &type_registry).map_err(|e| vec![e])?;

    if config.verbose() {
        println!("{}", "TypeRegistry check successful".green());
    }

    // Step 4: Type checks & inference.
    if config.verbose() {
        println!("{} Performing type inference...", "→".cyan());
    }

    let hir = infer(&source, typed_hir, &mut type_registry).map_err(|e| vec![e])?;

    if config.verbose() {
        println!("{}", "Type inference successful".green());

        if config.verbosity.show_hir {
            println!("\nHIR Structure:\n{:#?}", hir);
        }

        println!("\n{}", "Compilation completed successfully!".green().bold());
    }

    Ok(hir)
}

/// Parse DSL source code to AST.
///
/// This function performs lexing and parsing stages of compilation,
/// returning either the parsed AST Module or collected errors.
pub fn parse(source: &str, config: &Config) -> Result<Module, Vec<CompileError>> {
    let mut errors = Vec::new();
    // Step 1: Lexing
    let (tokens_opt, lex_errors) = lex(source, &config.path_str());
    errors.extend(lex_errors);
    match tokens_opt {
        Some(tokens) => {
            // Step 2: Parsing
            let (ast_opt, parse_errors) = parse_module(tokens, source, &config.path_str());
            errors.extend(parse_errors);
            match ast_opt {
                Some(ast) if errors.is_empty() => Ok(ast),
                _ => Err(errors),
            }
        }
        None => Err(errors),
    }
}

/// Convert AST to typed HIR.
///
/// This function performs semantic analysis on the AST and converts it
/// to a typed High-level Intermediate Representation (HIR).
pub fn ast_to_hir(
    source: &str,
    ast: Module,
    udfs: HashMap<String, Udf>,
) -> Result<(HIR<TypedSpan>, TypeRegistry), CompileError> {
    from_ast(&ast, udfs).map_err(|err_kind| {
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
pub fn infer(
    source: &str,
    hir: HIR<TypedSpan>,
    registry: &mut TypeRegistry,
) -> Result<HIR, CompileError> {
    // Step 1 & 2: Perform scope checking and generate type constraints
    // This traverses the HIR, verifies scopes, and creates constraints for all expressions
    registry.generate_constraints(&hir).map_err(|err_kind| {
        CompileError::AnalyzerError(AnalyzerError::new(source.to_string(), *err_kind))
    })?;

    // Step 3: Resolve constraints.
    registry.resolve().map_err(|err_kind| {
        CompileError::AnalyzerError(AnalyzerError::new(source.to_string(), *err_kind))
    })?;

    // Step 4: Transform HIR
    // After type inference, transform the HIR into its final form with complete type information.
    Ok(into_hir(hir, registry))
}
