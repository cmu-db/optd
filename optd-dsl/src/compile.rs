use crate::{
    analyzer::{
        context::Context,
        errors::AnalyzerError,
        from_ast::ASTConverter,
        hir::{HIR, TypedSpan},
        registry_check::{self},
        type_infer::Solver,
        types::TypeRegistry,
    },
    lexer::lex::lex,
    parser::{ast::Module, module::parse_module},
    utils::errors::CompileError,
};

/// Compilation options for the DSL.
pub struct CompileOptions {
    /// Path to the main module source file.
    pub source_path: String,
}

/// Parse DSL source code to AST.
///
/// This function performs lexing and parsing stages of compilation,
/// returning either the parsed AST Module or collected errors.
pub fn parse(source: &str, options: &CompileOptions) -> Result<Module, Vec<CompileError>> {
    let mut errors = Vec::new();
    // Step 1: Lexing
    let (tokens_opt, lex_errors) = lex(source, &options.source_path);
    errors.extend(lex_errors);
    match tokens_opt {
        Some(tokens) => {
            // Step 2: Parsing
            let (ast_opt, parse_errors) = parse_module(tokens, source, &options.source_path);
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
) -> Result<(HIR<TypedSpan>, TypeRegistry), CompileError> {
    let converter = ASTConverter::default();

    converter.convert(&ast).map_err(|err_kind| {
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
    registry_check::adt_check(registry, source_path).map_err(|err_kind| {
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
    hir: &HIR<TypedSpan>,
    registry: &TypeRegistry,
) -> Result<HIR, CompileError> {
    // Create a new constraint solver initialized with the type registry
    let mut solver = Solver::new(registry);

    // Step 1 & 2: Perform scope checking and generate type constraints
    // This traverses the HIR, verifies scopes, and creates constraints for all expressions
    solver.generate_constraints(hir).map_err(|err_kind| {
        CompileError::AnalyzerError(AnalyzerError::new(source.to_string(), *err_kind))
    })?;

    // TODO(alexis): Step 3: Resolve constraints.
    // This step should use unification or other techniques to solve the constraint system
    // and determine concrete types for all expressions.

    // TODO(alexis): Step 4: Transform HIR
    // After type inference, transform the HIR into its final form with complete type information.

    // Currently returns a simplified HIR with default context and original annotations
    // This is a placeholder until the TODO items are implemented.
    Ok(HIR {
        context: Context::default(),
        annotations: hir.annotations.clone(),
    })
}
