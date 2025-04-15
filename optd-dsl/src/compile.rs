use crate::{
    analyzer::{
        error::AnalyzerError,
        from_ast::ASTConverter,
        hir::{HIR, TypedSpan},
        semantic_check::{
            adt_check,
            scope_check::{self},
        },
        types::TypeRegistry,
    },
    lexer::lex::lex,
    parser::{ast::Module, module::parse_module},
    utils::error::CompileError,
};

/// Compilation options for the DSL
pub struct CompileOptions {
    /// Path to the main module source file
    pub source_path: String,
}

/// Parse DSL source code to AST
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

/// Convert AST to typed HIR
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

/// Perform scope checking on HIR
///
/// This function verifies that all variable references in the HIR are valid
/// and all bindings follow proper scoping rules.
pub fn scope_check(source: &str, hir: &HIR<TypedSpan>) -> Result<(), CompileError> {
    scope_check::scope_check(hir).map_err(|err_kind| {
        CompileError::AnalyzerError(AnalyzerError::new(source.to_string(), *err_kind))
    })
}

/// Perform ADT checking on type registry
///
/// This function verifies that all ADT definitions are valid:
/// - Checks for circular ADT definitions that would cause infinite recursion
/// - Checks for duplicate field names within product types
/// - Verifies that all referenced types exist in the registry
pub fn adt_check(
    source: &str,
    source_path: &str,
    registry: &TypeRegistry,
) -> Result<(), CompileError> {
    adt_check::adt_check(registry, source_path).map_err(|err_kind| {
        CompileError::AnalyzerError(AnalyzerError::new(source.to_string(), *err_kind))
    })
}
