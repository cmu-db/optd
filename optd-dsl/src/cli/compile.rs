use optd_dsl::{
    analyzer::{
        from_ast::ASTConverter,
        hir::{HIR, TypedSpan},
        semantic_checker::error::SemanticError,
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
///
/// # Arguments
/// * `source` - The source code to parse
/// * `options` - Compilation options including source path
///
/// # Returns
/// * `Result<Module, Vec<CompileError>>` - The parsed AST or errors
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
///
/// # Arguments
/// * `source` - The source code of the module to convert
/// * `ast` - The AST module to convert
///
/// # Returns
/// * `Result<(HIR<TypedSpan>, TypeRegistry), CompileError>` - The compiled HIR with type registry or error
pub fn ast_to_hir(
    source: &str,
    ast: Module,
) -> Result<(HIR<TypedSpan>, TypeRegistry), CompileError> {
    let converter = ASTConverter::default();

    converter.convert(&ast).map_err(|err_kind| {
        CompileError::SemanticError(SemanticError::new(source.to_string(), err_kind))
    })
}
