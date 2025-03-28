use crate::analyzer::hir::HIR;
use crate::lexer::lex::lex;
use crate::parser::ast::Module;
use crate::parser::module::parse_module;
use crate::utils::error::CompileError;

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

/// Compile DSL source code to HIR
///
/// This function performs the full compilation pipeline including lexing,
/// parsing, and semantic analysis to produce HIR.
///
/// # Arguments
/// * `source` - The source code to compile
/// * `options` - Compilation options including source path
///
/// # Returns
/// * `Result<HIR, Vec<CompileError>>` - The compiled HIR or errors
pub fn compile(source: &str, options: &CompileOptions) -> Result<HIR, Vec<CompileError>> {
    // Step 1 & 2: Parse to AST
    let _ast = parse(source, options)?;

    // Step 3: Semantic analysis to HIR
    todo!("Implement semantic analysis to convert AST to HIR")
}
