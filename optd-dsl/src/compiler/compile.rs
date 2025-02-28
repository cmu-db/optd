use chumsky::prelude::end;
use chumsky::{Parser, Stream};

use crate::lexer::lex::lex;
use crate::parser::module::module_parser;
use crate::utils::span::Span;
use crate::{analyzer::hir::HIR, utils::error::Error};

/// Compilation options for the DSL
pub struct CompileOptions {
    /// Path to the main module source file
    pub source_path: String,
}

/// Result of the compilation process
pub struct CompileResult {
    /// The resulting HIR if compilation was successful
    pub hir: Option<HIR>,
    /// Any errors encountered during compilation
    pub errors: Vec<Error>,
}

/// Compile DSL source code to HIR
pub fn compile(source: &str, options: CompileOptions) -> Result<HIR, Vec<Error>> {
    let mut errors = Vec::new();

    // Step 1: Lexing
    let (tokens_opt, lex_errors) = lex(source, &options.source_path);
    errors.extend(lex_errors);

    if tokens_opt.is_none() {
        // Early return if lexing failed completely
        return Err(errors);
    }
    let tokens = tokens_opt.unwrap();

    // Step 2: Parsing
    let len = source.chars().count();
    let eoi = Span::new(options.source_path.clone(), len..len);

    let (ast_opt, parse_errors) = module_parser()
        .then_ignore(end())
        .parse_recovery(Stream::from_iter(eoi, tokens.into_iter()));

    println!("{:?}", ast_opt);
    println!("{:?}", parse_errors);
    println!("{:?}", errors);

    todo!()
}
