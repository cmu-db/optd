use crate::lexer::tokens::Token;
use crate::utils::error::CompileError;
use crate::utils::span::Span;
use chumsky::{prelude::*, Stream};

use super::adt::adt_parser;
use super::ast::{Item, Module};
use super::error::ParserError;
use super::function::function_parser;

/// Parses a vector of tokens into a module AST.
/// Uses Chumsky for parsing and Ariadne for error reporting.
///
/// # Arguments
/// * `tokens` - The tokens to parse
/// * `source` - The original source code (needed for error reporting)
/// * `file_name` - Name of the source file, used for error reporting
///
/// # Returns
/// * `(Option<Module>, Vec<CompileError>)` - The parsed module (if successful) and any errors
pub fn parse_module(
    tokens: Vec<(Token, Span)>,
    source: &str,
    file_name: &str,
) -> (Option<Module>, Vec<CompileError>) {
    let len = source.chars().count();
    let eoi = Span::new(file_name.into(), len..len);

    let (module, errors) = module_parser()
        .then_ignore(end())
        .parse_recovery(Stream::from_iter(eoi, tokens.into_iter()));

    let errors = errors
        .into_iter()
        .map(|e| ParserError::new(source.into(), e).into())
        .collect();

    (module, errors)
}

/// Creates a parser for modules.
///
/// A module consists of a sequence of items, which can be either ADTs or functions.
pub fn module_parser() -> impl Parser<Token, Module, Error = Simple<Token, Span>> + Clone {
    let adt = adt_parser().map(Item::Adt);
    let func = function_parser().map(Item::Function);
    adt.or(func).repeated().map(|items| Module { items })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex::lex;

    #[test]
    fn test_module_parser() {
        let source = r#"
        data LogicalProps(schema_len: I64)

        data Scalar with
            | ColumnRef(idx: Int64)
            | Literal with
                | IntLiteral(value: Int64)
                | StringLiteral(value: String)
                | BoolLiteral(value: Bool)
                \ NullLiteral
            | Arithmetic with
                | Mult(left: Scalar, right: Scalar)
                | Add(left: Scalar, right: Scalar)
                | Sub(left: Scalar, right: Scalar)
                \ Div(left: Scalar, right: Scalar)
            | Predicate with
                | And(children: [Predicate])
                | Or(children: [Predicate])
                | Not(child: Predicate)
                | Equals(left: Scalar, right: Scalar)
                | NotEquals(left: Scalar, right: Scalar)
                | LessThan(left: Scalar, right: Scalar)
                | LessThanEqual(left: Scalar, right: Scalar)
                | GreaterThan(left: Scalar, right: Scalar)
                | GreaterThanEqual(left: Scalar, right: Scalar)
                | IsNull(expr: Scalar)
                \ IsNotNull(expr: Scalar)
            | Function with
                | Cast(expr: Scalar, target_type: String)
                | Substring(str: Scalar, start: Scalar, length: Scalar)
                \ Concat(args: [Scalar])
            \ AggregateExpr with
                | Sum(expr: Scalar)
                | Count(expr: Scalar)
                | Min(expr: Scalar)
                | Max(expr: Scalar)
                \ Avg(expr: Scalar)
        
        data Logical with
            | Scan(table_name: String)
            | Filter(child: Logical, cond: Predicate)
            | Project(child: Logical, exprs: [Scalar])
            | Join(
                  left: Logical,
                  right: Logical,
                  typ: JoinType,
                  cond: Predicate
              )
            \ Aggregate(
                  child: Logical,
                  group_by: [Scalar],
                  aggregates: [AggregateExpr]
              )
        
        data Physical with
            | Scan(table_name: String)
            | Filter(child: Physical, cond: Predicate)
            | Project(child: Physical, exprs: [Scalar])
            | Join with
                | HashJoin(
                      build_side: Physical,
                      probe_side: Physical,
                      typ: String,
                      cond: Predicate
                  )
                | MergeJoin(
                      left: Physical,
                      right: Physical,
                      typ: String,
                      cond: Predicate
                  )
                \ NestedLoopJoin(
                      outer: Physical,
                      inner: Physical,
                      typ: String,
                      cond: Predicate
                  )
            | Aggregate(
                  child: Physical,
                  group_by: [Scalar],
                  aggregates: [AggregateExpr]
              )
            \ Sort(
                  child: Physical,
                  order_by: [(Scalar, SortOrder)]
              )
        
        data JoinType with
            | Inner
            | Left
            | Right
            | Full
            \ Semi

        [rust]
        fn (expr: Scalar) apply_children(f: Scalar => Scalar) = ()

        fn (pred: Predicate) remap(map: {I64 : I64}) =
            match predicate
              | ColumnRef(idx) => ColumnRef(map(idx))
              \ _ => predicate -> apply_children(child => rewrite_column_refs(child, map))
            
        [rule]
        fn (expr: Logical) join_commute = match expr
            \ Join(left, right, Inner, cond) =>
                let 
                    right_indices = 0..right.schema_len,
                    left_indices = 0..left.schema_len,
                    remapping = left_indices.map(i => (i, i + right_len)) ++ 
                        right_indices.map(i => (left_len + i, i)).to_map,
                in
                    Project(
                        Join(right, left, Inner, cond.remap(remapping)),
                        right_indices.map(i => ColumnRef(i)).to_array
                    )
        "#;

        // First lex the input to get tokens
        let (tokens, lex_errors) = lex(source, "test.txt");
        assert_eq!(lex_errors.len(), 0, "Expected no lexer errors");
        assert!(tokens.is_some(), "Expected tokens from lexing");

        // Then parse the tokens into a module
        let (module, parse_errors) = parse_module(tokens.unwrap(), source, "test.txt");
        assert_eq!(parse_errors.len(), 0, "Expected no parser errors");
        assert!(module.is_some(), "Expected a module to be parsed");

        let module = module.unwrap();
        assert_eq!(module.items.len(), 8, "Expected 8 items in the module");
    }
}
