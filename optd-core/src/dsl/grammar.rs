use std::collections::HashMap;

use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "dsl/grammar.pest"]
pub struct DslParser;

#[cfg(test)]
mod tests {
    use std::fs;

    use pest::Parser;

    use super::*;

    #[test]
    fn test_parse_operator() {
        let input = fs::read_to_string("/home/alexis/optd/optd-core/src/dsl/test.optd")
            .expect("Failed to read file");

        let pairs = DslParser::parse(Rule::file, &input)
            .map_err(|e| e.to_string())
            .unwrap();
        print!("{:?}", pairs);

        // assert_eq!(file.operators.len(), 1);
        // Add more assertions...
    }
}
