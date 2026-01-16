//! Represents a SQL LIKE scalar operation.

use std::sync::Arc;
use pretty_xmlish::Pretty;
use crate::ir::{
    IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};

define_node!(
    Like, LikeBorrowed {
        properties: ScalarProperties,
        metadata: LikeMetadata {
            negated: bool,
            escape_char: Option<char>,
            case_insensative: bool,
        },
        inputs: {
            operators: [],
            scalars: [expr, pattern],
        }
    }
);
impl_scalar_conversion!(Like, LikeBorrowed);

/// Metadata:
/// - negated: Whether the LIKE operation is negated (NOT LIKE).
/// - escape_char: Optional escape character for the LIKE pattern.
/// - case_insensative: Whether the LIKE operation is case-insensitive (ILIKE).
/// Scalars:
/// - expr: The expression to be matched.
/// - pattern: The pattern to match against.
impl Like {
    pub fn new(
        expr: Arc<Scalar>,
        pattern: Arc<Scalar>,
        negated: bool,
        case_insensative: bool,
        escape_char: Option<char>,
    ) -> Self {
        Self {
            meta: LikeMetadata {
                negated,
                escape_char,
                case_insensative,
            },
            common: IRCommon::with_input_scalars_only(Arc::new([expr, pattern])),
        }
    }
}

impl Explain for LikeBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let maybe_negation = if *self.negated() { "NOT " } else { "" };
        let like_type = if *self.case_insensative() {
            "ILIKE"
        } else {
            "LIKE"
        };
        let maybe_escape = if let Some(c) = self.escape_char() {
            format!(" ESCAPE {c}")
        } else {
            "".to_string()
        };
        let fmt = format!(
            "{} {maybe_negation}{like_type} {}{maybe_escape}",
            self.expr().explain(ctx, option).to_one_line_string(true),
            self.pattern().explain(ctx, option).to_one_line_string(true),
        );
        Pretty::display(&fmt)
    }
}
