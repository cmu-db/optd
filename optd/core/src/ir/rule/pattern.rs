//! Patterns are a core component used to match transformation / implementation
//! rules against a specific operator pattern in the IR. Since most rules only
//! apply to specific operator types or structures, patterns allow rules to
//! specify which operators they can be applied to.

use crate::ir::{Operator, OperatorKind};

type MatchFunc<K> = Box<dyn Fn(&K) -> bool + 'static + Send + Sync>;
pub type OperatorMatchFunc = MatchFunc<OperatorKind>;

/// An OperatorPattern describes a pattern to match against an Operator in the 
/// IR. It contains some function to match against the top-level operator kind,
/// as well as a list of input operator patterns to match against the operator's
/// inputs. The input operator patterns are tuples of the form (child index, 
/// pattern).
pub struct OperatorPattern {
    matches: OperatorMatchFunc,
    input_operator_patterns: Vec<(usize, OperatorPattern)>,
}

impl OperatorPattern {
    /// A simple constructor to match against top-level operators only
    pub fn with_top_matches<F>(f: F) -> Self
    where
        F: Fn(&OperatorKind) -> bool + 'static + Send + Sync,
    {
        OperatorPattern {
            matches: Box::new(f),
            input_operator_patterns: Vec::new(),
        }
    }

    /// Allows adding an input operator pattern to match against a specific
    /// child index of the operator.
    pub fn add_input_operator_pattern(
        &mut self,
        index: usize,
        input_pattern: OperatorPattern,
    ) -> &mut Self {
        self.input_operator_patterns.push((index, input_pattern));
        self
    }

    /// Returns the input matching patterns for this operator pattern.
    pub fn input_operator_patterns(&self) -> &[(usize, OperatorPattern)] {
        &self.input_operator_patterns
    }

    /// Returns whether the top-level operator kind matches this pattern.
    pub fn top_matches(&self, kind: &OperatorKind) -> bool {
        (self.matches)(kind)
    }

    pub fn matches_without_expand(&self, operator: &Operator) -> bool {
        let input_ops = operator.input_operators();
        self.top_matches(&operator.kind) && self.input_operator_patterns.iter().all(
            |(i, input_pattern)| {
                input_ops
                    .get(*i)
                    .map_or(false, |input_op| 
                        input_pattern.matches_without_expand(input_op))
        })

    }
}
