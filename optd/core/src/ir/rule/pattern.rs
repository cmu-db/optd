use crate::ir::{Operator, OperatorKind};

type MatchFunc<K> = Box<dyn Fn(&K) -> bool + 'static + Send + Sync>;

pub type OperatorMatchFunc = MatchFunc<OperatorKind>;

pub struct OperatorPattern {
    matches: OperatorMatchFunc,
    input_operator_patterns: Vec<(usize, OperatorPattern)>,
}

impl OperatorPattern {
    pub fn with_top_matches<F>(f: F) -> Self
    where
        F: Fn(&OperatorKind) -> bool + 'static + Send + Sync,
    {
        OperatorPattern {
            matches: Box::new(f),
            input_operator_patterns: Vec::new(),
        }
    }

    pub fn add_input_operator_pattern(
        &mut self,
        index: usize,
        input_pattern: OperatorPattern,
    ) -> &mut Self {
        self.input_operator_patterns.push((index, input_pattern));
        self
    }

    pub fn input_operator_patterns(&self) -> &[(usize, OperatorPattern)] {
        &self.input_operator_patterns
    }

    pub fn top_matches(&self, operator: &Operator) -> bool {
        (self.matches)(&operator.kind)
    }

    pub fn matches_without_expand(&self, operator: &Operator) -> bool {
        let input_ops = operator.input_operators();
        self.input_operator_patterns.iter().fold(
            self.top_matches(operator),
            |prev_matches, (i, input_pattern)| {
                let Some(input_op) = &input_ops.get(*i) else {
                    return false;
                };

                prev_matches && input_pattern.matches_without_expand(input_op)
            },
        )
    }
}
