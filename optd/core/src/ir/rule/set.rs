//! Sets are used in Cascades to represent batches of transformations that can
//! be run

use std::sync::Arc;
use crate::ir::rule::Rule;

/// RuleSets are sorted, which allows them to represent batches of
/// transformations / implementation rules that can be run in order
#[derive(Default)]
pub struct RuleSet {
    rules: Arc<[Arc<dyn Rule>]>,
}

/// A RuleSetBuilder implements the builder pattern to create RuleSets easily.
/// RuleSets can be built as RuleSet::builder().add_rule(rule)...build()
#[derive(Default)]
pub struct RuleSetBuilder {
    rules: Vec<Arc<dyn super::Rule>>,
}

impl RuleSet {
    pub fn builder() -> RuleSetBuilder {
        RuleSetBuilder::default()
    }

    pub fn with_rules(rules: Vec<Arc<dyn Rule>>) -> Self {
        RuleSet {
            rules: rules.into(),
        }
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Arc<dyn super::Rule>> {
        self.rules.iter()
    }
}

impl RuleSetBuilder {
    pub fn add_rule(mut self, rule: impl super::Rule) -> Self {
        self.rules.push(Arc::new(rule));
        self
    }

    pub fn build(self) -> RuleSet {
        RuleSet {
            rules: self.rules.into(),
        }
    }
}
