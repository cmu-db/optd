use std::sync::Arc;

use crate::ir::rule::Rule;

#[derive(Default)]
pub struct RuleSet {
    rules: Arc<[Arc<dyn super::Rule>]>,
}

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
