#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransformationRule(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImplementationRule(pub String);

#[derive(Debug, Default)]
pub struct RuleBook {
    pub transformations: Vec<TransformationRule>,
    pub implementations: Vec<ImplementationRule>,
}

impl RuleBook {
    pub fn get_transformations(&self) -> &[TransformationRule] {
        &self.transformations
    }

    pub fn get_implementations(&self) -> &[ImplementationRule] {
        &self.implementations
    }
}
