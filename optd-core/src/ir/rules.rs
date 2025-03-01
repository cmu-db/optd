/// A unique identifier for a transformation rule in the system.
#[repr(transparent)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    sqlx::Type,
    serde::Serialize,
    serde::Deserialize,
)]
#[sqlx(transparent)]
pub struct TransformationRuleId(pub i64);

/// A unique identifier for an implementation rule in the system.
#[repr(transparent)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    sqlx::Type,
    serde::Serialize,
    serde::Deserialize,
)]
#[sqlx(transparent)]
pub struct ImplementationRuleId(pub i64);

pub enum RuleId {
    TransformationRule(TransformationRuleId),
    ImplementationRule(ImplementationRuleId),
}
