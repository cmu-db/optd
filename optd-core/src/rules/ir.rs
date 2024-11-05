use crate::nodes::NodeType;

// TODO: Documentation
pub enum RuleMatcher<T: NodeType> {
    /// Match a node of type `typ`.
    MatchAndPickNode {
        typ: T,
        children: Vec<Self>,
        predicates: Vec<Self>,
        pick_to: usize,
    },
    /// Match a node of type `typ`.
    MatchNode {
        typ: T,
        children: Vec<Self>,
        predicates: Vec<Self>,
    },
    /// Match "discriminant" (Only check for variant matches---don't consider
    /// inner data).
    /// This may be useful when, for example, one has an enum variant such as
    /// ConstantExpr(ConstantType), and one wants to match on all ConstantExpr
    /// regardless of the inner ConstantType.
    MatchAndPickDiscriminant {
        typ_discriminant: std::mem::Discriminant<T>,
        children: Vec<Self>,
        predicates: Vec<Self>,
        pick_to: usize,
    },
    /// Match "discriminant" (Only check for variant matches---don't consider
    /// inner data).
    /// This may be useful when, for example, one has an enum variant such as
    /// ConstantExpr(ConstantType), and one wants to match on all ConstantExpr
    /// regardless of the inner ConstantType.
    MatchDiscriminant {
        typ_discriminant: std::mem::Discriminant<T>,
        children: Vec<Self>,
        predicates: Vec<Self>,
    },
    /// Match predicate (TODO consider more elegant design)
    PickPred { pick_to: usize },
    /// Match any plan node,
    PickOne { pick_to: usize },
    /// Match all things in the group
    PickMany { pick_to: usize },
    /// Ignore one
    IgnoreOne,
    /// Ignore many
    IgnoreMany,
}
