use crate::rel_node::RelNodeTyp;

// TODO: Documentation
pub enum RuleMatcher<T: RelNodeTyp> {
    /// Match a node of type `typ`.
    MatchAndPickNode {
        typ: T,
        children: Vec<Self>,
        pick_to: usize,
    },
    /// Match a node of type `typ`.
    MatchNode { typ: T, children: Vec<Self> },
    /// Match "discriminant" (Only check for variant matches---don't consider
    /// inner data).
    /// This may be useful when, for example, one has an enum variant such as
    /// ConstantExpr(ConstantType), and one wants to match on all ConstantExpr
    /// regardless of the inner ConstantType.
    MatchAndPickDiscriminant {
        typ_discriminant: std::mem::Discriminant<T>,
        children: Vec<Self>,
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
    },
    /// Match anything,
    PickOne { pick_to: usize, expand: bool },
    /// Match all things in the group
    PickMany { pick_to: usize },
    /// Ignore one
    IgnoreOne,
    /// Ignore many
    IgnoreMany,
}
