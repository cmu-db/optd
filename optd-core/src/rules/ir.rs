use crate::rel_node::RelNodeTyp;

pub enum RuleMatcher<T: RelNodeTyp> {
    /// Match a node of type `typ`.
    MatchAndPickNode {
        typ: T,
        children: Vec<Self>,
        pick_to: usize,
    },
    /// Match a node of type `typ`.
    MatchNode { typ: T, children: Vec<Self> },
    /// Match anything,
    PickOne { pick_to: usize },
    /// Match all things in the group
    PickMany { pick_to: usize },
    /// Ignore one
    IgnoreOne,
    /// Ignore many
    IgnoreMany,
}
