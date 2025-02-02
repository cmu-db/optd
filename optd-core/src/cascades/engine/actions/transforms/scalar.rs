use std::sync::Arc;

use crate::cascades::engine::patterns::scalar::ScalarPattern;

pub type ScalarComposition = (String, Arc<()>); // TODO(alexis): scalar transform + scalar analyze here

#[derive(Clone)]
pub struct ScalarTransform {
    pub name: String,
    pub matches: Vec<ScalarMatch>,
}

#[derive(Clone)]
pub struct ScalarMatch {
    pub pattern: ScalarPattern,
    pub composition: Vec<ScalarComposition>,
    pub output: (), // TODO(alexis): partial scalar plan expr here...
}
