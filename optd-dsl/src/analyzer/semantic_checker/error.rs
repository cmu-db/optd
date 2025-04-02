use ariadne::{Report, Source};

use crate::utils::{error::Diagnose, span::Span};

#[derive(Debug)]
pub struct SemanticError {}

impl Diagnose for SemanticError {
    fn report(&self) -> Report<Span> {
        todo!()
    }

    fn source(&self) -> (String, Source) {
        todo!()
    }
}
