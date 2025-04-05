use ariadne::{Report, Source};

use crate::utils::{error::Diagnose, span::Span};

#[derive(Debug)]
pub struct TypeError {}

impl Diagnose for Box<TypeError> {
    fn report(&self) -> Report<Span> {
        todo!()
    }

    fn source(&self) -> (String, Source) {
        todo!()
    }
}
