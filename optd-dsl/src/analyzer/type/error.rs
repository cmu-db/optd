use ariadne::{Report, Source};

use crate::utils::{error::Diagnose, span::Span};

#[derive(Debug)]
pub struct TypeError {}

impl Diagnose for TypeError {
    fn report(&self) -> Report<Span> {
        todo!()
    }

    fn source(&self) -> (String, Source) {
        todo!()
    }
}
