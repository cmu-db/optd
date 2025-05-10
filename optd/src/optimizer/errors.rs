use crate::memo::MemoError;

#[derive(Debug)]
pub enum OptimizeError {
    MemoError(MemoError),
}
