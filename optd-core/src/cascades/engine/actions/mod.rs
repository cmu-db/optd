use std::sync::Arc;

pub mod analyzers;
pub mod transformers;

type WithBinding<T> = (String, Arc<T>);
