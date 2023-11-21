use itertools::Itertools;

use crate::rel_node::{RelNodeTyp, Value};
use std::any::Any;

trait PropertyAny<T: RelNodeTyp>: 'static + Send + Sync {
    fn derive_any(typ: T, data: Option<Value>, children: &[Box<dyn Any>]) -> Box<dyn Any>;
}

pub trait Property<T: RelNodeTyp>: 'static + Send + Sync + Sized {
    type Prop: 'static + Send + Sync + Sized;
    fn derive(typ: T, data: Option<Value>, children: &[&Self::Prop]) -> Self::Prop;
}

impl<T: RelNodeTyp, P: Property<T>> PropertyAny<T> for P {
    fn derive_any(typ: T, data: Option<Value>, children: &[Box<dyn Any>]) -> Box<dyn Any> {
        let children: Vec<&P::Prop> = children
            .iter()
            .map(|child| {
                child
                    .downcast_ref::<P::Prop>()
                    .expect("Failed to downcast child")
            })
            .collect();
        Box::new(Self::derive(typ, data, &children))
    }
}
