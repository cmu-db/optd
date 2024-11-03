use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};

use crate::rel_node::{RelNodeTyp, Value};
use std::fmt::Debug;

pub trait PropertyBuilderAny<T: RelNodeTyp>: 'static + Send + Sync {
    fn derive_any(
        &self,
        typ: T,
        data: Option<Value>,
        children: &[&serde_json::Value],
    ) -> serde_json::Value;
    fn display(&self, prop: &serde_json::Value) -> String;
    fn property_name(&self) -> &'static str;
}

pub trait PropertyBuilder<T: RelNodeTyp>: 'static + Send + Sync + Sized {
    type Prop: 'static + Send + Sync + Sized + Clone + Debug + Serialize + DeserializeOwned;
    fn derive(&self, typ: T, data: Option<Value>, children: &[&Self::Prop]) -> Self::Prop;
    fn property_name(&self) -> &'static str;
}

impl<T: RelNodeTyp, P: PropertyBuilder<T>> PropertyBuilderAny<T> for P {
    fn derive_any(
        &self,
        typ: T,
        data: Option<Value>,
        children: &[&serde_json::Value],
    ) -> serde_json::Value {
        let children: Vec<P::Prop> = children
            .iter()
            .map(|child| serde_json::from_value((*child).clone()).unwrap())
            .collect();
        let children_ref = children.iter().collect_vec();
        serde_json::to_value(self.derive(typ, data, &children_ref)).unwrap()
    }

    fn display(&self, prop: &serde_json::Value) -> String {
        let prop: P::Prop = serde_json::from_value(prop.clone()).unwrap();
        format!("{:?}", prop)
    }

    fn property_name(&self) -> &'static str {
        PropertyBuilder::property_name(self)
    }
}
