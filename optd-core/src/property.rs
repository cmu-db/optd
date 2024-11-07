// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::any::Any;
use std::fmt::{Debug, Display};

use crate::nodes::{ArcPredNode, NodeType};

pub trait PropertyBuilderAny<T: NodeType>: 'static + Send + Sync {
    fn derive_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children: &[&dyn Any],
    ) -> Box<dyn Any + Send + Sync + 'static>;
    fn display(&self, prop: &dyn Any) -> String;
    fn property_name(&self) -> &'static str;
}

pub trait PropertyBuilder<T: NodeType>: 'static + Send + Sync + Sized {
    type Prop: 'static + Send + Sync + Sized + Clone + Debug + Display;
    fn derive(&self, typ: T, predicates: &[ArcPredNode<T>], children: &[&Self::Prop])
        -> Self::Prop;
    fn property_name(&self) -> &'static str;
}

impl<T: NodeType, P: PropertyBuilder<T>> PropertyBuilderAny<T> for P {
    fn derive_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children: &[&dyn Any],
    ) -> Box<dyn Any + Send + Sync + 'static> {
        let children: Vec<&P::Prop> = children
            .iter()
            .map(|child| {
                child
                    .downcast_ref::<P::Prop>()
                    .expect("Failed to downcast child")
            })
            .collect();
        Box::new(self.derive(typ, predicates, &children))
    }

    fn display(&self, prop: &dyn Any) -> String {
        let prop = prop
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast property");
        prop.to_string()
    }

    fn property_name(&self) -> &'static str {
        PropertyBuilder::property_name(self)
    }
}
