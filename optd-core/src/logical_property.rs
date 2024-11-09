// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::any::Any;
use std::fmt::{Debug, Display};

use crate::nodes::{ArcPredNode, NodeType};

pub trait LogicalProperty: 'static + Any + Send + Sync + Debug + Display {
    fn as_any(&self) -> &dyn Any;
}

pub trait LogicalPropertyBuilderAny<T: NodeType>: 'static + Send + Sync {
    fn derive_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children: &[&dyn LogicalProperty],
    ) -> Box<dyn LogicalProperty>;
    fn property_name(&self) -> &'static str;
}

pub trait LogicalPropertyBuilder<T: NodeType>: 'static + Send + Sync + Sized {
    type Prop: LogicalProperty + Sized + Clone;

    /// Derive the output logical property based on the input logical properties and the current plan node information.
    fn derive(&self, typ: T, predicates: &[ArcPredNode<T>], children: &[&Self::Prop])
        -> Self::Prop;

    fn property_name(&self) -> &'static str;
}

impl<T: NodeType, P: LogicalPropertyBuilder<T>> LogicalPropertyBuilderAny<T> for P {
    fn derive_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children: &[&dyn LogicalProperty],
    ) -> Box<dyn LogicalProperty> {
        let children: Vec<&P::Prop> = children
            .iter()
            .map(|child| {
                child
                    .as_any()
                    .downcast_ref::<P::Prop>()
                    .expect("Failed to downcast child")
            })
            .collect();
        Box::new(self.derive(typ, predicates, &children))
    }

    fn property_name(&self) -> &'static str {
        LogicalPropertyBuilder::property_name(self)
    }
}
