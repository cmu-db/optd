// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::any::Any;
use std::borrow::Borrow;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use itertools::Itertools;

use crate::nodes::{ArcPredNode, NodeType, PlanNode, PlanNodeOrGroup};

pub trait PhysicalProperty: 'static + Any + Send + Sync + Debug + Display {
    fn as_any(&self) -> &dyn Any;
    fn to_boxed(&self) -> Box<dyn PhysicalProperty>;
}

pub trait PhysicalPropertyBuilderAny<T: NodeType>: 'static + Send + Sync {
    fn derive_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children: &[&dyn PhysicalProperty],
    ) -> Box<dyn PhysicalProperty>;

    fn passthrough_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required_property: &dyn PhysicalProperty,
    ) -> Vec<Box<dyn PhysicalProperty>>;

    fn satisfies_any(&self, prop: &dyn PhysicalProperty, required: &dyn PhysicalProperty) -> bool;

    fn enforce_any(&self, prop: &dyn PhysicalProperty) -> (T, Vec<ArcPredNode<T>>);

    fn default_any(&self) -> Box<dyn PhysicalProperty>;

    fn property_name(&self) -> &'static str;
}

pub trait PhysicalPropertyBuilder<T: NodeType>: 'static + Send + Sync + Sized {
    type Prop: PhysicalProperty + Clone + Sized;

    /// Derive the output physical property based on the input physical properties and the current plan node information.
    fn derive(&self, typ: T, predicates: &[ArcPredNode<T>], children: &[&Self::Prop])
        -> Self::Prop;

    /// Passsthrough the `required` properties to the children if possible. Returns the derived properties for each child.
    /// If nothing can be passthroughed, simply return the default properties for each child.
    fn passthrough(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required: &Self::Prop,
    ) -> Vec<Self::Prop>;

    /// Check if the input physical property satisfies the required output physical property.
    fn satisfies(&self, prop: &Self::Prop, required: &Self::Prop) -> bool;

    /// Enforce the required output physical property on the input plan node.
    fn enforce(&self, prop: &Self::Prop) -> (T, Vec<ArcPredNode<T>>);

    /// Represents no requirement on a property.
    fn default(&self) -> Self::Prop;

    fn property_name(&self) -> &'static str;
}

impl<T: NodeType, P: PhysicalPropertyBuilder<T>> PhysicalPropertyBuilderAny<T> for P {
    fn derive_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children: &[&dyn PhysicalProperty],
    ) -> Box<dyn PhysicalProperty> {
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

    fn passthrough_any(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required: &dyn PhysicalProperty,
    ) -> Vec<Box<dyn PhysicalProperty>> {
        let required = required
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast required property");
        self.passthrough(typ, predicates, required)
            .into_iter()
            .map(|prop| Box::new(prop) as Box<dyn PhysicalProperty>)
            .collect()
    }

    fn satisfies_any(&self, prop: &dyn PhysicalProperty, required: &dyn PhysicalProperty) -> bool {
        let prop = prop
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast property");
        let required = required
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast required property");
        self.satisfies(prop, required)
    }

    fn enforce_any(&self, prop: &dyn PhysicalProperty) -> (T, Vec<ArcPredNode<T>>) {
        let prop = prop
            .as_any()
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast property");
        self.enforce(prop)
    }

    fn default_any(&self) -> Box<dyn PhysicalProperty> {
        Box::new(self.default())
    }

    fn property_name(&self) -> &'static str {
        PhysicalPropertyBuilder::property_name(self)
    }
}

pub(crate) struct PhysicalPropertyBuilders<T: NodeType>(
    pub Arc<[Box<dyn PhysicalPropertyBuilderAny<T>>]>,
);

/// Represents a set of physical properties for a specific plan node
pub(crate) type PhysicalPropertySet = Vec<Box<dyn PhysicalProperty>>;

impl<T: NodeType> PhysicalPropertyBuilders<T> {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Takes children_len x props_len (input properties for each child) and returns props_len derived properties
    pub fn derive_many<X, Y, Z>(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children: Z,
        children_len: usize,
    ) -> PhysicalPropertySet
    where
        X: Borrow<dyn PhysicalProperty>,
        Y: AsRef<[X]>,
        Z: AsRef<[Y]>,
    {
        if let Some(first) = children.as_ref().first() {
            assert_eq!(first.as_ref().len(), self.0.len())
        }
        let children = children.as_ref();
        assert_eq!(children.len(), children_len);
        let mut derived_prop = Vec::with_capacity(self.0.len());
        for i in 0..self.0.len() {
            let builder = &self.0[i];
            let children = children
                .iter()
                .map(|child| child.as_ref()[i].borrow())
                .collect_vec();
            let prop = builder.derive_any(typ.clone(), predicates, &children);
            derived_prop.push(prop);
        }
        derived_prop
    }

    /// Returns children_len x props_len (required properties for each child)
    pub fn passthrough_many_no_required_property(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        children_len: usize,
    ) -> Vec<PhysicalPropertySet> {
        let required_prop = self
            .0
            .iter()
            .map(|builder| builder.default_any())
            .collect_vec();
        self.passthrough_many(typ, predicates, required_prop, children_len)
    }

    /// Returns children_len x props_len (required properties for each child)
    pub fn passthrough_many<X, Y>(
        &self,
        typ: T,
        predicates: &[ArcPredNode<T>],
        required: Y,
        children_len: usize,
    ) -> Vec<PhysicalPropertySet>
    where
        X: Borrow<dyn PhysicalProperty>,
        Y: AsRef<[X]>,
    {
        let required = required.as_ref();
        assert_eq!(self.0.len(), required.len());
        let mut required_prop = Vec::with_capacity(children_len);
        required_prop.resize_with(children_len, Vec::new);
        for i in 0..self.0.len() {
            let builder = &self.0[i];
            let required = builder.passthrough_any(typ.clone(), predicates, required[i].borrow());
            assert_eq!(
                required.len(),
                children_len,
                "required properties length mismatch: passthrough {} != children_num {} for property {} and plan node typ {}",
                required.len(),
                children_len,
                builder.property_name(),
                typ
            );
            for (child_idx, child_prop) in required.into_iter().enumerate() {
                required_prop[child_idx].push(child_prop);
            }
        }
        required_prop
    }

    pub fn default_many(&self) -> PhysicalPropertySet {
        self.0.iter().map(|builder| builder.default_any()).collect()
    }

    /// Enforces the required properties on the input plan node if not satisfied.
    pub fn enforce_many_if_not_satisfied<X1, Y1, X2, Y2>(
        &self,
        child: PlanNodeOrGroup<T>,
        input_props: Y1,
        required_props: Y2,
    ) -> (PlanNodeOrGroup<T>, Vec<Box<dyn PhysicalProperty>>)
    where
        X1: Borrow<dyn PhysicalProperty>,
        Y1: AsRef<[X1]>,
        X2: Borrow<dyn PhysicalProperty>,
        Y2: AsRef<[X2]>,
    {
        let input_props = input_props.as_ref();
        let required_props = required_props.as_ref();
        let mut new_props = input_props
            .iter()
            .map(|x| x.borrow().to_boxed())
            .collect_vec();

        assert_eq!(self.0.len(), input_props.len());
        assert_eq!(self.0.len(), required_props.len());
        let mut child = child;
        for i in 0..self.0.len() {
            let builder = &self.0[i];
            let input_prop = input_props[i].borrow();
            let required_prop = required_props[i].borrow();
            if !builder.satisfies_any(input_prop, required_prop) {
                let (typ, predicates) = builder.enforce_any(required_prop);
                child = PlanNodeOrGroup::PlanNode(Arc::new(PlanNode {
                    typ,
                    predicates,
                    children: vec![child],
                }));
                // TODO: check if the new plan node really enforced the property by deriving in the very end;
                // enforcing new properties might cause the old properties to be lost. For example, order matters
                // when enforcing gather + sort.
                new_props[i] = required_prop.to_boxed();
            }
        }
        (child, new_props)
    }
}
