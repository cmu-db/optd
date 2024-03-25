use crate::rel_node::{RelNodeTyp, Value, RelNodeRef};
use std::{any::Any, fmt::Debug};

pub trait PhysicalPropertyBuilderAny<T: RelNodeTyp>: 'static + Send + Sync {
    fn derive_any(
        &self,
        typ: T,
        data: Option<Value>,
        children: &[&dyn Any],
    ) -> Box<dyn Any + Send + Sync + 'static>;
    fn satisfy_requirement(&self, derived: &dyn Any, required: &dyn Any) -> bool;
    fn enforce(&self, node: RelNodeRef<T>, derived: &dyn Any, required: &dyn Any) -> RelNodeRef<T>;
    fn display(&self, prop: &dyn Any) -> String;
    fn property_name(&self) -> &'static str;
}

pub trait PhysicalPropertyBuilder<T: RelNodeTyp>: 'static + Send + Sync + Sized {
    // Prop is the same for both derived and required property
    // eg. for sort, one can define type Prop = struct { sort_keys: Vec<SortKey>, orders: Vec<Order> }
    type Prop: 'static + Send + Sync + Sized + Clone + Debug;

    // derive gets the physical property for current node given its children derived properties and current data
    // for simple properties like sort, we can deduct current property. eg. A(a.x ordered) union B(b.x ordered) -> current node x is ordered
    // Like calcite, we should also give users the ability to define how the property is derived
    // eg. from left children, right children, both or customized derive function
    fn derive(&self, typ: T, data: Option<Value>, children: &[&Self::Prop]) -> Self::Prop;

    // satisfy_requirement checks if the derived property satisfies the required property
    // eg. for sort, required property is x ordered(asc), derived property is x ordered(asc), y ordered(asc)
    // then satisfy_requirement should return true
    fn satisfy_requirement(&self, derived: &Self::Prop, required: &Self::Prop) -> bool;

    // enforce adds the enforcer operator to the plan to satisfy the required property 
    fn enforce(&self, node: RelNodeRef<T>, derived: &Self::Prop, required: &Self::Prop) -> RelNodeRef<T>;

    fn property_name(&self) -> &'static str;
}

impl<T: RelNodeTyp, P: PhysicalPropertyBuilder<T>> PhysicalPropertyBuilderAny<T> for P {
    fn derive_any(
        &self,
        typ: T,
        data: Option<Value>,
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
        Box::new(self.derive(typ, data, &children))
    }

    fn satisfy_requirement(&self, derived: &dyn Any, required: &dyn Any) -> bool {
        let derived = derived
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast derived property");
        let required = required
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast required property");
        self.satisfy_requirement(derived, required)
    }

    fn enforce(&self, node: RelNodeRef<T>, derived: &dyn Any, required: &dyn Any) -> RelNodeRef<T> {
        let derived = derived
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast derived property");
        let required = required
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast required property");
        self.enforce(node, derived, required)
    }

    fn display(&self, prop: &dyn Any) -> String {
        let prop = prop
            .downcast_ref::<P::Prop>()
            .expect("Failed to downcast property");
        format!("{:?}", prop)
    }

    fn property_name(&self) -> &'static str {
        PhysicalPropertyBuilder::property_name(self)
    }
}
