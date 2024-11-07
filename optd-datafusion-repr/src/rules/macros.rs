macro_rules! define_matcher {
    ( ( $typ:expr $(, $children:tt )* ) ) => {
        RuleMatcher::MatchNode {
            typ: $typ,
            children: vec![
                $( crate::rules::macros::define_matcher!($children) ),*
            ],
        }
    };
    ( $pick_one:tt ) => {
        RuleMatcher::Any
    };
}

macro_rules! define_rule_inner {
    ($rule_type:expr, $name:ident, $apply:ident, $($matcher:tt)+) => {
        pub struct $name {
            matcher: RuleMatcher<DfNodeType>,
        }

        impl $name {
            pub fn new() -> Self {
                #[allow(unused_imports)]
                use DfNodeType::*;
                let matcher = crate::rules::macros::define_matcher!($($matcher)+);
                Self { matcher }
            }
        }

        impl <O: Optimizer<DfNodeType>> Rule<DfNodeType, O> for $name {
            fn matcher(&self) -> &RuleMatcher<DfNodeType> {
                &self.matcher
            }

            fn apply(
                &self,
                optimizer: &O,
                binding: optd_core::nodes::ArcPlanNode<DfNodeType>,
            ) -> Vec<optd_core::nodes::PlanNodeOrGroup<DfNodeType>> {
                $apply(optimizer, binding)
            }

            camelpaste::paste! {
                fn name(&self) -> &'static str {
                    stringify!([< $name:snake >])
                }
            }

            fn is_impl_rule(&self) -> bool {
                $rule_type
            }
        }
    };
}

macro_rules! define_rule {
    ($name:ident, $apply:ident, $($matcher:tt)+) => {
        crate::rules::macros::define_rule_inner! { false, $name, $apply, $($matcher)+ }
    };
}

macro_rules! define_impl_rule {
    ($name:ident, $apply:ident, $($matcher:tt)+) => {
        crate::rules::macros::define_rule_inner! { true, $name, $apply, $($matcher)+ }
    };
}

pub(crate) use {define_impl_rule, define_matcher, define_rule, define_rule_inner};
