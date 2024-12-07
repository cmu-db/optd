// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

macro_rules! define_matcher {
    ( $discriminant:expr, ( $typ:expr $(, $children:tt )* ) ) => {
        if $discriminant {
            RuleMatcher::MatchDiscriminant {
                typ_discriminant: std::mem::discriminant(&$typ),
                children: vec![
                    $( crate::rules::macros::define_matcher!($discriminant, $children) ),*
                ],
            }
        } else {
            RuleMatcher::MatchNode {
                typ: $typ,
                children: vec![
                    $( crate::rules::macros::define_matcher!($discriminant, $children) ),*
                ],
            }
        }
    };
    ( $discriminant:expr, $pick_one:tt ) => {
        RuleMatcher::Any
    };
}

macro_rules! define_rule_inner {
    ($rule_type:expr, $discriminant:expr, $name:ident, $apply:ident, $($matcher:tt)+) => {
        pub struct $name {
            matcher: RuleMatcher<DfNodeType>,
        }

        impl $name {
            pub fn new() -> Self {
                #[allow(unused_imports)]
                use DfNodeType::*;
                let matcher = crate::rules::macros::define_matcher! { $discriminant, $($matcher)+ };
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
        crate::rules::macros::define_rule_inner! { false, false, $name, $apply, $($matcher)+ }
    };
}

macro_rules! define_rule_discriminant {
    ($name:ident, $apply:ident, $($matcher:tt)+) => {
        crate::rules::macros::define_rule_inner! { false, true, $name, $apply, $($matcher)+ }
    };
}

macro_rules! define_impl_rule {
    ($name:ident, $apply:ident, $($matcher:tt)+) => {
        crate::rules::macros::define_rule_inner! { true, false, $name, $apply, $($matcher)+ }
    };
}

pub(crate) use {
    define_impl_rule, define_matcher, define_rule, define_rule_discriminant, define_rule_inner,
};
