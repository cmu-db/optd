macro_rules! define_matcher {
    // Entry point: Processes both children and predicates lists.
    ( $pick_num:ident, ( $typ:expr, [ $($children:tt),* ], [ $($predicates:tt),* ] ) ) => {
        RuleMatcher::MatchNode {
            typ: $typ,
            children: vec![
                $( crate::rules::macros::define_matcher!(@child $pick_num, $children) ),*
            ],
            predicates: vec![
                $( crate::rules::macros::define_matcher!(@pred $pick_num, $predicates) ),*
            ],
        }
    };

    // Child case: Processes each child as `PickOne`
    (@child $pick_num:ident, $pick_one:tt ) => {
        RuleMatcher::PickOne {
            pick_to: { let x = $pick_num; $pick_num += 1; x },
        }
    };

    // Predicate case: Processes each predicate as `PickPred`
    (@pred $pick_num:ident, $pick_pred:tt ) => {
        RuleMatcher::PickPred
    };
}

macro_rules! define_picks {
    // Entry rule to handle type, children, and predicates lists
    ( ( $typ:expr, [ $($children:ident),* ], [ $($predicates:ident),* ] ) ) => {
        // Initialize variables for children
        $( crate::rules::macros::define_picks!(@child $children); )*
        // Initialize variables for predicates
        $( crate::rules::macros::define_picks!(@pred $predicates); )*
    };

    // Handle each child
    (@child $pick_one:ident ) => {
        let $pick_one: PlanNodeOrGroup<DfNodeType>;
    };

    // Handle each predicate
    (@pred $pred_one:ident ) => {
        let $pred_one: ArcPredNode<DfNodeType>;
    };
}

macro_rules! collect_picks {
    // Match the main structure: name, type, plan nodes, and predicates
    ($name:ident, ( $typ:expr, [ $( $children:ident ),* ], [ $( $predicates:ident ),* ] )) => {
        pub struct $name {
            // Generate fields for each child
            $( $children, )*
            // Generate fields for each predicate
            $( $predicates, )*
        }
    };
}

macro_rules! define_picks_struct {
    // Match the main structure: name, type, plan nodes, and predicates
    ($name:ident, ( $typ:expr, [ $( $children:ident ),* ], [ $( $predicates:ident ),* ] )) => {
        pub struct $name {
            // Generate fields for each child
            $( pub $children: PlanNodeOrGroup<DfNodeType>, )*
            // Generate fields for each predicate
            $( pub $predicates: ArcPredNode<DfNodeType>, )*
        }
    };
}

macro_rules! apply_matcher {
    ( $pick_num:ident, $pred_pick_num:ident, $input:ident, $pred_input:ident, ( $typ:expr, [ $($children:tt),* ], [ $($predicates:tt),* ] ) ) => {
        $( crate::rules::macros::apply_matcher!(@child $pick_num, $input, $children); )*
        $( crate::rules::macros::apply_matcher!(@pred $pred_pick_num, $pred_input, $predicates); )*
    };
    // Each child is assigned from `input`
    (@child $pick_num:ident, $input:ident, $pick_one:ident ) => {
        {
            $pick_one = $input.remove(&$pick_num).unwrap();
            $pick_num += 1;
        }
    };

    // Each predicate is assigned from `pred_input`
    (@pred $pred_pick_num:ident, $pred_input:ident, $pick_pred:ident ) => {
        {
            $pick_pred = $pred_input.remove(&$pred_pick_num).unwrap();
            $pred_pick_num += 1;
        }
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

                let mut pick_num = 0;
                let matcher = crate::rules::macros::define_matcher!(pick_num, $($matcher)+);
                let _ = pick_num;
                Self { matcher }
            }
        }

        camelpaste::paste! {
            crate::rules::macros::define_picks_struct! { [<$name Picks>], $($matcher)+ }
        }

        impl <O: Optimizer<DfNodeType>> Rule<DfNodeType, O> for $name {
            fn matcher(&self) -> &RuleMatcher<DfNodeType> {
                &self.matcher
            }

            fn apply(
                &self,
                optimizer: &O,
                mut input: HashMap<usize, PlanNodeOrGroup<DfNodeType>>,
                mut pred_input: HashMap<usize, ArcPredNode<DfNodeType>>,
            ) -> Vec<PlanNode<DfNodeType>> {

                crate::rules::macros::define_picks!( $($matcher)+ );

                let mut pick_num = 0;
                let mut pred_pick_num = 0;

                crate::rules::macros::apply_matcher!(pick_num, pred_pick_num, input, pred_input, $($matcher)+);
                let res;
                camelpaste::paste! {
                    res = crate::rules::macros::collect_picks!( [<$name Picks>], $($matcher)+ );
                }
                let _ = pick_num;
                $apply(optimizer, res)
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

pub(crate) use apply_matcher;
pub(crate) use collect_picks;
pub(crate) use define_impl_rule;
pub(crate) use define_matcher;
pub(crate) use define_picks;
pub(crate) use define_picks_struct;
pub(crate) use define_rule;
pub(crate) use define_rule_inner;
