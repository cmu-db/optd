// Helper macro to generate getter method for a specific input at an index
macro_rules! generate_input_getter {
    (operator, $method_name:ident, $index:expr) => {
        pub fn $method_name(&self) -> &std::sync::Arc<crate::ir::Operator> {
            &self.common.input_operators[$index]
        }
    };
    (scalar, $method_name:ident, $index:expr) => {
        pub fn $method_name(&self) -> &std::sync::Arc<crate::ir::Scalar> {
            &self.common.input_scalars[$index]
        }
    };
    (operator[], $method_name:ident) => {
        pub fn $method_name(&self) -> &[std::sync::Arc<crate::ir::Operator>] {
            &self.common.input_operators
        }
    };
    (scalar[], $method_name:ident) => {
        pub fn $method_name(&self) -> &[std::sync::Arc<crate::ir::Scalar>] {
            &self.common.input_scalars
        }
    };
}

// Helper macro to generate operator input getters
macro_rules! generate_operator_input_getters {
    ($node_type:ident, $ref_type:ident, [$($op:ident),*]) => {
        impl $node_type {
            crate::ir::macros::generate_operator_input_getters!(@getters 0, $($op),*);
        }

        impl<'ir> $ref_type<'ir> {
            crate::ir::macros::generate_operator_input_getters!(@getters 0, $($op),*);
        }
    };

    (@getters $index:expr, $first:ident $(, $rest:ident)*) => {
        crate::ir::macros::generate_input_getter!(operator, $first, $index);
        crate::ir::macros::generate_operator_input_getters!(@getters $index + 1, $($rest),*);
    };

    (@getters $index:expr,) => {};

    ($node_type:ident, $ref_type:ident, $ops:ident[]) => {
        impl $node_type {
            crate::ir::macros::generate_input_getter!(operator[], $ops);
        }

        impl<'ir> $ref_type<'ir> {
            crate::ir::macros::generate_input_getter!(operator[], $ops);
        }
    };
}

// Helper macro to generate scalar input getters
macro_rules! generate_scalar_input_getters {
    ($node_type:ident, $ref_type:ident, [$($scalar:ident),*]) => {
        impl $node_type {
            crate::ir::macros::generate_scalar_input_getters!(@getters 0, $($scalar),*);
        }

        impl<'ir> $ref_type<'ir> {
            crate::ir::macros::generate_scalar_input_getters!(@getters 0, $($scalar),*);
        }
    };

    (@getters $index:expr, $first:ident $(, $rest:ident)*) => {
        crate::ir::macros::generate_input_getter!(scalar, $first, $index);
        crate::ir::macros::generate_scalar_input_getters!(@getters $index + 1, $($rest),*);
    };

    (@getters $index:expr,) => {};

    ($node_type:ident, $ref_type:ident, $scalars:ident[]) => {
        impl $node_type {
            crate::ir::macros::generate_input_getter!(scalar[], $scalars);
        }

        impl<'ir> $ref_type<'ir> {
            crate::ir::macros::generate_input_getter!(scalar[], $scalars);
        }
    };
}

// Helper macro to generate metadata field getters
macro_rules! generate_metadata_getters {
    ($node_type:ident, $ref_type:ident, $($field:ident: $field_type:ty),*) => {
        impl $node_type {
            $(
                pub fn $field(&self) -> &$field_type {
                    &self.meta.$field
                }
            )*
        }

        impl<'ir> $ref_type<'ir> {
            $(
                pub fn $field(&self) -> &$field_type {
                    &self.meta.$field
                }
            )*
        }
    };
}

// NEW: Common node generation macro
macro_rules! generate_common_node {
    ($node_name:ident, $ref_name:ident, $(#[$($m:meta)*])*, $props_type:ty, $metadata_type:ident, $($field_name:ident: $field_type:ty),*) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $metadata_type {
            $(pub $field_name: $field_type),*
        }

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        $(#[$($m)*])*
        pub struct $node_name {
            meta: $metadata_type,
            common: crate::ir::IRCommon<$props_type>,
        }

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $ref_name<'ir> {
            meta: &'ir $metadata_type,
            common: &'ir crate::ir::IRCommon<$props_type>,
        }

        impl $node_name {
            /// Constructs the operator from raw metadata and IR inputs.
            pub fn from_raw_parts(meta: $metadata_type, common: crate::ir::IRCommon<$props_type>) -> Self {
                Self {
                    meta,
                    common,
                }
            }

            /// Constructs the operator from raw metadata and IR inputs.
            pub fn borrow_raw_parts<'ir>(meta: &'ir $metadata_type, common: &'ir crate::ir::IRCommon<$props_type>) -> $ref_name<'ir> {
                $ref_name {
                    meta,
                    common,
                }
            }

            /// Gets a slice to the input operators.
            pub fn input_operators(&self) -> &[std::sync::Arc<crate::ir::Operator>] {
                &self.common.input_operators
            }

            /// Gets a slice to the input scalar expressions.
            pub fn input_scalars(&self) -> &[std::sync::Arc<crate::ir::Scalar>] {
                &self.common.input_scalars
            }
        }


    };
}

// Main operator definition macro (refactored)
macro_rules! define_node {
    (
        $(#[$($m:meta)*])*
        $node_name:ident, $ref_name:ident {
            properties: $props_type:ty,
            metadata: $metadata_type:ident {
                $($field_name:ident: $field_type:ty),* $(,)?
            },
            inputs: {
                operators: [$($op_input:ident),*],
                scalars: [$($scalar_input:ident),*]$(,)?
            }$(,)?
        }$(,)?
    ) => {
        crate::ir::macros::generate_common_node!($node_name, $ref_name, $(#[$($m)*])*, $props_type, $metadata_type, $($field_name: $field_type),*);
        crate::ir::macros::generate_metadata_getters!($node_name, $ref_name, $($field_name: $field_type),*);
        crate::ir::macros::generate_operator_input_getters!($node_name, $ref_name, [$($op_input),*]);
        crate::ir::macros::generate_scalar_input_getters!($node_name, $ref_name, [$($scalar_input),*]);
    };

    (
        $(#[$($m:meta)*])*
        $node_name:ident, $ref_name:ident {
            properties: $props_type:ty,
            metadata: $metadata_type:ident {
                $($field_name:ident: $field_type:ty),* $(,)?
            },
            inputs: {
                operators: $op_inputs:ident[],
                scalars: [$($scalar_input:ident),*]$(,)?
            }$(,)?
        }$(,)?
    ) => {
        crate::ir::macros::generate_common_node!($node_name, $ref_name, $(#[$($m)*])*, $props_type, $metadata_type, $($field_name: $field_type),*);
        crate::ir::macros::generate_metadata_getters!($node_name, $ref_name, $($field_name: $field_type),*);
        crate::ir::macros::generate_operator_input_getters!($node_name, $ref_name, $op_inputs[]);
        crate::ir::macros::generate_scalar_input_getters!($node_name, $ref_name, [$($scalar_input),*]);
    };


    (
        $(#[$($m:meta)*])*
        $node_name:ident, $ref_name:ident {
            properties: $props_type:ty,
            metadata: $metadata_type:ident {
                $($field_name:ident: $field_type:ty),* $(,)?
            },
            inputs: {
                operators: [$($op_input:ident),*],
                scalars: $scalar_inputs:ident[]$(,)?
            }$(,)?
        }$(,)?
    ) => {
        crate::ir::macros::generate_common_node!($node_name, $ref_name, $(#[$($m)*])*, $props_type, $metadata_type, $($field_name: $field_type),*);
        crate::ir::macros::generate_metadata_getters!($node_name, $ref_name, $($field_name: $field_type),*);
        crate::ir::macros::generate_operator_input_getters!($node_name, $ref_name, [$($op_input),*]);
        crate::ir::macros::generate_scalar_input_getters!($node_name, $ref_name, $scalar_inputs[]);
    };

    (
        $(#[$($m:meta)*])*
        $node_name:ident, $ref_name:ident {
            properties: $props_type:ty,
            metadata: $metadata_type:ident {
                $($field_name:ident: $field_type:ty),* $(,)?
            },
            inputs: {
                operators: $op_inputs:ident[],
                scalars: $scalar_inputs:ident[]$(,)?
            }$(,)?
        }
    ) => {
        crate::ir::macros::generate_common_node!($node_name, $ref_name, $(#[$($m)*])*, $props_type, $metadata_type, $($field_name: $field_type),*);
        crate::ir::macros::generate_metadata_getters!($node_name, $ref_name, $($field_name: $field_type),*);
        crate::ir::macros::generate_operator_input_getters!($node_name, $ref_name, $op_inputs[]);
        crate::ir::macros::generate_scalar_input_getters!($node_name, $ref_name, $scalar_inputs[]);
    };
}

macro_rules! impl_scalar_conversion {
    ($node_name:ident, $ref_name:ident) => {
        impl crate::ir::convert::TryFromScalar for $node_name {
            fn try_from_scalar(scalar: crate::ir::Scalar) -> Result<Self, crate::ir::ScalarKind> {
                match scalar.kind {
                    crate::ir::ScalarKind::$node_name(meta) => {
                        Ok($node_name::from_raw_parts(meta, scalar.common))
                    }
                    other_kind => Err(other_kind),
                }
            }
        }

        impl crate::ir::convert::IntoScalar for $node_name {
            fn into_scalar(self) -> std::sync::Arc<crate::ir::Scalar> {
                std::sync::Arc::new(crate::ir::Scalar {
                    kind: crate::ir::ScalarKind::$node_name(self.meta),
                    common: self.common,
                })
            }
        }

        impl<'a> crate::ir::convert::TryBorrowScalar<'a> for $ref_name<'a> {
            fn try_borrow_scalar(
                scalar: &'a crate::ir::Scalar,
            ) -> Result<Self, &'a crate::ir::ScalarKind> {
                match &scalar.kind {
                    crate::ir::ScalarKind::$node_name(meta) => {
                        Ok($node_name::borrow_raw_parts(meta, &scalar.common))
                    }
                    other_kind => Err(other_kind),
                }
            }
        }

        impl<'ir> crate::ir::convert::TryBorrowScalarMarker<'ir> for $node_name {
            type BorrowedType = $ref_name<'ir>;
        }
    };
}

macro_rules! impl_operator_conversion {
    ($node_name:ident, $ref_name:ident) => {
        impl crate::ir::convert::TryFromOperator for $node_name {
            fn try_from_operator(
                operator: crate::ir::Operator,
            ) -> Result<Self, crate::ir::OperatorKind> {
                match operator.kind {
                    crate::ir::OperatorKind::$node_name(meta) => {
                        Ok($node_name::from_raw_parts(meta, operator.common))
                    }
                    other_kind => Err(other_kind),
                }
            }
        }

        impl crate::ir::convert::IntoOperator for $node_name {
            fn into_operator(self) -> std::sync::Arc<crate::ir::Operator> {
                std::sync::Arc::new(crate::ir::Operator {
                    group_id: None,
                    kind: crate::ir::OperatorKind::$node_name(self.meta),
                    common: self.common,
                })
            }
        }

        impl<'a> crate::ir::convert::TryBorrowOperator<'a> for $ref_name<'a> {
            fn try_borrow_operator(
                operator: &'a crate::ir::Operator,
            ) -> Result<Self, &'a crate::ir::OperatorKind> {
                match &operator.kind {
                    crate::ir::OperatorKind::$node_name(meta) => {
                        Ok($node_name::borrow_raw_parts(meta, &operator.common))
                    }
                    other_kind => Err(other_kind),
                }
            }
        }

        impl<'ir> crate::ir::convert::TryBorrowOperatorMarker<'ir> for $node_name {
            type BorrowedType = $ref_name<'ir>;
        }
    };
}

pub(super) use {
    define_node, generate_common_node, generate_input_getter, generate_metadata_getters,
    generate_operator_input_getters, generate_scalar_input_getters, impl_operator_conversion,
    impl_scalar_conversion,
};

mod tests {
    use crate::ir::properties::{OperatorProperties, ScalarProperties};

    #[allow(dead_code)]
    fn all_branches_compiles() {
        // === OPERATOR TESTS ===

        // # metadata field: 0
        // # input op:       0
        // # input scalar:   0
        define_node!(Node0, Node0Ref {
            properties: ScalarProperties,
            metadata: Node0Metadata {},
            inputs: {
                operators: [],
                scalars: [],
            }
        });

        // # metadata field: 1
        // # input op:       0
        // # input scalar:   0
        define_node!(
            /// This is node1. [`Node1Ref`] is the borrowed version.
            Node1, Node1Ref {
            properties: ScalarProperties,
            metadata: Node1Metadata {
                m1: usize,
            },
            inputs: {
                operators: [],
                scalars: [],
            }
        });

        // # metadata field: 2
        // # input op:       0
        // # input scalar:   0
        define_node!(Node2, Node2Ref {
            properties: ScalarProperties,
            metadata: Node2Metadata {
                m1: usize,
                m2: String,
            },
            inputs: {
                operators: [],
                scalars: [],
            }
        });

        // # metadata field: 2
        // # input op:       2
        // # input scalar:   2
        define_node!(Node3, Node3Ref {
            properties: ScalarProperties,
            metadata: Node3Metadata {
                m1: usize,
                m2: String,
            },
            inputs: {
                operators: [op1, op2],
                scalars: [s1, s2],
            }
        });

        // # metadata field: 2
        // # input op:       n
        // # input scalar:   n
        define_node!(Node4, Node4Ref {
            properties: OperatorProperties,
            metadata: Node4Metadata {
                m1: usize,
                m2: String,
            },
            inputs: {
                operators: ops[],
                scalars: ssss[],
            }
        });

        // # metadata field: 2
        // # input op:       n
        // # input scalar:   2
        define_node!(Node5, Node5Ref {
            properties: OperatorProperties,
            metadata: Node5Metadata {
                m1: usize,
                m2: String,
            },
            inputs: {
                operators: ops[],
                scalars: [s1, s2],
            }
        });

        // # metadata field: 2
        // # input op:       2
        // # input scalar:   n
        define_node!(
            Node6, Node6Ref {
            properties: OperatorProperties,
            metadata: Node6Metadata {
                m1: usize,
                m2: String,
            },
            inputs: {
                operators: [op1, op2],
                scalars: ssss[],
            }
        });
    }
}
