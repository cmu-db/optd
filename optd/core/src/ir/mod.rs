//! Intermediate Representation (IR) module.
//!
//! This module contains the core definitions and structures for the
//! intermediate representation used in the query optimizer. It includes
//! definitions for operators, scalars, groups, properties, and related
//! components.

use std::sync::{Arc, LazyLock};

pub mod builder;
pub mod catalog;
mod column;
mod context;
pub mod convert;
pub mod cost;
pub mod explain;
mod group;
mod macros;
pub mod operator;
pub mod properties;
pub mod rule;
pub mod scalar;
mod types;

pub use column::*;
pub use context::IRContext;
pub use group::*;
pub use operator::{Operator, OperatorCategory, OperatorKind};
use pretty_xmlish::Pretty;
pub use scalar::{Scalar, ScalarKind};
pub use types::DataType;
pub use types::value::ScalarValue;

use crate::ir::{
    explain::{Explain, ExplainOption},
    properties::OperatorProperties,
};

/// The portion of the IR shared by all nodes.
#[derive(Debug)]
pub struct IRCommon<P> {
    /// The input operators.
    input_operators: Arc<[Arc<Operator>]>,
    /// The input scalars.
    input_scalars: Arc<[Arc<Scalar>]>,
    properties: Arc<P>,
}

static EMPTY_INPUT_OPERATORS: LazyLock<Arc<[Arc<Operator>]>> = LazyLock::new(|| Arc::new([]));
static EMPTY_INPUT_SCALARS: LazyLock<Arc<[Arc<Scalar>]>> = LazyLock::new(|| Arc::new([]));

impl<P: Default> IRCommon<P> {
    pub(crate) fn new(
        input_operators: Arc<[Arc<Operator>]>,
        input_scalars: Arc<[Arc<Scalar>]>,
    ) -> Self {
        Self {
            input_operators,
            input_scalars,
            properties: Arc::default(),
        }
    }

    fn empty_input_operators() -> Arc<[Arc<Operator>]> {
        EMPTY_INPUT_OPERATORS.clone()
    }

    fn empty_input_scalars() -> Arc<[Arc<Scalar>]> {
        EMPTY_INPUT_SCALARS.clone()
    }

    pub fn empty() -> Self {
        Self::new(Self::empty_input_operators(), Self::empty_input_scalars())
    }

    pub fn with_input_scalars_only(scalars: Arc<[Arc<Scalar>]>) -> Self {
        Self::new(Self::empty_input_operators(), scalars)
    }

    pub fn with_input_operators_only(operators: Arc<[Arc<Operator>]>) -> Self {
        Self::new(operators, Self::empty_input_scalars())
    }

    pub(crate) fn new_with_properties(
        input_operators: Arc<[Arc<Operator>]>,
        input_scalars: Arc<[Arc<Scalar>]>,
        properties: Arc<P>,
    ) -> Self {
        Self {
            input_operators,
            input_scalars,
            properties,
        }
    }

    pub(crate) fn with_properties_only(properties: Arc<P>) -> Self {
        Self {
            input_operators: Self::empty_input_operators(),
            input_scalars: Self::empty_input_scalars(),
            properties,
        }
    }

    pub fn explain_input_operators<'a>(
        &self,
        ctx: &IRContext,
        option: &ExplainOption,
    ) -> Vec<Pretty<'a>> {
        self.input_operators
            .iter()
            .map(|input_op| input_op.explain(ctx, option))
            .collect()
    }
}

impl IRCommon<OperatorProperties> {
    pub fn explain_operator_properties<'a>(
        &self,
        _ctx: &IRContext,
        _option: &ExplainOption,
    ) -> Vec<(&'static str, Pretty<'a>)> {
        let mut fields = Vec::with_capacity(2);
        let cardinality = self
            .properties
            .cardinality
            .get()
            .map(|x| format!("{:.2}", x.as_f64()))
            .unwrap_or("?".to_string());
        let output_columns = self
            .properties
            .output_columns
            .get()
            .map(|x| format!("{x}"))
            .unwrap_or("?".to_string());
        fields.push(("(.output_columns)", Pretty::display(&output_columns)));
        fields.push(("(.cardinality)", Pretty::display(&cardinality)));
        fields
    }
}

impl<P> PartialEq for IRCommon<P> {
    fn eq(&self, other: &Self) -> bool {
        self.input_operators == other.input_operators && self.input_scalars == other.input_scalars
    }
}

impl<P> Eq for IRCommon<P> {}

impl<P> std::hash::Hash for IRCommon<P> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.input_operators.hash(state);
        self.input_scalars.hash(state);
    }
}

impl<P> Clone for IRCommon<P> {
    fn clone(&self) -> Self {
        Self {
            input_operators: self.input_operators.clone(),
            input_scalars: self.input_scalars.clone(),
            properties: self.properties.clone(),
        }
    }
}
