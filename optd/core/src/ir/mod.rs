use std::sync::{Arc, LazyLock};

pub mod catalog;
mod column;
mod context;
pub mod convert;
pub mod cost;
mod data_type;
mod group;
mod macros;
pub mod operator;
pub mod properties;
pub mod rule;
pub mod scalar;
mod value;

pub use column::*;
pub use context::IRContext;
pub use data_type::DataType;
pub use group::*;
pub use operator::{Operator, OperatorCategory, OperatorKind};
pub use scalar::{Scalar, ScalarKind};
pub use value::ScalarValue;

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
