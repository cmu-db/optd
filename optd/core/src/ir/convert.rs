use std::sync::Arc;

use crate::ir::{Operator, OperatorKind, Scalar, ScalarKind};

pub trait IntoOperator {
    /// Converts the specfic operator type to the generic [`Operator`].
    fn into_operator(self) -> Arc<Operator>;
}

pub trait TryFromOperator: Sized {
    /// Trys to convert from an [`Operator`]. On error, returns the unmatched [`OperatorKind`].
    fn try_from_operator(operator: Operator) -> Result<Self, OperatorKind>;
}

impl Operator {
    /// Trys to bind a specific operator type. On error, returns the unmatched [`OperatorKind`].
    pub fn try_bind<T: TryFromOperator>(self) -> Result<T, OperatorKind> {
        T::try_from_operator(self)
    }

    pub fn try_bind_ref<T: TryFromOperator>(&self) -> Result<T, OperatorKind> {
        T::try_from_operator(self.clone())
    }
}

pub trait IntoScalar {
    /// Converts the specfic scalar expression type to the generic [`Scalar`].
    fn into_scalar(self) -> Arc<Scalar>;
}

pub trait TryFromScalar: Sized {
    /// Trys to convert from a [`Scalar`]. On error, returns the unmatched [`ScalarKind`].
    fn try_from_scalar(scalar: Scalar) -> Result<Self, ScalarKind>;
}

impl Scalar {
    /// Trys to bind a specific scalar expression type. On error, returns the unmatched [`ScalarKind`].
    pub fn try_bind<T: TryFromScalar>(self) -> Result<T, ScalarKind> {
        T::try_from_scalar(self)
    }

    pub fn try_bind_ref<T: TryFromScalar>(&self) -> Result<T, ScalarKind> {
        T::try_from_scalar(self.clone())
    }
}
