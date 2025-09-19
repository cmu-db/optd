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

pub trait TryBorrowOperator<'a>: Sized {
    fn try_borrow_operator(operator: &'a Operator) -> Result<Self, &'a OperatorKind>;
}

pub trait TryBorrowOperatorMarker<'a> {
    type BorrowedType: TryBorrowOperator<'a>;
}

impl Operator {
    /// Trys to bind a specific operator type. On error, returns the unmatched [`OperatorKind`].
    pub fn try_bind<T: TryFromOperator>(self) -> Result<T, OperatorKind> {
        T::try_from_operator(self)
    }

    pub fn bind<T: TryFromOperator>(self) -> T {
        T::try_from_operator(self).unwrap()
    }

    pub fn try_borrow<O>(
        &self,
    ) -> Result<<O as TryBorrowOperatorMarker<'_>>::BorrowedType, &OperatorKind>
    where
        for<'a> O: TryBorrowOperatorMarker<'a>,
    {
        O::BorrowedType::try_borrow_operator(self)
    }

    pub fn borrow<O>(&self) -> <O as TryBorrowOperatorMarker<'_>>::BorrowedType
    where
        for<'a> O: TryBorrowOperatorMarker<'a>,
    {
        O::BorrowedType::try_borrow_operator(self).unwrap()
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

pub trait TryBorrowScalar<'a>: Sized {
    fn try_borrow_scalar(scalar: &'a Scalar) -> Result<Self, &'a ScalarKind>;
}

pub trait TryBorrowScalarMarker<'a> {
    type BorrowedType: TryBorrowScalar<'a>;
}

impl Scalar {
    /// Trys to bind a specific scalar expression type. On error, returns the unmatched [`ScalarKind`].
    pub fn try_bind<T: TryFromScalar>(self) -> Result<T, ScalarKind> {
        T::try_from_scalar(self)
    }

    pub fn bind<T: TryFromScalar>(self) -> T {
        T::try_from_scalar(self).unwrap()
    }

    pub fn try_borrow<S>(
        &self,
    ) -> Result<<S as TryBorrowScalarMarker<'_>>::BorrowedType, &ScalarKind>
    where
        for<'a> S: TryBorrowScalarMarker<'a>,
    {
        S::BorrowedType::try_borrow_scalar(self)
    }

    pub fn borrow<S>(&self) -> <S as TryBorrowScalarMarker<'_>>::BorrowedType
    where
        for<'a> S: TryBorrowScalarMarker<'a>,
    {
        S::BorrowedType::try_borrow_scalar(self).unwrap()
    }
}
