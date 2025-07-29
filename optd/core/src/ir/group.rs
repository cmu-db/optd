/// Uniquely identifies an equivalent class in the optimizer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(pub i64);

use std::sync::Arc;

use crate::ir::{
    IRCommon, Operator, OperatorKind, Scalar, ScalarKind,
    convert::{IntoOperator, IntoScalar, TryFromOperator, TryFromScalar},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupMetadata<T> {
    pub group_id: GroupId,
    pub normalized: Arc<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Group<T> {
    metadata: GroupMetadata<T>,
}

impl<T> Group<T> {
    pub fn normalized(&self) -> &Arc<T> {
        &self.metadata.normalized
    }

    pub fn group_id(&self) -> &GroupId {
        &self.metadata.group_id
    }

    pub fn from_raw_parts(metadata: GroupMetadata<T>) -> Self {
        Group { metadata }
    }
}

pub type OperatorGroup = Group<Operator>;
pub type OperatorGroupMetadata = GroupMetadata<Operator>;

pub type ScalarGroup = Group<Scalar>;
pub type ScalarGroupMetadata = GroupMetadata<Scalar>;

impl IntoOperator for OperatorGroup {
    fn into_operator(self) -> Arc<Operator> {
        Arc::new(Operator {
            kind: OperatorKind::Group(self.metadata),
            common: IRCommon::empty(),
        })
    }
}

impl TryFromOperator for OperatorGroup {
    fn try_from_operator(operator: Operator) -> Result<Self, OperatorKind> {
        match operator.kind {
            OperatorKind::Group(meta) => Ok(Group::from_raw_parts(meta)),
            kind => Err(kind),
        }
    }
}

impl IntoScalar for ScalarGroup {
    fn into_scalar(self) -> Arc<Scalar> {
        Arc::new(Scalar {
            kind: ScalarKind::Group(self.metadata),
            common: IRCommon::empty(),
        })
    }
}

impl TryFromScalar for ScalarGroup {
    fn try_from_scalar(scalar: Scalar) -> Result<Self, ScalarKind> {
        match scalar.kind {
            ScalarKind::Group(meta) => Ok(Group::from_raw_parts(meta)),
            kind => Err(kind),
        }
    }
}
