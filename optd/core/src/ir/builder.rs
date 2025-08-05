use std::sync::Arc;

use crate::ir::{
    Column, Group, GroupId, Operator, Scalar, ScalarValue,
    convert::{IntoOperator, IntoScalar},
    operator::{LogicalJoin, LogicalSelect, PhysicalFilter, PhysicalNLJoin, join::JoinType},
    properties::OperatorProperties,
    scalar::*,
};

/// Creates a mock scan operator.
pub fn mock_scan(id: usize, columns: Vec<i64>, card: f64) -> Arc<Operator> {
    use crate::ir::operator::{MockScan, MockSpec};

    let spec = MockSpec::new_test_only(columns, card);
    MockScan::with_mock_spec(id, spec).into_operator()
}

pub fn group(group_id: GroupId, properties: Arc<OperatorProperties>) -> Arc<Operator> {
    Group::new(group_id, properties).into_operator()
}

impl Operator {
    pub fn logical_join(
        self: Arc<Self>,
        inner: Arc<Self>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Self> {
        LogicalJoin::new(join_type, self, inner, join_cond).into_operator()
    }

    pub fn nl_join(
        self: Arc<Self>,
        inner: Arc<Self>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Self> {
        PhysicalNLJoin::new(join_type, self, inner, join_cond).into_operator()
    }

    pub fn logical_select(self: Arc<Self>, predicate: Arc<Scalar>) -> Arc<Self> {
        LogicalSelect::new(self, predicate).into_operator()
    }

    pub fn physical_filter(self: Arc<Self>, predicate: Arc<Scalar>) -> Arc<Self> {
        PhysicalFilter::new(self, predicate).into_operator()
    }
}

/// Creates a raw column reference.
pub fn column_ref(column: Column) -> Arc<Scalar> {
    ColumnRef::new(column).into_scalar()
}

/// Creates a literal of type boolean.
pub fn boolean(v: impl Into<Option<bool>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Boolean(v.into())).into_scalar()
}

/// Creates a literal of type int32.
pub fn int32(v: impl Into<Option<i32>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Int32(v.into())).into_scalar()
}

impl Scalar {
    fn binary_op(self: Arc<Self>, rhs: Arc<Self>, op_kind: BinaryOpKind) -> Arc<Self> {
        BinaryOp::new(op_kind, self, rhs).into_scalar()
    }

    pub fn plus(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Plus)
    }

    pub fn equal(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Equal)
    }
}
