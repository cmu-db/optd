use std::sync::Arc;

use itertools::Itertools;

use crate::ir::{
    Column, DataType, Group, GroupId, IRContext, Operator, Scalar, ScalarValue,
    catalog::{DataSourceId, Schema},
    convert::{IntoOperator, IntoScalar},
    operator::{
        LogicalAggregate, LogicalGet, LogicalJoin, LogicalProject, LogicalRemap, LogicalSelect,
        PhysicalFilter, PhysicalHashAggregate, PhysicalHashJoin, PhysicalNLJoin, PhysicalProject,
        PhysicalTableScan, join::JoinType,
    },
    properties::OperatorProperties,
    scalar::*,
};

pub fn group(group_id: GroupId, properties: Arc<OperatorProperties>) -> Arc<Operator> {
    Group::new(group_id, properties).into_operator()
}

impl IRContext {
    /// Creates a mock scan operator.
    pub fn mock_scan(&self, id: usize, columns: Vec<usize>, card: f64) -> Arc<Operator> {
        use crate::ir::operator::{MockScan, MockSpec};
        let mut column_meta = self.column_meta.lock().unwrap();
        let columns = columns
            .iter()
            .map(|i| {
                column_meta.new_column(
                    DataType::Int32,
                    Some(format!("col{}", i)),
                    Some(DataSourceId(id as i64)),
                )
            })
            .collect_vec();

        let spec = MockSpec::new_test_only(columns, card);
        MockScan::with_mock_spec(id, spec).into_operator()
    }
    pub fn logical_get(
        &self,
        source: DataSourceId,
        schema: &Schema,
        projections: Option<Arc<[usize]>>,
    ) -> Arc<Operator> {
        let first_column = self.add_base_table_columns(source, schema);

        let projections = projections.unwrap_or_else(|| (0..schema.columns().len()).collect());
        LogicalGet::new(source, first_column, projections).into_operator()
    }

    pub fn table_scan(
        &self,
        source: DataSourceId,
        schema: &Schema,
        projections: Option<Arc<[usize]>>,
    ) -> Arc<Operator> {
        let first_column = self.add_base_table_columns(source, schema);

        let projections = projections.unwrap_or_else(|| (0..schema.columns().len()).collect());
        PhysicalTableScan::new(source, first_column, projections).into_operator()
    }
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

    pub fn hash_join(
        self: Arc<Self>,
        probe_side: Arc<Self>,
        keys: Arc<[(Column, Column)]>,
        non_equi_conds: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Self> {
        PhysicalHashJoin::new(join_type, self, probe_side, keys, non_equi_conds).into_operator()
    }

    pub fn logical_select(self: Arc<Self>, predicate: Arc<Scalar>) -> Arc<Self> {
        LogicalSelect::new(self, predicate).into_operator()
    }

    pub fn physical_filter(self: Arc<Self>, predicate: Arc<Scalar>) -> Arc<Self> {
        PhysicalFilter::new(self, predicate).into_operator()
    }

    pub fn logical_project(
        self: Arc<Self>,
        projections: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Arc<Self> {
        LogicalProject::new(self, list(projections)).into_operator()
    }

    pub fn physical_project(
        self: Arc<Self>,
        projections: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Arc<Self> {
        PhysicalProject::new(self, list(projections)).into_operator()
    }

    pub fn logical_remap(
        self: Arc<Self>,
        mappings: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Arc<Self> {
        LogicalRemap::new(self, list(mappings)).into_operator()
    }

    pub fn logical_aggregate(
        self: Arc<Self>,
        exprs: impl IntoIterator<Item = Arc<Scalar>>,
        keys: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Arc<Self> {
        LogicalAggregate::new(self, list(exprs), list(keys)).into_operator()
    }

    pub fn hash_aggregate(
        self: Arc<Self>,
        exprs: impl IntoIterator<Item = Arc<Scalar>>,
        keys: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Arc<Self> {
        PhysicalHashAggregate::new(self, list(exprs), list(keys)).into_operator()
    }
}

/// Creates a raw column reference.
pub fn column_ref(column: Column) -> Arc<Scalar> {
    ColumnRef::new(column).into_scalar()
}

/// Creates a new assignment of some column to an scalar expression.
pub fn column_assign(column: Column, expr: Arc<Scalar>) -> Arc<Scalar> {
    ColumnAssign::new(column, expr).into_scalar()
}

/// Creates a literal of type boolean.
pub fn boolean(v: impl Into<Option<bool>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Boolean(v.into())).into_scalar()
}

pub fn utf8<'a>(v: impl Into<Option<&'a str>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Utf8(v.into().map(|x| x.to_string()))).into_scalar()
}

pub fn utf8_view<'a>(v: impl Into<Option<&'a str>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Utf8View(v.into().map(|x| x.to_string()))).into_scalar()
}

pub fn decimal128(v: impl Into<Option<i128>>, precision: u8, scale: i8) -> Arc<Scalar> {
    Literal::new(ScalarValue::Decimal128(v.into(), precision, scale)).into_scalar()
}

/// Creates a literal of type integer (i32).
pub fn integer(v: impl Into<Option<i32>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Int32(v.into())).into_scalar()
}

pub fn bigint(v: impl Into<Option<i64>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Int64(v.into())).into_scalar()
}

pub fn cast(expr: Arc<Scalar>, data_type: DataType) -> Arc<Scalar> {
    Cast::new(data_type, expr).into_scalar()
}

pub fn like(
    expr: Arc<Scalar>,
    pattern: Arc<Scalar>,
    negated: bool,
    case_insensative: bool,
    escape_char: Option<char>,
) -> Arc<Scalar> {
    Like::new(expr, pattern, negated, case_insensative, escape_char).into_scalar()
}

pub fn list(members: impl IntoIterator<Item = Arc<Scalar>>) -> Arc<Scalar> {
    List::new(members.into_iter().collect()).into_scalar()
}

impl Scalar {
    pub fn binary_op(self: Arc<Self>, rhs: Arc<Self>, op_kind: BinaryOpKind) -> Arc<Self> {
        BinaryOp::new(op_kind, self, rhs).into_scalar()
    }

    pub fn plus(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Plus)
    }

    pub fn eq(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Eq)
    }

    pub fn lt(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Lt)
    }

    pub fn le(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Le)
    }

    pub fn gt(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Gt)
    }

    pub fn ge(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Ge)
    }

    pub fn nary_op(self: Arc<Self>, rhs: Arc<Self>, op_kind: NaryOpKind) -> Arc<Self> {
        if let Ok(nary_op) = self.try_borrow::<NaryOp>()
            && nary_op.op_kind() == &op_kind
        {
            let terms = nary_op
                .terms()
                .iter()
                .cloned()
                .chain(std::iter::once(rhs))
                .collect();
            NaryOp::new(op_kind, terms).into_scalar()
        } else {
            NaryOp::new(op_kind, Arc::new([self, rhs])).into_scalar()
        }
    }

    pub fn and(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.nary_op(rhs, NaryOpKind::And)
    }

    pub fn or(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.nary_op(rhs, NaryOpKind::Or)
    }
}
