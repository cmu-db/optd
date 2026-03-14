//! The builder module provides helper functions to construct IR nodes such as
//! operators and scalar expressions in a more ergonomic way.

use arrow_schema::Field;
use itertools::Itertools;

use crate::ir::{
    Column, DataType, Group, GroupId, IRContext, Operator, Scalar, ScalarValue,
    binder::Binding,
    catalog::{DataSourceId, Schema},
    convert::{IntoOperator, IntoScalar},
    operator::{
        Aggregate, Get, Join, JoinSide, LogicalDependentJoin, LogicalRemap, Project, Select,
        join::JoinType,
    },
    properties::OperatorProperties,
    scalar::*,
    table_ref::TableRef,
};
use std::sync::Arc;

pub fn group(group_id: GroupId, properties: Arc<OperatorProperties>) -> Arc<Operator> {
    Group::new(group_id, properties).into_operator()
}

impl IRContext {
    /// Creates a mock scan operator.
    pub fn mock_scan(&self, table_index: i64, num_columns: usize, card: f64) -> Arc<Operator> {
        use crate::ir::operator::{MockScan, MockSpec};

        let binding = Binding::new(
            TableRef::bare(format!("mock#{table_index}")),
            Arc::new(Schema::new(
                (0..num_columns)
                    .map(|i| Arc::new(Field::new(format!("col{i}"), DataType::Int32, true)))
                    .collect_vec(),
            )),
            table_index,
        );
        let mut guard = self.binder.write().unwrap();

        guard.bindings.insert(table_index, binding);
        let spec = MockSpec::new_test_only(table_index, num_columns, card);
        MockScan::with_mock_spec(table_index, spec).into_operator()
    }

    pub fn logical_get(
        &self,
        table_index: i64,
        schema: &Schema,
        projections: Option<Arc<[usize]>>,
    ) -> Arc<Operator> {
        let projections = projections.unwrap_or_else(|| (0..schema.fields().len()).collect());
        Get::logical(DataSourceId(table_index), table_index, projections).into_operator()
    }

    pub fn table_scan(
        &self,
        table_index: i64,
        schema: &Schema,
        projections: Option<Arc<[usize]>>,
    ) -> Arc<Operator> {
        let projections = projections.unwrap_or_else(|| (0..schema.fields().len()).collect());
        Get::table_scan(DataSourceId(table_index), table_index, projections).into_operator()
    }
}

impl Operator {
    pub fn logical_join(
        self: Arc<Self>,
        inner: Arc<Self>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Self> {
        Join::logical(join_type, self, inner, join_cond).into_operator()
    }

    /// Creates a dependent (correlated) join.
    pub fn logical_dependent_join(
        self: Arc<Self>,
        inner: Arc<Self>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Self> {
        LogicalDependentJoin::new(join_type, self, inner, join_cond).into_operator()
    }

    pub fn nl_join(
        self: Arc<Self>,
        inner: Arc<Self>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Self> {
        Join::nested_loop(join_type, self, inner, join_cond).into_operator()
    }

    pub fn hash_join(
        self: Arc<Self>,
        probe_side: Arc<Self>,
        keys: Arc<[(Column, Column)]>,
        non_equi_conds: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Self> {
        let join_cond = NaryOp::new(
            NaryOpKind::And,
            keys.iter()
                .map(|(l, r)| column_ref(*l).eq(column_ref(*r)))
                .chain(std::iter::once(non_equi_conds))
                .collect(),
        )
        .into_scalar();

        Join::hash(
            join_type,
            self,
            probe_side,
            join_cond,
            JoinSide::Outer,
            keys,
        )
        .into_operator()
    }

    pub fn logical_select(self: Arc<Self>, predicate: Arc<Scalar>) -> Arc<Self> {
        Select::new(self, predicate).into_operator()
    }

    pub fn physical_filter(self: Arc<Self>, predicate: Arc<Scalar>) -> Arc<Self> {
        Select::new(self, predicate).into_operator()
    }

    pub fn logical_project(
        self: Arc<Self>,
        table_index: i64,
        projections: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Arc<Self> {
        Project::new(table_index, self, list(projections)).into_operator()
    }

    pub fn physical_project(
        self: Arc<Self>,
        table_index: i64,
        projections: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Arc<Self> {
        Project::new(table_index, self, list(projections)).into_operator()
    }

    pub fn logical_remap(self: Arc<Self>, table_index: i64) -> Arc<Self> {
        LogicalRemap::new(table_index, self).into_operator()
    }

    pub fn logical_aggregate(
        self: Arc<Self>,
        aggregate_table_index: i64,
        exprs: impl IntoIterator<Item = Arc<Scalar>>,
        keys: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Arc<Self> {
        Aggregate::logical(aggregate_table_index, self, list(exprs), list(keys)).into_operator()
    }

    pub fn hash_aggregate(
        self: Arc<Self>,
        aggregate_table_index: i64,
        exprs: impl IntoIterator<Item = Arc<Scalar>>,
        keys: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Arc<Self> {
        Aggregate::hash(aggregate_table_index, self, list(exprs), list(keys)).into_operator()
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

pub fn utf8<'a>(v: impl Into<Option<&'a str>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Utf8(v.into().map(|x| x.to_string()))).into_scalar()
}

pub fn utf8_view<'a>(v: impl Into<Option<&'a str>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Utf8View(v.into().map(|x| x.to_string()))).into_scalar()
}

pub fn literal<T: Into<ScalarValue>>(v: T) -> Arc<Scalar> {
    Literal::new(v.into()).into_scalar()
}

/// Creates a literal of type integer (i32).
pub fn int32(v: impl Into<Option<i32>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Int32(v.into())).into_scalar()
}

pub fn int64(v: impl Into<Option<i64>>) -> Arc<Scalar> {
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
