//! The builder module provides helper functions to construct IR nodes such as
//! operators and scalar expressions in a more ergonomic way.

use arrow_schema::Field;
use itertools::Itertools;

use crate::ir::{
    Column, DataType, Group, GroupId, IRContext, Operator, Scalar, ScalarValue,
    binder::Binding,
    builder_v2::{column_ref, list},
    catalog::{DataSourceId, Schema},
    convert::{IntoOperator, IntoScalar},
    operator::{
        Aggregate, AggregateImplementation, DependentJoin, Get, Join, JoinImplementation, JoinSide,
        Project, Remap, Select, join::JoinType,
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
        Join::new(join_type, self, inner, join_cond, None).into_operator()
    }

    /// Creates a dependent (correlated) join.
    pub fn logical_dependent_join(
        self: Arc<Self>,
        inner: Arc<Self>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Self> {
        DependentJoin::new(join_type, self, inner, join_cond).into_operator()
    }

    pub fn nl_join(
        self: Arc<Self>,
        inner: Arc<Self>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Self> {
        Join::new(
            join_type,
            self,
            inner,
            join_cond,
            Some(JoinImplementation::nested_loop()),
        )
        .into_operator()
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

        Join::new(
            join_type,
            self,
            probe_side,
            join_cond,
            Some(JoinImplementation::hash(JoinSide::Outer, keys)),
        )
        .into_operator()
    }

    pub fn select(self: Arc<Self>, predicate: Arc<Scalar>) -> Arc<Self> {
        Select::new(self, predicate).into_operator()
    }

    pub fn project(
        self: Arc<Self>,
        table_index: i64,
        projections: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Arc<Self> {
        Project::new(table_index, self, list(projections)).into_operator()
    }

    pub fn remap(self: Arc<Self>, table_index: i64) -> Arc<Self> {
        Remap::new(table_index, self).into_operator()
    }

    pub fn logical_aggregate(
        self: Arc<Self>,
        aggregate_table_index: i64,
        exprs: impl IntoIterator<Item = Arc<Scalar>>,
        keys: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Arc<Self> {
        Aggregate::new(aggregate_table_index, self, list(exprs), list(keys), None).into_operator()
    }

    pub fn hash_aggregate(
        self: Arc<Self>,
        aggregate_table_index: i64,
        exprs: impl IntoIterator<Item = Arc<Scalar>>,
        keys: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Arc<Self> {
        Aggregate::new(
            aggregate_table_index,
            self,
            list(exprs),
            list(keys),
            Some(AggregateImplementation::Hash),
        )
        .into_operator()
    }
}
