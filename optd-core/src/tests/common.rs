// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;

use itertools::Itertools;

use crate::{
    cascades::GroupId,
    logical_property::{LogicalProperty, LogicalPropertyBuilder},
    nodes::{ArcPlanNode, ArcPredNode, NodeType, PlanNode, PlanNodeOrGroup, PredNode, Value},
    physical_property::{PhysicalProperty, PhysicalPropertyBuilder},
};

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum MemoTestRelTyp {
    Join,
    Project,
    Scan,
    Sort,
    Filter,
    Agg,
    PhysicalNestedLoopJoin,
    PhysicalProject,
    PhysicalFilter,
    PhysicalScan,
    PhysicalSort,
    PhysicalPartition,
    PhysicalStreamingAgg,
    PhysicalHashAgg,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum MemoTestPredTyp {
    List,
    Expr,
    TableName,
    ColumnRef,
}

impl std::fmt::Display for MemoTestRelTyp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::fmt::Display for MemoTestPredTyp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl NodeType for MemoTestRelTyp {
    type PredType = MemoTestPredTyp;

    fn is_logical(&self) -> bool {
        matches!(
            self,
            Self::Project | Self::Scan | Self::Join | Self::Sort | Self::Filter
        )
    }
}

pub(crate) fn join(
    left: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    right: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    cond: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::Join,
        children: vec![left.into(), right.into()],
        predicates: vec![cond],
    })
}

#[allow(dead_code)]
pub(crate) fn agg(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    group_bys: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::Agg,
        children: vec![input.into()],
        predicates: vec![group_bys],
    })
}

pub(crate) fn scan(table: &str) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::Scan,
        children: vec![],
        predicates: vec![table_name(table)],
    })
}

pub(crate) fn table_name(table: &str) -> ArcPredNode<MemoTestRelTyp> {
    Arc::new(PredNode {
        typ: MemoTestPredTyp::TableName,
        children: vec![],
        data: Some(Value::String(table.to_string().into())),
    })
}

pub(crate) fn project(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    expr_list: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::Project,
        children: vec![input.into()],
        predicates: vec![expr_list],
    })
}

pub(crate) fn physical_nested_loop_join(
    left: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    right: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    cond: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalNestedLoopJoin,
        children: vec![left.into(), right.into()],
        predicates: vec![cond],
    })
}

#[allow(dead_code)]
pub(crate) fn physical_project(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    expr_list: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalProject,
        children: vec![input.into()],
        predicates: vec![expr_list],
    })
}

pub(crate) fn physical_filter(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    cond: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalFilter,
        children: vec![input.into()],
        predicates: vec![cond],
    })
}

pub(crate) fn physical_scan(table: &str) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalScan,
        children: vec![],
        predicates: vec![table_name(table)],
    })
}

pub(crate) fn physical_sort(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    sort_expr: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalSort,
        children: vec![input.into()],
        predicates: vec![sort_expr],
    })
}

#[allow(dead_code)]
pub(crate) fn physical_partition(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    partition_expr: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalPartition,
        children: vec![input.into()],
        predicates: vec![partition_expr],
    })
}

pub(crate) fn physical_streaming_agg(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    group_bys: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalStreamingAgg,
        children: vec![input.into()],
        predicates: vec![group_bys],
    })
}

pub(crate) fn physical_hash_agg(
    input: impl Into<PlanNodeOrGroup<MemoTestRelTyp>>,
    group_bys: ArcPredNode<MemoTestRelTyp>,
) -> ArcPlanNode<MemoTestRelTyp> {
    Arc::new(PlanNode {
        typ: MemoTestRelTyp::PhysicalHashAgg,
        children: vec![input.into()],
        predicates: vec![group_bys],
    })
}

pub(crate) fn list(items: Vec<ArcPredNode<MemoTestRelTyp>>) -> ArcPredNode<MemoTestRelTyp> {
    Arc::new(PredNode {
        typ: MemoTestPredTyp::List,
        children: items,
        data: None,
    })
}

pub(crate) fn expr(data: Value) -> ArcPredNode<MemoTestRelTyp> {
    Arc::new(PredNode {
        typ: MemoTestPredTyp::Expr,
        children: vec![],
        data: Some(data),
    })
}

pub(crate) fn column_ref(col: &str) -> ArcPredNode<MemoTestRelTyp> {
    Arc::new(PredNode {
        typ: MemoTestPredTyp::ColumnRef,
        children: vec![],
        data: Some(Value::String(col.to_string().into())),
    })
}

pub(crate) fn group(group_id: GroupId) -> PlanNodeOrGroup<MemoTestRelTyp> {
    PlanNodeOrGroup::Group(group_id)
}

pub struct TestPropertyBuilder;

#[derive(Clone, Debug)]
pub struct TestProp(pub Vec<String>);

impl std::fmt::Display for TestProp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
impl LogicalProperty for TestProp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
impl LogicalPropertyBuilder<MemoTestRelTyp> for TestPropertyBuilder {
    type Prop = TestProp;
    fn derive(
        &self,
        typ: MemoTestRelTyp,
        pred: &[ArcPredNode<MemoTestRelTyp>],
        children: &[&Self::Prop],
    ) -> Self::Prop {
        match typ {
            MemoTestRelTyp::Join => {
                let mut a = children[0].0.clone();
                let b = children[1].0.clone();
                a.extend(b);
                TestProp(a)
            }
            MemoTestRelTyp::Project => {
                let preds = &pred[0].children;
                TestProp(
                    preds
                        .iter()
                        .map(|x| x.data.as_ref().unwrap().as_i64().to_string())
                        .collect(),
                )
            }
            MemoTestRelTyp::Scan => TestProp(vec!["scan_col".to_string()]),
            _ => unreachable!("the memo table tests don't use other plan nodes"),
        }
    }
    fn property_name(&self) -> &'static str {
        "test"
    }
}

pub struct SortPropertyBuilder;

#[derive(Clone, Debug)]
pub struct SortProp(pub Vec<String>);

impl std::fmt::Display for SortProp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
impl PhysicalProperty for SortProp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn to_boxed(&self) -> Box<dyn PhysicalProperty> {
        Box::new(self.clone())
    }
}

impl PhysicalPropertyBuilder<MemoTestRelTyp> for SortPropertyBuilder {
    type Prop = SortProp;

    fn derive(
        &self,
        typ: MemoTestRelTyp,
        predicates: &[ArcPredNode<MemoTestRelTyp>],
        children: &[&Self::Prop],
    ) -> Self::Prop {
        match typ {
            // the node doesn't have any sort properties
            MemoTestRelTyp::PhysicalHashAgg => SortProp(vec![]),
            MemoTestRelTyp::PhysicalScan => SortProp(vec![]),
            // passthrough child sort properties
            MemoTestRelTyp::PhysicalPartition => children[0].clone(),
            MemoTestRelTyp::PhysicalNestedLoopJoin => children[0].clone(),
            MemoTestRelTyp::PhysicalProject => children[0].clone(),
            MemoTestRelTyp::PhysicalFilter => children[0].clone(),
            MemoTestRelTyp::PhysicalStreamingAgg => children[0].clone(),
            // assume sort node doesn't ensure stable sort, the derived sort property is simply the predicates
            MemoTestRelTyp::PhysicalSort => {
                let columns = predicates[0]
                    .children
                    .iter()
                    .map(|x| {
                        assert_eq!(x.typ, MemoTestPredTyp::ColumnRef);
                        x.unwrap_data().as_str().to_string()
                    })
                    .collect_vec();
                // columns.extend(children[0].0.iter().cloned()); // if the sort is stable, we can derive from child
                SortProp(columns)
            }
            _ => panic!("unsupported type"),
        }
    }

    fn passthrough(
        &self,
        typ: MemoTestRelTyp,
        predicates: &[ArcPredNode<MemoTestRelTyp>],
        required: &Self::Prop,
    ) -> Vec<Self::Prop> {
        match typ {
            // cannot passthrough
            MemoTestRelTyp::PhysicalHashAgg => vec![SortProp(vec![])],
            MemoTestRelTyp::PhysicalScan => vec![],
            MemoTestRelTyp::PhysicalSort => vec![SortProp(vec![])],
            // passthrough the required property to the left / only child
            MemoTestRelTyp::PhysicalPartition => vec![required.clone()],
            MemoTestRelTyp::PhysicalNestedLoopJoin => vec![required.clone(), SortProp(vec![])],
            MemoTestRelTyp::PhysicalProject => vec![required.clone()],
            MemoTestRelTyp::PhysicalFilter => vec![required.clone()],
            // do not passthrough, just require the sort property
            MemoTestRelTyp::PhysicalStreamingAgg => {
                let columns = predicates[0]
                    .children
                    .iter()
                    .map(|x| {
                        assert_eq!(x.typ, MemoTestPredTyp::ColumnRef);
                        x.unwrap_data().as_str().to_string()
                    })
                    .collect_vec();
                vec![SortProp(columns)]
            }
            _ => panic!("unsupported type"),
        }
    }

    fn satisfies(&self, prop: &SortProp, required: &SortProp) -> bool {
        // required should be a prefix of the current property
        for i in 0..required.0.len() {
            if i >= prop.0.len() || prop.0[i] != required.0[i] {
                return false;
            }
        }
        true
    }

    fn default(&self) -> Self::Prop {
        SortProp(vec![])
    }

    fn enforce(&self, prop: &Self::Prop) -> (MemoTestRelTyp, Vec<ArcPredNode<MemoTestRelTyp>>) {
        let mut predicates = Vec::new();
        for column in &prop.0 {
            predicates.push(column_ref(column));
        }
        (MemoTestRelTyp::PhysicalSort, vec![list(predicates)])
    }

    fn property_name(&self) -> &'static str {
        "sort"
    }
}
