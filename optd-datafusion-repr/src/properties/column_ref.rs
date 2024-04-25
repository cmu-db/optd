use std::collections::HashSet;
use std::ops::Index;
use std::{ops::Deref, sync::Arc};

use optd_core::property::PropertyBuilder;
use union_find::disjoint_sets::DisjointSets;
use union_find::union_find::UnionFind;

use crate::plan_nodes::{BinOpType, EmptyRelationData, JoinType, LogOpType, OptRelNodeTyp};

use super::schema::Catalog;
use super::DEFAULT_NAME;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BaseTableColumnRef {
    pub table: String,
    pub col_idx: usize,
}

#[derive(Clone, Debug)]
pub enum ColumnRef {
    BaseTableColumnRef(BaseTableColumnRef),
    /// This variant is only used when building the property. It should NEVER
    /// be used when computing cost.
    ChildColumnRef {
        col_idx: usize,
    },
    Derived,
}

impl ColumnRef {
    pub fn base_table_column_ref(table: String, col_idx: usize) -> Self {
        ColumnRef::BaseTableColumnRef(BaseTableColumnRef { table, col_idx })
    }

    pub fn child_column_ref(col_idx: usize) -> Self {
        ColumnRef::ChildColumnRef { col_idx }
    }
}

/// `SemanticCorrelation` represents the semantic correlation between columns in a
/// query. "Semantic" means that the columns are correlated based on the
/// semantics of the query, not the statistics.
#[derive(Clone, Debug)]
pub struct SemanticCorrelation {
    eq_column: EqColumns,
}

#[derive(Clone, Debug)]
pub enum EqColumns {
    /// Equal columns denoted by sets of base table columns,
    /// e.g. t1.c1 = t2.c2 = t3.c3.
    EqBaseTableColumnSets(EqBaseTableColumnSets),
    /// Equal columns denoted by pairs of column indices. This is for keeping
    /// track of the column indices in the filter/join predicates, which only
    /// contains relative column indices.
    ///
    /// It is only used when building the property. It should NEVER be used when
    /// computing cost.
    EqColumnIdxPairs(Vec<(usize, usize)>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EqPredicate {
    left: BaseTableColumnRef,
    right: BaseTableColumnRef,
}

impl EqPredicate {
    pub fn new(left: BaseTableColumnRef, right: BaseTableColumnRef) -> Self {
        Self { left, right }
    }

    pub fn left(&self) -> &BaseTableColumnRef {
        &self.left
    }

    pub fn right(&self) -> &BaseTableColumnRef {
        &self.right
    }
}

/// A disjoint set of base table columns with equal values in the same row,
/// along with the predicates that define the equality.
#[derive(Clone, Debug)]
pub struct EqBaseTableColumnSets {
    disjoint_eq_col_sets: DisjointSets<BaseTableColumnRef>,
    eq_predicates: HashSet<EqPredicate>,
}

impl EqBaseTableColumnSets {
    fn new() -> Self {
        Self {
            disjoint_eq_col_sets: DisjointSets::new(),
            eq_predicates: HashSet::new(),
        }
    }

    pub fn add_predicate(&mut self, predicate: EqPredicate) {
        let left = predicate.left();
        let right = predicate.right();

        // Add the indices to the set if they do not exist.
        if !self.disjoint_eq_col_sets.contains(left) {
            self.disjoint_eq_col_sets
                .make_set(left.clone())
                .expect("just checked left column index does not exist");
        }
        if !self.disjoint_eq_col_sets.contains(right) {
            self.disjoint_eq_col_sets
                .make_set(right.clone())
                .expect("just checked right column index does not exist");
        }
        // Union the columns.
        self.disjoint_eq_col_sets
            .union(left, right)
            .expect("both column indices should exist");

        // Keep track of the predicate.
        self.eq_predicates.insert(predicate);
    }

    pub fn union(x: &EqBaseTableColumnSets, y: &EqBaseTableColumnSets) -> EqBaseTableColumnSets {
        let mut eq_col_sets = Self::new();
        // TODO: one redundant clone here.
        for predicate in x.eq_predicates.union(&y.eq_predicates).cloned() {
            eq_col_sets.add_predicate(predicate);
        }
        eq_col_sets
    }
}

#[derive(Clone, Debug)]
pub struct GroupColumnRefs {
    pub column_refs: Vec<ColumnRef>,
    pub correlation: Option<SemanticCorrelation>,
}

impl GroupColumnRefs {
    pub fn new(column_refs: Vec<ColumnRef>, correlation: Option<SemanticCorrelation>) -> Self {
        Self {
            column_refs,
            correlation,
        }
    }
}

impl Index<usize> for GroupColumnRefs {
    type Output = ColumnRef;

    fn index(&self, index: usize) -> &Self::Output {
        &self.column_refs[index]
    }
}

pub struct ColumnRefPropertyBuilder {
    catalog: Arc<dyn Catalog>,
}

impl ColumnRefPropertyBuilder {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    fn concat_children_col_refs(children: &[&GroupColumnRefs]) -> Vec<ColumnRef> {
        children
            .iter()
            .map(|c| &c.column_refs)
            .flat_map(Deref::deref)
            .cloned()
            .collect()
    }
}

impl PropertyBuilder<OptRelNodeTyp> for ColumnRefPropertyBuilder {
    type Prop = GroupColumnRefs;

    fn derive(
        &self,
        typ: OptRelNodeTyp,
        data: Option<optd_core::rel_node::Value>,
        children: &[&Self::Prop],
    ) -> Self::Prop {
        match typ {
            // Should account for PhysicalScan.
            OptRelNodeTyp::Scan => {
                let table_name = data.unwrap().as_str().to_string();
                let schema = self.catalog.get(&table_name);
                let column_cnt = schema.fields.len();
                let column_refs = (0..column_cnt)
                    .map(|i| ColumnRef::base_table_column_ref(table_name.clone(), i))
                    .collect();
                GroupColumnRefs::new(column_refs, None)
            }
            OptRelNodeTyp::EmptyRelation => {
                let data = data.unwrap().as_slice();
                let empty_relation_data: EmptyRelationData =
                    bincode::deserialize(data.as_ref()).unwrap();
                let schema = empty_relation_data.schema;
                let column_cnt = schema.fields.len();
                let column_refs = (0..column_cnt)
                    .map(|i| ColumnRef::base_table_column_ref(DEFAULT_NAME.to_string(), i))
                    .collect();
                GroupColumnRefs::new(column_refs, None)
            }
            OptRelNodeTyp::ColumnRef => {
                let col_ref_idx = data.unwrap().as_u64();
                // this is always safe since col_ref_idx was initially a usize in ColumnRefExpr::new()
                let usize_col_ref_idx = col_ref_idx as usize;
                let column_refs = vec![ColumnRef::ChildColumnRef {
                    col_idx: usize_col_ref_idx,
                }];
                GroupColumnRefs::new(column_refs, None)
            }
            OptRelNodeTyp::List => {
                // Concatentate the children column refs.
                let column_refs = Self::concat_children_col_refs(children);
                GroupColumnRefs::new(column_refs, None)
            }
            OptRelNodeTyp::LogOp(op_type) => {
                let column_refs = vec![ColumnRef::Derived];
                // For AND, combine the eq columns of each child expression.
                let correlation = {
                    match op_type {
                        LogOpType::And => {
                            let mut eq_column_idx_pairs = Vec::new();
                            for child in children {
                                if let Some(SemanticCorrelation {
                                    eq_column: EqColumns::EqColumnIdxPairs(pairs),
                                }) = &child.correlation
                                {
                                    eq_column_idx_pairs.extend(pairs.iter());
                                }
                            }
                            Some(SemanticCorrelation {
                                eq_column: EqColumns::EqColumnIdxPairs(eq_column_idx_pairs),
                            })
                        }
                        _ => None,
                    }
                };
                GroupColumnRefs::new(column_refs, correlation)
            }
            OptRelNodeTyp::Projection => {
                let child = children[0];
                let exprs = children[1];
                let column_refs = exprs
                    .column_refs
                    .iter()
                    .map(|p| match p {
                        ColumnRef::ChildColumnRef { col_idx } => {
                            children[0].column_refs[*col_idx].clone()
                        }
                        ColumnRef::Derived => ColumnRef::Derived,
                        _ => panic!("projection expr must be Derived or ChildColumnRef"),
                    })
                    .collect();
                // Projection keeps the semantic correlations of the children.
                GroupColumnRefs::new(column_refs, child.correlation.clone())
            }
            // Should account for all physical join types.
            OptRelNodeTyp::Join(join_type) => {
                // Concatenate left and right children column refs.
                let column_refs = Self::concat_children_col_refs(&children[0..2]);
                let correlation = match join_type {
                    JoinType::Inner | JoinType::Cross => {
                        // Merge the equal columns in the join condition into the those from the children.
                        // Step 1: merge the equal columns of two children.
                        let mut eq_columns = {
                            let left = children[0].correlation.as_ref();
                            let right = children[1].correlation.as_ref();
                            match (left, right) {
                                (
                                    Some(SemanticCorrelation {
                                        eq_column: EqColumns::EqBaseTableColumnSets(left),
                                    }),
                                    Some(SemanticCorrelation {
                                        eq_column: EqColumns::EqBaseTableColumnSets(right),
                                    }),
                                ) => EqBaseTableColumnSets::union(left, right),
                                (
                                    Some(SemanticCorrelation {
                                        eq_column: EqColumns::EqBaseTableColumnSets(left),
                                    }),
                                    None,
                                ) => left.clone(),
                                (
                                    None,
                                    Some(SemanticCorrelation {
                                        eq_column: EqColumns::EqBaseTableColumnSets(right),
                                    }),
                                ) => right.clone(),
                                _ => EqBaseTableColumnSets::new(),
                            }
                        };
                        // Step 2: merge join condition into children's equal columns.
                        if let Some(SemanticCorrelation {
                            eq_column: EqColumns::EqColumnIdxPairs(pairs),
                        }) = &children[2].correlation
                        {
                            for (l_col_idx, r_col_idx) in pairs {
                                let l_col_ref = &column_refs[*l_col_idx];
                                let r_col_ref = &column_refs[*r_col_idx];
                                if let (
                                    ColumnRef::BaseTableColumnRef(l),
                                    ColumnRef::BaseTableColumnRef(r),
                                ) = (l_col_ref, r_col_ref)
                                {
                                    eq_columns
                                        .add_predicate(EqPredicate::new(l.clone(), r.clone()));
                                }
                            }
                        };
                        Some(SemanticCorrelation {
                            eq_column: EqColumns::EqBaseTableColumnSets(eq_columns),
                        })
                    }
                    _ => None,
                };
                // println!(
                //     "left: {:#?}, right: {:#?}, join condition: {:#?}, new correlation: {:#?}",
                //     children[0], children[1], children[2], correlation
                // );
                GroupColumnRefs::new(column_refs, correlation)
            }
            OptRelNodeTyp::Agg => {
                // Group by columns first.
                let mut group_by_col_refs: Vec<_> = children[2]
                    .column_refs
                    .iter()
                    .map(|p| {
                        let col_idx = match p {
                            ColumnRef::ChildColumnRef { col_idx } => *col_idx,
                            _ => panic!("group by expr must be ColumnRef"),
                        };
                        children[0].column_refs[col_idx].clone()
                    })
                    .collect();
                // Then the aggregate expressions. These columns, (e.g. SUM, COUNT, etc.) are derived columns.
                let agg_expr_cnt = children[1].column_refs.len();
                group_by_col_refs.extend((0..agg_expr_cnt).map(|_| ColumnRef::Derived));
                // Aggregation clears all semantic correlations.
                GroupColumnRefs::new(group_by_col_refs, None)
            }
            OptRelNodeTyp::Filter
            | OptRelNodeTyp::Sort
            | OptRelNodeTyp::Limit
            | OptRelNodeTyp::SortOrder(_) => children[0].clone(),
            OptRelNodeTyp::Cast => {
                // FIXME: we just assume the column value does not change.
                children[0].clone()
            }
            OptRelNodeTyp::BinOp(op_type) => {
                let column_refs = vec![ColumnRef::Derived];
                // For correlation, we only handle the column = column case, e.g. #0 = #1.
                let correlation = match op_type {
                    BinOpType::Eq => {
                        let l_col_ref = &children[0].column_refs;
                        let r_col_ref = &children[1].column_refs;
                        if l_col_ref.len() != 1 || r_col_ref.len() != 1 {
                            None
                        } else {
                            match (&l_col_ref[0], &r_col_ref[0]) {
                                (
                                    ColumnRef::ChildColumnRef { col_idx: l_col_idx },
                                    ColumnRef::ChildColumnRef { col_idx: r_col_idx },
                                ) => Some(SemanticCorrelation {
                                    eq_column: EqColumns::EqColumnIdxPairs(vec![(
                                        *l_col_idx, *r_col_idx,
                                    )]),
                                }),
                                _ => None,
                            }
                        }
                    }
                    _ => None,
                };
                GroupColumnRefs::new(column_refs, correlation)
            }
            OptRelNodeTyp::Constant(_)
            | OptRelNodeTyp::Func(_)
            | OptRelNodeTyp::DataType(_)
            | OptRelNodeTyp::Between
            | OptRelNodeTyp::Like
            | OptRelNodeTyp::InList => GroupColumnRefs::new(vec![ColumnRef::Derived], None),
            _ => unimplemented!("Unsupported rel node type {:?}", typ),
        }
    }

    fn property_name(&self) -> &'static str {
        "column_ref"
    }
}
