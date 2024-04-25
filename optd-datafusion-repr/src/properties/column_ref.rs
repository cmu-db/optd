use std::collections::HashSet;
use std::ops::Index;
use std::{ops::Deref, sync::Arc};

use optd_core::property::PropertyBuilder;
use union_find::disjoint_sets::DisjointSets;
use union_find::union_find::UnionFind;

use crate::plan_nodes::{EmptyRelationData, OptRelNodeTyp};

use super::schema::Catalog;
use super::DEFAULT_NAME;

#[derive(Clone, Debug)]
pub enum ColumnRef {
    BaseTableColumnRef {
        table: String,
        col_idx: usize,
    },
    /// This variant is only used when building the property. It should NEVER
    /// be used when computing cost.
    ChildColumnRef {
        col_idx: usize,
    },
    Derived,
}

/// `SemanticCorrelation` represents the semantic correlation between columns in a
/// query. "Semantic" means that the columns are correlated based on the
/// semantics of the query, not the statistics.
#[derive(Clone, Debug, Default)]
pub struct SemanticCorrelation {
    eq_column_sets: EqColumnSets,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EqPredicate {
    left: usize,
    right: usize,
}

impl EqPredicate {
    pub fn new(left: usize, right: usize) -> Self {
        Self {
            left: left.min(right),
            right: left.max(right),
        }
    }

    pub fn left(&self) -> usize {
        self.left
    }

    pub fn right(&self) -> usize {
        self.right
    }
}

/// A disjoint set of columns with equal values in the same row, along with the
/// predicates that define the equality.
#[derive(Clone, Debug, Default)]
pub struct EqColumnSets {
    eq_cols: DisjointSets<usize>,
    eq_predicates: HashSet<EqPredicate>,
}

impl EqColumnSets {
    pub fn add_predicate(&mut self, predicate: EqPredicate) {
        self.eq_predicates.insert(predicate);

        let left = predicate.left();
        let right = predicate.right();
        // Add the indices to the set if they do not exist.
        if !self.eq_cols.contains(left) {
            self.eq_cols
                .make_set(left)
                .expect("just checked left column index does not exist");
        }
        if !self.eq_cols.contains(right) {
            self.eq_cols
                .make_set(right)
                .expect("just checked right column index does not exist");
        }
        // Union the columns.
        self.eq_cols
            .union(left, right)
            .expect("both column indices should exist");
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
                    .map(|i| ColumnRef::BaseTableColumnRef {
                        table: table_name.clone(),
                        col_idx: i,
                    })
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
                    .map(|i| ColumnRef::BaseTableColumnRef {
                        table: DEFAULT_NAME.to_string(),
                        col_idx: i,
                    })
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
            OptRelNodeTyp::LogOp(_) => {
                // Concatentate the children column refs.
                let column_refs = Self::concat_children_col_refs(children);
                GroupColumnRefs::new(column_refs, None)
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
            OptRelNodeTyp::Join(_) => {
                // Concatenate left and right children column refs.
                let column_refs = Self::concat_children_col_refs(&children[0..2]);
                GroupColumnRefs::new(column_refs, None)
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
            OptRelNodeTyp::Constant(_)
            | OptRelNodeTyp::Func(_)
            | OptRelNodeTyp::BinOp(_)
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
