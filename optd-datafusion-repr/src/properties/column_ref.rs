use std::collections::HashSet;
use std::{ops::Deref, sync::Arc};

use crate::plan_nodes::{BinOpType, EmptyRelationData, JoinType, LogOpType, OptRelNodeTyp};
use anyhow::anyhow;
use optd_core::property::PropertyBuilder;
use union_find::disjoint_sets::DisjointSets;
use union_find::union_find::UnionFind;

use super::schema::Catalog;
use super::DEFAULT_NAME;

pub type BaseTableColumnRefs = Vec<ColumnRef>;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
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

impl From<BaseTableColumnRef> for ColumnRef {
    fn from(col: BaseTableColumnRef) -> Self {
        ColumnRef::BaseTableColumnRef(col)
    }
}

/// `SemanticCorrelation` represents the semantic correlation between columns in a
/// query. "Semantic" means that the columns are correlated based on the
/// semantics of the query, not the statistics.
#[derive(Clone, Debug)]
pub struct SemanticCorrelation {
    eq_columns: EqColumns,
}

impl SemanticCorrelation {
    #[cfg(test)]
    pub fn new(eq_columns: EqBaseTableColumnSets) -> Self {
        Self {
            eq_columns: EqColumns::EqBaseTableColumnSets(eq_columns),
        }
    }

    pub fn merge(x: Option<Self>, y: Option<Self>) -> Option<Self> {
        let eq_columns = match (x, y) {
            (
                Some(SemanticCorrelation {
                    eq_columns: EqColumns::EqBaseTableColumnSets(x),
                }),
                Some(SemanticCorrelation {
                    eq_columns: EqColumns::EqBaseTableColumnSets(y),
                }),
            ) => EqBaseTableColumnSets::union(x, y),
            (
                Some(SemanticCorrelation {
                    eq_columns: EqColumns::EqBaseTableColumnSets(x),
                }),
                None,
            ) => x.clone(),
            (
                None,
                Some(SemanticCorrelation {
                    eq_columns: EqColumns::EqBaseTableColumnSets(y),
                }),
            ) => y.clone(),
            _ => return None,
        };
        Some(SemanticCorrelation {
            eq_columns: EqColumns::EqBaseTableColumnSets(eq_columns),
        })
    }
}

impl TryFrom<SemanticCorrelation> for EqBaseTableColumnSets {
    type Error = anyhow::Error;

    fn try_from(semantic_correlation: SemanticCorrelation) -> Result<Self, Self::Error> {
        if let EqColumns::EqBaseTableColumnSets(eq_columns) = semantic_correlation.eq_columns {
            Ok(eq_columns)
        } else {
            Err(anyhow!("eq_columns is not EqBaseTableColumnSets"))
        }
    }
}

#[derive(Clone, Debug)]
pub enum EqColumns {
    /// Equal columns denoted by disjoint sets of base table columns,
    /// e.g. {{ t1.c1 = t2.c1 = t3.c1 }, { t1.c2 = t2.c2 }}.
    EqBaseTableColumnSets(EqBaseTableColumnSets),
    /// Equal columns denoted by pairs of column indices. This is for keeping
    /// track of the column indices in the filter/join predicates, which only
    /// contains relative column indices.
    ///
    /// It is only used when building the property. It should NEVER be used in
    /// cost computation.
    EqColumnIdxPairs(Vec<(usize, usize)>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EqPredicate {
    pub left: BaseTableColumnRef,
    pub right: BaseTableColumnRef,
}

impl EqPredicate {
    pub fn new(left: BaseTableColumnRef, right: BaseTableColumnRef) -> Self {
        Self { left, right }
    }
}

/// A disjoint set of base table columns with equal values in the same row,
/// along with the predicates that define the equalities.
#[derive(Clone, Debug, Default)]
pub struct EqBaseTableColumnSets {
    disjoint_eq_col_sets: DisjointSets<BaseTableColumnRef>,
    eq_predicates: HashSet<EqPredicate>,
}

impl EqBaseTableColumnSets {
    pub fn new() -> Self {
        Self {
            disjoint_eq_col_sets: DisjointSets::new(),
            eq_predicates: HashSet::new(),
        }
    }

    pub fn add_predicate(&mut self, predicate: EqPredicate) {
        let left = &predicate.left;
        let right = &predicate.right;

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

    /// Determine if two columns are in the same set.
    pub fn is_eq(&mut self, left: &BaseTableColumnRef, right: &BaseTableColumnRef) -> bool {
        self.disjoint_eq_col_sets
            .same_set(left, right)
            .unwrap_or(false)
    }

    pub fn contains(&self, base_col_ref: &BaseTableColumnRef) -> bool {
        self.disjoint_eq_col_sets.contains(base_col_ref)
    }

    /// Get the number of columns that are equal to `col`, including `col` itself.
    pub fn num_eq_columns(&mut self, col: &BaseTableColumnRef) -> usize {
        self.disjoint_eq_col_sets.set_size(col).unwrap()
    }

    /// Find the set of predicates that define the equality of the set of columns `col` belongs to.
    pub fn find_predicates_for_eq_column_set(
        &mut self,
        col: &BaseTableColumnRef,
    ) -> Vec<EqPredicate> {
        let mut predicates = Vec::new();
        for predicate in &self.eq_predicates {
            let left = &predicate.left;
            let right = &predicate.right;
            if (left != col && self.disjoint_eq_col_sets.same_set(col, left).unwrap())
                || (right != col && self.disjoint_eq_col_sets.same_set(col, right).unwrap())
            {
                predicates.push(predicate.clone());
            }
        }
        predicates
    }

    /// Find the set of columns that define the equality of the set of columns `col` belongs to.
    pub fn find_cols_for_eq_column_set(
        &mut self,
        col: &BaseTableColumnRef,
    ) -> HashSet<BaseTableColumnRef> {
        let predicates = self.find_predicates_for_eq_column_set(col);
        predicates
            .into_iter()
            .flat_map(|predicate| vec![predicate.left, predicate.right])
            .collect()
    }

    /// Union two `EqBaseTableColumnSets` to produce a new disjoint sets.
    pub fn union(x: EqBaseTableColumnSets, y: EqBaseTableColumnSets) -> EqBaseTableColumnSets {
        let mut eq_col_sets = Self::new();
        for predicate in x
            .eq_predicates
            .into_iter()
            .chain(y.eq_predicates.into_iter())
        {
            eq_col_sets.add_predicate(predicate);
        }
        eq_col_sets
    }
}

#[derive(Clone, Debug)]
pub struct GroupColumnRefs {
    column_refs: BaseTableColumnRefs,
    /// Correlation of the output columns of the group.
    output_correlation: Option<SemanticCorrelation>,
}

impl GroupColumnRefs {
    pub fn new(
        column_refs: BaseTableColumnRefs,
        output_correlation: Option<SemanticCorrelation>,
    ) -> Self {
        Self {
            column_refs,
            output_correlation,
        }
    }

    pub fn base_table_column_refs(&self) -> &BaseTableColumnRefs {
        &self.column_refs
    }

    pub fn output_correlation(&self) -> Option<&SemanticCorrelation> {
        self.output_correlation.as_ref()
    }
}

pub struct ColumnRefPropertyBuilder {
    catalog: Arc<dyn Catalog>,
}

impl ColumnRefPropertyBuilder {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    fn concat_children_col_refs(children: &[&GroupColumnRefs]) -> BaseTableColumnRefs {
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
                                    eq_columns: EqColumns::EqColumnIdxPairs(pairs),
                                }) = &child.output_correlation
                                {
                                    eq_column_idx_pairs.extend(pairs.iter());
                                }
                            }
                            Some(SemanticCorrelation {
                                eq_columns: EqColumns::EqColumnIdxPairs(eq_column_idx_pairs),
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
                GroupColumnRefs::new(column_refs, child.output_correlation.clone())
            }
            // Should account for all physical join types.
            OptRelNodeTyp::Join(join_type)
            | OptRelNodeTyp::RawDepJoin(join_type)
            | OptRelNodeTyp::DepJoin(join_type) => {
                // Concatenate left and right children column refs.
                let column_refs = Self::concat_children_col_refs(&children[0..2]);
                // Merge the equal columns of two children as input correlation.
                let children_correlation = SemanticCorrelation::merge(
                    children[0].output_correlation.clone(),
                    children[1].output_correlation.clone(),
                );
                let mut children_eq_columns =
                    if let Some(children_correlation) = children_correlation {
                        EqBaseTableColumnSets::try_from(children_correlation).unwrap()
                    } else {
                        EqBaseTableColumnSets::new()
                    };

                // If the join type is inner or cross, merge the equal columns in the join condition
                // into the those from the children.
                //
                // Otherwise be conservative and discard all correlations.
                let output_correlation = match join_type {
                    JoinType::Inner | JoinType::Cross => {
                        // Merge the equal columns in the join condition into the those from the children.
                        if let Some(SemanticCorrelation {
                            eq_columns: EqColumns::EqColumnIdxPairs(pairs),
                        }) = &children[2].output_correlation
                        {
                            for (l_col_idx, r_col_idx) in pairs {
                                let l_col_ref = &column_refs[*l_col_idx];
                                let r_col_ref = &column_refs[*r_col_idx];
                                if let (
                                    ColumnRef::BaseTableColumnRef(l),
                                    ColumnRef::BaseTableColumnRef(r),
                                ) = (l_col_ref, r_col_ref)
                                {
                                    children_eq_columns
                                        .add_predicate(EqPredicate::new(l.clone(), r.clone()));
                                }
                            }
                        };
                        Some(SemanticCorrelation {
                            eq_columns: EqColumns::EqBaseTableColumnSets(children_eq_columns),
                        })
                    }
                    _ => None,
                };
                GroupColumnRefs::new(column_refs, output_correlation)
            }
            OptRelNodeTyp::Agg => {
                let child = children[0];
                // Group by columns first.
                let mut group_by_col_refs: Vec<_> = children[2]
                    .column_refs
                    .iter()
                    .map(|p| {
                        let col_idx = match p {
                            ColumnRef::ChildColumnRef { col_idx } => *col_idx,
                            _ => panic!("group by expr must be ColumnRef"),
                        };
                        child.column_refs[col_idx].clone()
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
                                    eq_columns: EqColumns::EqColumnIdxPairs(vec![(
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
            | OptRelNodeTyp::InList
            | OptRelNodeTyp::ExternColumnRef => {
                GroupColumnRefs::new(vec![ColumnRef::Derived], None)
            }
            _ => unimplemented!("Unsupported rel node type {:?}", typ),
        }
    }

    fn property_name(&self) -> &'static str {
        "column_ref"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eq_base_table_column_sets() {
        let col1 = BaseTableColumnRef {
            table: "t1".to_string(),
            col_idx: 1,
        };
        let col2 = BaseTableColumnRef {
            table: "t2".to_string(),
            col_idx: 2,
        };
        let col3 = BaseTableColumnRef {
            table: "t3".to_string(),
            col_idx: 3,
        };
        let col4 = BaseTableColumnRef {
            table: "t4".to_string(),
            col_idx: 4,
        };
        let pred1 = EqPredicate::new(col1.clone(), col2.clone());
        let pred2 = EqPredicate::new(col3.clone(), col4.clone());
        let pred3 = EqPredicate::new(col1.clone(), col3.clone());

        let mut eq_col_sets = EqBaseTableColumnSets::new();

        // (1, 2)
        eq_col_sets.add_predicate(pred1.clone());
        assert!(eq_col_sets.is_eq(&col1, &col2));

        // (1, 2), (3, 4)
        eq_col_sets.add_predicate(pred2.clone());
        assert!(eq_col_sets.is_eq(&col3, &col4));
        assert!(!eq_col_sets.is_eq(&col2, &col3));

        let predicates = eq_col_sets.find_predicates_for_eq_column_set(&col1);
        assert_eq!(predicates.len(), 1);
        assert!(predicates.contains(&pred1));

        let predicates = eq_col_sets.find_predicates_for_eq_column_set(&col3);
        assert_eq!(predicates.len(), 1);
        assert!(predicates.contains(&pred2));

        // (1, 2, 3, 4)
        eq_col_sets.add_predicate(pred3.clone());
        assert!(eq_col_sets.is_eq(&col1, &col3));
        assert!(eq_col_sets.is_eq(&col2, &col4));
        assert!(eq_col_sets.is_eq(&col1, &col4));

        let predicates = eq_col_sets.find_predicates_for_eq_column_set(&col1);
        assert_eq!(predicates.len(), 3);
        assert!(predicates.contains(&pred1));
        assert!(predicates.contains(&pred2));
        assert!(predicates.contains(&pred3));
    }
}
