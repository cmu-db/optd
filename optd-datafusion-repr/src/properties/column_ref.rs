use std::{ops::Deref, sync::Arc};

use optd_core::property::PropertyBuilder;

use crate::plan_nodes::{EmptyRelationData, OptRelNodeTyp};

use super::schema::Catalog;
use super::DEFAULT_NAME;

#[derive(Clone, Debug)]
pub enum ColumnRef {
    BaseTableColumnRef { table: String, col_idx: usize },
    ChildColumnRef { col_idx: usize },
    Derived,
}

pub type GroupColumnRefs = Vec<ColumnRef>;

pub struct ColumnRefPropertyBuilder {
    catalog: Arc<dyn Catalog>,
}

impl ColumnRefPropertyBuilder {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    fn concat_children_properties(children: &[&GroupColumnRefs]) -> GroupColumnRefs {
        children.iter().flat_map(Deref::deref).cloned().collect()
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
                (0..column_cnt)
                    .map(|i| ColumnRef::BaseTableColumnRef {
                        table: table_name.clone(),
                        col_idx: i,
                    })
                    .collect()
            }
            OptRelNodeTyp::EmptyRelation => {
                let data = data.unwrap().as_slice();
                let empty_relation_data: EmptyRelationData =
                    bincode::deserialize(data.as_ref()).unwrap();
                let schema = empty_relation_data.schema;
                let column_cnt = schema.fields.len();
                (0..column_cnt)
                    .map(|i| ColumnRef::BaseTableColumnRef {
                        table: DEFAULT_NAME.to_string(),
                        col_idx: i,
                    })
                    .collect()
            }
            OptRelNodeTyp::ColumnRef => {
                let col_ref_idx = data.unwrap().as_u64();
                // this is always safe since col_ref_idx was initially a usize in ColumnRefExpr::new()
                let usize_col_ref_idx = col_ref_idx as usize;
                vec![ColumnRef::ChildColumnRef {
                    col_idx: usize_col_ref_idx,
                }]
            }
            OptRelNodeTyp::List => {
                // Concatentate the children properties.
                Self::concat_children_properties(children)
            }
            OptRelNodeTyp::LogOp(_) => {
                // Concatentate the children properties.
                Self::concat_children_properties(children)
            }
            OptRelNodeTyp::Projection => children[1]
                .iter()
                .map(|p| match p {
                    ColumnRef::ChildColumnRef { col_idx } => children[0][*col_idx].clone(),
                    ColumnRef::Derived => ColumnRef::Derived,
                    _ => panic!("projection expr must be Derived or ChildColumnRef"),
                })
                .collect(),
            // Should account for all physical join types.
            OptRelNodeTyp::Join(_) => {
                // Concatenate left and right children properties.
                Self::concat_children_properties(&children[0..2])
            }
            OptRelNodeTyp::Agg => {
                // Group by columns first.
                let mut group_by_col_refs: Vec<_> = children[2]
                    .iter()
                    .map(|p| {
                        let col_idx = match p {
                            ColumnRef::ChildColumnRef { col_idx } => *col_idx,
                            _ => panic!("group by expr must be ColumnRef"),
                        };
                        children[0][col_idx].clone()
                    })
                    .collect();
                // Then the aggregate expressions. These columns, (e.g. SUM, COUNT, etc.) are derived columns.
                let agg_expr_cnt = children[1].len();
                group_by_col_refs.extend((0..agg_expr_cnt).map(|_| ColumnRef::Derived));
                group_by_col_refs
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
            | OptRelNodeTyp::InList => {
                vec![ColumnRef::Derived]
            }
            _ => unimplemented!("Unsupported rel node type {:?}", typ),
        }
    }

    fn property_name(&self) -> &'static str {
        "column_ref"
    }
}
