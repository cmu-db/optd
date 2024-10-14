use arrow_schema::DataType;
use optd_core::rel_node::{RelNode, RelNodeMetaMap};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp};

#[derive(Clone, Debug)]
pub struct DataTypeExpr(pub Expr);

impl DataTypeExpr {
    pub fn new(typ: DataType) -> Self {
        DataTypeExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::DataType(typ),
                children: vec![],
                data: None,
            }
            .into(),
        ))
    }

    pub fn data_type(&self) -> DataType {
        if let OptRelNodeTyp::DataType(data_type) = self.0.typ() {
            data_type
        } else {
            panic!("not a data type")
        }
    }
}

impl OptRelNode for DataTypeExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::DataType(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, _meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::display(&self.data_type().to_string())
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalDataTypeExpr(pub Expr);

impl PhysicalDataTypeExpr {
    pub fn new(typ: DataType) -> Self {
        PhysicalDataTypeExpr(Expr(
            RelNode {
                typ: OptRelNodeTyp::PhysicalDataType(typ),
                children: vec![],
                data: None,
            }
            .into(),
        ))
    }

    pub fn data_type(&self) -> DataType {
        if let OptRelNodeTyp::PhysicalDataType(data_type) = self.0.typ() {
            data_type
        } else {
            panic!("not a data type")
        }
    }
}

impl OptRelNode for PhysicalDataTypeExpr {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if !matches!(rel_node.typ, OptRelNodeTyp::PhysicalDataType(_)) {
            return None;
        }
        Expr::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self, _meta_map: Option<&RelNodeMetaMap>) -> Pretty<'static> {
        Pretty::display(&self.data_type().to_string())
    }
}
