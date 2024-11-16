// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use arrow_schema::DataType;
use optd_core::nodes::{PlanNodeMetaMap, Value};
use pretty_xmlish::Pretty;

use crate::plan_nodes::{ArcDfPredNode, DfPredNode, DfPredType, DfReprPredNode};

#[derive(Clone, Debug)]
pub struct DataTypePred(pub ArcDfPredNode);

impl DataTypePred {
    pub fn new(typ: DataType) -> Self {
        DataTypePred(
            DfPredNode {
                typ: DfPredType::DataType,
                children: vec![],
                data: Some(Value::Serialized(bincode::serialize(&typ).unwrap().into())),
            }
            .into(),
        )
    }

    pub fn data_type(&self) -> DataType {
        if let DfPredType::DataType = self.0.typ {
            let data_type: DataType =
                bincode::deserialize(&self.0.data.as_ref().unwrap().as_slice()).unwrap();
            data_type
        } else {
            panic!("not a data type")
        }
    }
}

impl DfReprPredNode for DataTypePred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if !matches!(pred_node.typ, DfPredType::DataType) {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, _meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::display(&self.data_type().to_string())
    }
}
