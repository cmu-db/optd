/// Plan nodes with data fields must implement `ExplainData` trait. An example:
///
/// ```ignore
/// #[derive(Clone, Debug)]
/// struct PhysicalDummy(PlanNode);
///
/// // Implement `OptRelNode` using `define_plan_node!`...
///
/// impl ExplainData for PhysicalDummy {
///     fn explain_data(data: &Value) -> Vec<(&'static str, Pretty<'static>)> {
///         if let Value::Int32(i) = data {
///             vec![("primitive_data", i.to_string().into())]
///         } else {
///             unreachable!()
///         }
///     }
/// }
/// ```
macro_rules! define_plan_node {
    (
        $struct_name:ident : $meta_typ:tt,
        $variant:ident,
        [ $({ $child_id:literal, $child_name:ident : $child_meta_typ:ty }),* ] ,
        [ $({ $attr_id:literal, $attr_name:ident : $attr_meta_typ:ty }),* ]
        $(, { $inner_name:ident : $inner_typ:ty })?
        $(, $data_name:ident)?
    ) => {
        impl OptRelNode for $struct_name {
            fn into_rel_node(self) -> OptRelNodeRef {
                self.0.into_rel_node()
            }

            fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
                #[allow(unused_variables)]
                if let OptRelNodeTyp :: $variant $( ($inner_name) )? = rel_node.typ {
                    <$meta_typ>::from_rel_node(rel_node).map(Self)
                } else {
                    None
                }
            }

            fn dispatch_explain(&self, meta_map: Option<&crate::RelNodeMetaMap>) -> pretty_xmlish::Pretty<'static> {
                use crate::explain::Insertable;

                let mut fields = vec![
                    $( (stringify!($inner_name), self.$inner_name().to_string().into() ) , )?
                    $( (stringify!($attr_name), self.$attr_name().explain(meta_map) ) ),*
                ];
                if let Some(meta_map) = meta_map {
                    fields = fields.with_meta(self.0.get_meta(meta_map));
                };
                define_plan_node!(@expand_fields self, $struct_name, fields $(, $data_name)?);

                pretty_xmlish::Pretty::simple_record(
                    stringify!($struct_name),
                    fields,
                    vec![
                        $( self.$child_name().explain(meta_map) ),*
                    ],
                )
            }
        }

        impl $struct_name {
            pub fn new(
                $($child_name : $child_meta_typ,)*
                $($attr_name : $attr_meta_typ),*
                $($data_name: Value)?
                $(, $inner_name : $inner_typ)?
            ) -> $struct_name {
                #[allow(unused_mut, unused)]
                let mut data = None;
                $(
                    data = Some($data_name);
                )*
                $struct_name($meta_typ(
                    optd_core::rel_node::RelNode {
                        typ: OptRelNodeTyp::$variant $( ($inner_name) )?,
                        children: vec![
                            $($child_name.into_rel_node(),)*
                            $($attr_name.into_rel_node()),*
                        ],
                        data,
                    }
                    .into(),
                ))
            }

            $(
                pub fn $child_name(&self) -> $child_meta_typ {
                    <$child_meta_typ>::from_rel_node(self.clone().into_rel_node().child($child_id)).unwrap()
                }
            )*


            $(
                pub fn $attr_name(&self) -> $attr_meta_typ {
                    <$attr_meta_typ>::from_rel_node(self.clone().into_rel_node().child($attr_id)).unwrap()
                }
            )*

            $(
                pub fn $inner_name(&self) -> JoinType {
                    if let OptRelNodeTyp :: $variant ($inner_name) = self.0 .0.typ {
                        return $inner_name;
                    } else {
                        unreachable!();
                    }
                }
            )?
        }
    };
    // Dummy branch that does nothing when data is `None`.
    (@expand_fields $self:ident, $struct_name:ident, $fields:ident) => {};
    // Expand explain fields with data.
    (@expand_fields $self:ident, $struct_name:ident, $fields:ident, $data_name:ident) => {
        let data = $self.0 .0.data.as_ref().unwrap();
        $fields.extend($struct_name::explain_data(data));
    };
}

pub(crate) use define_plan_node;

#[cfg(test)]
mod test {
    use crate::plan_nodes::*;
    use optd_core::rel_node::Value;
    use serde::{Deserialize, Serialize};

    fn get_explain_str(pretty: &Pretty) -> String {
        let mut config = PrettyConfig {
            need_boundaries: false,
            reduced_spaces: false,
            width: 300,
            ..Default::default()
        };
        let mut out = String::new();
        config.unicode(&mut out, pretty);
        out
    }

    /// Ensure `define_plan_node` works with data field.
    #[test]
    fn test_explain_complex_data() {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        struct ComplexData {
            a: i32,
            b: String,
        }

        #[derive(Clone, Debug)]
        struct PhysicalComplexDummy(PlanNode);

        impl ExplainData for PhysicalComplexDummy {
            fn explain_data(data: &Value) -> Vec<(&'static str, Pretty<'static>)> {
                if let Value::Serialized(serialized_data) = data {
                    let data: ComplexData = bincode::deserialize(serialized_data).unwrap();
                    vec![
                        ("a", data.a.to_string().into()),
                        ("b", data.b.to_string().into()),
                    ]
                } else {
                    unreachable!()
                }
            }
        }

        define_plan_node!(
            PhysicalComplexDummy: PlanNode,
            PhysicalScan, [
                { 0, child: PlanNode }
            ], [
            ],
            complex_data
        );

        let node = PhysicalComplexDummy::new(
            LogicalScan::new("a".to_string()).0,
            Value::Serialized(
                bincode::serialize(&ComplexData {
                    a: 1,
                    b: "a".to_string(),
                })
                .unwrap()
                .into_iter()
                .collect(),
            ),
        );
        let pretty = node.dispatch_explain(None);
        println!("{}", get_explain_str(&pretty));
    }
}
