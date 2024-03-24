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
                $(, $data_name : Value)?
                $(, $inner_name : $inner_typ)?
            ) -> $struct_name {
                #[allow(unused_mut, unused)]
                let mut data = None;
                $(
                    data = Some($data_name);
                )?


                $struct_name($meta_typ(
                    optd_core::rel_node::RelNode {
                        typ: OptRelNodeTyp::$variant $( ($inner_name) )?,
                        children: vec![
                            $($child_name.into_rel_node(),)*
                            $($attr_name.into_rel_node()),*
                        ],
                        data
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
}

pub(crate) use define_plan_node;
