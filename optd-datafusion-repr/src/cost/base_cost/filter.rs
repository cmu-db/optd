use crate::plan_nodes::{OptRelNodeTyp, UnOpType};

impl<M: MostCommonValues, D: Distribution> OptCostModel<M, D> {
    /// The expr_tree input must be a "mixed expression tree".
    ///
    /// - An "expression node" refers to a RelNode that returns true for is_expression()
    /// - A "full expression tree" is where every node in the tree is an expression node
    /// - A "mixed expression tree" is where every base-case node and all its parents are expression nodes
    /// - A "base-case node" is a node that doesn't lead to further recursion (such as a BinOp(Eq))
    ///
    /// The schema input is the schema the predicate represented by the expr_tree is applied on.
    ///
    /// The output will be the selectivity of the expression tree if it were a "filter predicate".
    ///
    /// A "filter predicate" operates on one input node, unlike a "join predicate" which operates on two input nodes.
    /// This is why the function only takes in a single schema.
    fn get_filter_selectivity(
        &self,
        expr_tree: OptRelNodeRef,
        column_refs: &GroupColumnRefs,
    ) -> f64 {
        assert!(expr_tree.typ.is_expression());
        match &expr_tree.typ {
            OptRelNodeTyp::Constant(_) => Self::get_constant_selectivity(expr_tree),
            OptRelNodeTyp::ColumnRef => unimplemented!("check bool type or else panic"),
            OptRelNodeTyp::UnOp(un_op_typ) => {
                assert!(expr_tree.children.len() == 1);
                let child = expr_tree.child(0);
                match un_op_typ {
                    // not doesn't care about nulls so there's no complex logic. it just reverses the selectivity
                    // for instance, != _will not_ include nulls but "NOT ==" _will_ include nulls
                    UnOpType::Not => 1.0 - self.get_filter_selectivity(child, column_refs),
                    UnOpType::Neg => panic!(
                        "the selectivity of operations that return numerical values is undefined"
                    ),
                }
            }
            OptRelNodeTyp::BinOp(bin_op_typ) => {
                assert!(expr_tree.children.len() == 2);
                let left_child = expr_tree.child(0);
                let right_child = expr_tree.child(1);

                if bin_op_typ.is_comparison() {
                    self.get_filter_comp_op_selectivity(
                        *bin_op_typ,
                        left_child,
                        right_child,
                        column_refs,
                    )
                } else if bin_op_typ.is_numerical() {
                    panic!(
                        "the selectivity of operations that return numerical values is undefined"
                    )
                } else {
                    unreachable!("all BinOpTypes should be true for at least one is_*() function")
                }
            }
            OptRelNodeTyp::LogOp(log_op_typ) => {
                self.get_filter_log_op_selectivity(*log_op_typ, &expr_tree.children, column_refs)
            }
            OptRelNodeTyp::Func(_) => unimplemented!("check bool type or else panic"),
            OptRelNodeTyp::SortOrder(_) => {
                panic!("the selectivity of sort order expressions is undefined")
            }
            OptRelNodeTyp::Between => UNIMPLEMENTED_SEL,
            OptRelNodeTyp::Cast => unimplemented!("check bool type or else panic"),
            OptRelNodeTyp::Like => DEFAULT_MATCH_SEL,
            OptRelNodeTyp::DataType(_) => {
                panic!("the selectivity of a data type is not defined")
            }
            OptRelNodeTyp::InList => {
                let in_list_expr = InListExpr::from_rel_node(expr_tree).unwrap();
                self.get_filter_in_list_selectivity(&in_list_expr, column_refs)
            }
            _ => unreachable!(
                "all expression OptRelNodeTyp were enumerated. this should be unreachable"
            ),
        }
    }
}
