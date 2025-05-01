use super::field_indexing::find_field_index;
use super::operators::{transform_pattern_to_operator, transform_struct_to_operator};
use crate::dsl::analyzer::hir::{Literal, LogicalOp, Materializable, Operator, PhysicalOp};
use crate::dsl::analyzer::type_checks::registry::{LOGICAL_TYPE, TypeKind};
use crate::dsl::analyzer::{
    hir::{
        self, CoreData, Expr, ExprKind, FunKind, LetBinding, MatchArm, Pattern, PatternKind,
        TypedSpan,
    },
    type_checks::registry::TypeRegistry,
};
use std::sync::Arc;

/// Converts a typed expression to an untyped HIR expression.
///
/// Recursively processes each expression node, transforming specific patterns
/// like structs into operators when appropriate, and handling field access.
///
/// # Arguments
///
/// * `expr` - The typed expression to convert
/// * `registry` - Type registry for resolving types and inheritance
///
/// # Returns
///
/// An untyped expression with equivalent structure
pub(super) fn convert_expr(expr: &Arc<Expr<TypedSpan>>, registry: &TypeRegistry) -> Arc<Expr> {
    use ExprKind::*;

    let converted_kind = match &expr.kind {
        PatternMatch(expr, arms) => {
            let converted_arms = arms
                .iter()
                .map(|arm| MatchArm {
                    pattern: convert_pattern(&arm.pattern, registry),
                    expr: convert_expr(&arm.expr, registry),
                })
                .collect();

            PatternMatch(convert_expr(expr, registry), converted_arms)
        }
        IfThenElse(cond, then_expr, else_expr) => IfThenElse(
            convert_expr(cond, registry),
            convert_expr(then_expr, registry),
            convert_expr(else_expr, registry),
        ),
        NewScope(expr) => NewScope(convert_expr(expr, registry)),
        Let(binding, expr) => Let(
            LetBinding::new(binding.name.clone(), convert_expr(&binding.expr, registry)),
            convert_expr(expr, registry),
        ),
        Binary(left, op, right) => Binary(
            convert_expr(left, registry),
            op.clone(),
            convert_expr(right, registry),
        ),
        Unary(op, expr) => Unary(op.clone(), convert_expr(expr, registry)),
        Call(func, args) => {
            let converted_func = convert_expr(func, registry);
            let converted_args = args.iter().map(|arg| convert_expr(arg, registry)).collect();

            Call(converted_func, converted_args)
        }
        Map(entries) => {
            let converted_entries = entries
                .iter()
                .map(|(key, value)| (convert_expr(key, registry), convert_expr(value, registry)))
                .collect();

            Map(converted_entries)
        }
        Ref(ident) => Ref(ident.clone()),
        Return(expr) => Return(convert_expr(expr, registry)),
        FieldAccess(expr, field_name) => convert_field_access(expr, field_name, registry),
        CoreExpr(core_data) => CoreExpr(convert_core_data_expr(core_data, registry)),
        CoreVal(_) => {
            // TODO(#80): Nothing to do here as only one type of CoreVal is created.
            // 1. External closures (i.e. declared with fn), but those are already handled in
            //    convert.
            // Note: It makes no sense that functions can be expressions. They should only be
            // values. Once we change that, we just move the function core_expr code.
            panic!("CoreVal should not be in HIR<TypedSpan>")
        }
    };

    Expr::new(converted_kind).into()
}

/// Converts a typed pattern to an untyped HIR pattern.
///
/// Processes pattern nodes, transforming structured patterns into operator
/// patterns when they match logical or physical types.
///
/// # Arguments
///
/// * `pattern` - The typed pattern to convert
/// * `registry` - Type registry for resolving types and inheritance
///
/// # Returns
///
/// An untyped pattern with equivalent structure
pub(super) fn convert_pattern(pattern: &Pattern<TypedSpan>, registry: &TypeRegistry) -> Pattern {
    use PatternKind::*;

    let converted_kind = match &pattern.kind {
        Bind(ident, pattern) => Bind(ident.clone(), convert_pattern(pattern, registry).into()),
        Literal(lit) => Literal(lit.clone()),
        Struct(name, patterns) => {
            if registry.is_logical_or_physical(name) {
                let (children, data) = transform_pattern_to_operator(name, patterns, registry);

                Operator(hir::Operator {
                    tag: name.clone(),
                    data,
                    children,
                })
            } else {
                let converted_patterns = patterns
                    .iter()
                    .map(|pattern| convert_pattern(pattern, registry))
                    .collect();

                Struct(name.clone(), converted_patterns)
            }
        }
        Operator(_) => {
            panic!("Operator patterns are not supported in HIR<TypedSpan>");
        }
        Wildcard => Wildcard,
        EmptyArray => EmptyArray,
        ArrayDecomp(head, tail) => ArrayDecomp(
            convert_pattern(head, registry).into(),
            convert_pattern(tail, registry).into(),
        ),
    };

    Pattern::new(converted_kind)
}

/// Converts core data expressions to untyped HIR.
///
/// Handles special transformation of logical/physical types into operator
/// representations while preserving other core expressions.
///
/// # Arguments
///
/// * `core` - The core data expression to convert
/// * `registry` - Type registry for type resolution
///
/// # Returns
///
/// Converted core data without type information
fn convert_core_data_expr(
    core: &CoreData<Arc<Expr<TypedSpan>>, TypedSpan>,
    registry: &TypeRegistry,
) -> CoreData<Arc<Expr>> {
    use CoreData::*;
    use FunKind::*;
    use Materializable::*;

    match core {
        Literal(lit) => Literal(lit.clone()),
        Array(elements) => {
            let converted_elements = elements
                .iter()
                .map(|expr| convert_expr(expr, registry))
                .collect();

            Array(converted_elements)
        }
        Tuple(elements) => {
            let converted_elements = elements
                .iter()
                .map(|expr| convert_expr(expr, registry))
                .collect();

            Tuple(converted_elements)
        }
        Struct(name, fields) => {
            if registry.is_logical_or_physical(name) {
                let (children, data) = transform_struct_to_operator(name, fields, registry);
                let operator = Operator {
                    tag: name.clone(),
                    data,
                    children,
                };

                if registry.inherits_adt(name, LOGICAL_TYPE) {
                    Logical(Materialized(LogicalOp::logical(operator)))
                } else {
                    Physical(Materialized(PhysicalOp::physical(operator)))
                }
            } else {
                let converted_fields = fields
                    .iter()
                    .map(|field| convert_expr(field, registry))
                    .collect();

                Struct(name.clone(), converted_fields)
            }
        }
        Map(_) | Logical(_) | Physical(_) => {
            panic!("Types may not be in the HIR yet")
        }
        Function(Udf(_)) => panic!("UDFs may not appear within functions"),
        Function(Closure(params, body)) => {
            Function(Closure(params.to_vec(), convert_expr(body, registry)))
        }
        Fail(expr) => Fail(convert_expr(expr, registry).into()),
        None => None,
    }
}

/// Converts field access expressions into their HIR representation.
///
/// This function handles three cases:
/// 1. Tuple field access using numeric indices or _N pattern
/// 2. ADT field access, which is converted to an indexed call
/// 3. Error handling for invalid access on other types
///
/// # Arguments
///
/// * `expr` - The expression whose field is being accessed
/// * `field_name` - The name of the field or index being accessed
/// * `registry` - The type registry for type resolution
///
/// # Returns
///
/// An `ExprKind` representing the transformed field access
fn convert_field_access(
    expr: &Arc<Expr<TypedSpan>>,
    field_name: &str,
    registry: &TypeRegistry,
) -> ExprKind {
    use ExprKind::*;

    let expr_type = registry.resolve_type(&expr.metadata.ty);

    match &*expr_type.value {
        TypeKind::Tuple(_) => {
            let index = if let Some(index_str) = field_name.strip_prefix('_') {
                // _N pattern.
                index_str
                    .parse::<usize>()
                    .expect("Type checking should have validated the tuple index")
            } else {
                panic!("Invalid tuple field access pattern: {}", field_name)
            };

            Call(
                convert_expr(expr, registry),
                vec![Expr::new(CoreExpr(CoreData::Literal(Literal::Int64(index as i64)))).into()],
            )
        }

        TypeKind::Adt(struct_name) => {
            let field_index = find_field_index(struct_name, field_name, registry);

            Call(
                convert_expr(expr, registry),
                vec![Expr::new(CoreExpr(CoreData::Literal(Literal::Int64(field_index)))).into()],
            )
        }

        _ => panic!("Field access on non-struct, non-tuple type: error in type inference"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{
        analyzer::{
            hir::{BinOp, Literal},
            type_checks::registry::{
                LOGICAL_TYPE, PHYSICAL_TYPE, TypeKind, TypeRegistry,
                type_registry_tests::{
                    create_product_adt, create_sum_adt, create_test_span, spanned,
                },
            },
        },
        parser::ast::Type as AstType,
    };

    fn create_registry_with_operators() -> TypeRegistry {
        let mut registry = TypeRegistry::new();

        // Create LogicalJoin
        let logical_join = create_product_adt(
            "LogicalJoin",
            vec![
                ("join_type", AstType::String),
                ("condition", AstType::String),
                ("left", AstType::Identifier(LOGICAL_TYPE.to_string())),
                ("right", AstType::Identifier(LOGICAL_TYPE.to_string())),
            ],
        );

        // Create LogicalScan
        let logical_scan = create_product_adt("LogicalScan", vec![("table_name", AstType::String)]);

        // Create PhysicalHashJoin
        let physical_hash_join = create_product_adt(
            "PhysicalHashJoin",
            vec![
                ("hash_keys", AstType::Array(spanned(AstType::String))),
                ("build_side", AstType::String),
                ("left", AstType::Identifier(PHYSICAL_TYPE.to_string())),
                ("right", AstType::Identifier(PHYSICAL_TYPE.to_string())),
            ],
        );

        // Create PhysicalTableScan
        let physical_table_scan = create_product_adt(
            "PhysicalTableScan",
            vec![
                ("table_name", AstType::String),
                ("columns", AstType::Array(spanned(AstType::String))),
            ],
        );

        // Create enums
        let logical_enum = create_sum_adt(LOGICAL_TYPE, vec![logical_join, logical_scan]);
        let physical_enum =
            create_sum_adt(PHYSICAL_TYPE, vec![physical_hash_join, physical_table_scan]);

        registry.register_adt(&logical_enum).unwrap();
        registry.register_adt(&physical_enum).unwrap();
        registry
    }

    #[test]
    fn test_convert_literal_expr() {
        let registry = TypeRegistry::new();
        let lit_expr = Expr::new_with(
            ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(42))),
            TypeKind::I64.into(),
            create_test_span(),
        );

        let converted = convert_expr(&Arc::new(lit_expr), &registry);

        match &converted.kind {
            ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(value))) => {
                assert_eq!(*value, 42);
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_convert_binary_expr() {
        let registry = TypeRegistry::new();
        let binary_expr = Expr::new_with(
            ExprKind::Binary(
                Arc::new(Expr::new_with(
                    ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(1))),
                    TypeKind::I64.into(),
                    create_test_span(),
                )),
                BinOp::Add,
                Arc::new(Expr::new_with(
                    ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(2))),
                    TypeKind::I64.into(),
                    create_test_span(),
                )),
            ),
            TypeKind::I64.into(),
            create_test_span(),
        );

        let converted = convert_expr(&Arc::new(binary_expr), &registry);

        match &converted.kind {
            ExprKind::Binary(left, op, right) => {
                assert!(matches!(op, BinOp::Add));
                match &left.kind {
                    ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(1))) => {}
                    _ => panic!("Expected literal 1"),
                }
                match &right.kind {
                    ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(2))) => {}
                    _ => panic!("Expected literal 2"),
                }
            }
            _ => panic!("Expected binary operation"),
        }
    }

    #[test]
    fn test_convert_field_access() {
        let mut registry = TypeRegistry::new();
        let point = create_product_adt(
            "Point",
            vec![
                ("x", AstType::Int64),
                ("y", AstType::Int64),
                ("z", AstType::Int64),
            ],
        );
        registry.register_adt(&point).unwrap();

        let field_access = Expr::new_with(
            ExprKind::FieldAccess(
                Arc::new(Expr::new_with(
                    ExprKind::Ref("point".to_string()),
                    TypeKind::Adt("Point".to_string()).into(),
                    create_test_span(),
                )),
                "y".to_string(),
            ),
            TypeKind::I64.into(),
            create_test_span(),
        );

        let converted = convert_expr(&Arc::new(field_access), &registry);

        match &converted.kind {
            ExprKind::Call(_, args) => {
                assert_eq!(args.len(), 1);
                match &args[0].kind {
                    ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(1))) => {}
                    _ => panic!("Expected index 1 for field 'y'"),
                }
            }
            _ => panic!("Expected call expression"),
        }
    }

    #[test]
    fn test_convert_tuple_field_access_underscore_syntax() {
        let registry = TypeRegistry::new();

        // Create a tuple expression with three elements
        let tuple_expr = Expr::new_with(
            ExprKind::CoreExpr(CoreData::Tuple(vec![
                Arc::new(Expr::new_with(
                    ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(1))),
                    TypeKind::I64.into(),
                    create_test_span(),
                )),
                Arc::new(Expr::new_with(
                    ExprKind::CoreExpr(CoreData::Literal(Literal::String("hello".to_string()))),
                    TypeKind::String.into(),
                    create_test_span(),
                )),
                Arc::new(Expr::new_with(
                    ExprKind::CoreExpr(CoreData::Literal(Literal::Bool(true))),
                    TypeKind::Bool.into(),
                    create_test_span(),
                )),
            ])),
            TypeKind::Tuple(vec![
                TypeKind::I64.into(),
                TypeKind::String.into(),
                TypeKind::Bool.into(),
            ])
            .into(),
            create_test_span(),
        );

        // Access the second element (_1) which should be the string "hello"
        let field_access = Expr::new_with(
            ExprKind::FieldAccess(Arc::new(tuple_expr), "_1".to_string()),
            TypeKind::String.into(),
            create_test_span(),
        );

        let converted = convert_expr(&Arc::new(field_access), &registry);

        match &converted.kind {
            ExprKind::Call(_, args) => {
                assert_eq!(args.len(), 1);
                match &args[0].kind {
                    ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(index))) => {
                        assert_eq!(*index, 1, "Expected index 1 for field '_1'");
                    }
                    _ => panic!("Expected literal int index"),
                }
            }
            _ => panic!("Expected call expression"),
        }
    }

    #[test]
    fn test_convert_pattern_literal() {
        let registry = TypeRegistry::new();
        let pattern = Pattern::new_with(
            PatternKind::Literal(Literal::String("test".to_string())),
            TypeKind::String.into(),
            create_test_span(),
        );

        let converted = convert_pattern(&pattern, &registry);

        match &converted.kind {
            PatternKind::Literal(Literal::String(s)) => {
                assert_eq!(s, "test");
            }
            _ => panic!("Expected literal pattern"),
        }
    }

    #[test]
    fn test_convert_logical_struct_to_operator() {
        let registry = create_registry_with_operators();

        // Create LogicalJoin struct
        let logical_join = Expr::new_with(
            ExprKind::CoreExpr(CoreData::Struct(
                "LogicalJoin".to_string(),
                vec![
                    // join_type: "inner"
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(CoreData::Literal(Literal::String("inner".to_string()))),
                        TypeKind::String.into(),
                        create_test_span(),
                    )),
                    // condition: "a = b"
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(CoreData::Literal(Literal::String("a = b".to_string()))),
                        TypeKind::String.into(),
                        create_test_span(),
                    )),
                    // left: LogicalScan
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(CoreData::Struct(
                            "LogicalScan".to_string(),
                            vec![Arc::new(Expr::new_with(
                                ExprKind::CoreExpr(CoreData::Literal(Literal::String(
                                    "left_table".to_string(),
                                ))),
                                TypeKind::String.into(),
                                create_test_span(),
                            ))],
                        )),
                        TypeKind::Adt("LogicalScan".to_string()).into(),
                        create_test_span(),
                    )),
                    // right: LogicalScan
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(CoreData::Struct(
                            "LogicalScan".to_string(),
                            vec![Arc::new(Expr::new_with(
                                ExprKind::CoreExpr(CoreData::Literal(Literal::String(
                                    "right_table".to_string(),
                                ))),
                                TypeKind::String.into(),
                                create_test_span(),
                            ))],
                        )),
                        TypeKind::Adt("LogicalScan".to_string()).into(),
                        create_test_span(),
                    )),
                ],
            )),
            TypeKind::Adt("LogicalJoin".to_string()).into(),
            create_test_span(),
        );

        let converted = convert_expr(&Arc::new(logical_join), &registry);

        match &converted.kind {
            ExprKind::CoreExpr(CoreData::Logical(Materializable::Materialized(logical_op))) => {
                assert_eq!(logical_op.operator.tag, "LogicalJoin");
                assert_eq!(logical_op.operator.data.len(), 2); // join_type and condition
                assert_eq!(logical_op.operator.children.len(), 2); // left and right

                // Check data fields
                match &logical_op.operator.data[0].kind {
                    ExprKind::CoreExpr(CoreData::Literal(Literal::String(s))) => {
                        assert_eq!(s, "inner");
                    }
                    _ => panic!("Expected join_type string"),
                }
                match &logical_op.operator.data[1].kind {
                    ExprKind::CoreExpr(CoreData::Literal(Literal::String(s))) => {
                        assert_eq!(s, "a = b");
                    }
                    _ => panic!("Expected condition string"),
                }

                // Check children are logical operators
                for child in &logical_op.operator.children {
                    match &child.kind {
                        ExprKind::CoreExpr(CoreData::Logical(_)) => {}
                        _ => panic!("Expected logical operator child"),
                    }
                }
            }
            _ => panic!("Expected LogicalOp"),
        }
    }

    #[test]
    fn test_convert_physical_struct_to_operator() {
        let registry = create_registry_with_operators();

        // Create PhysicalHashJoin struct
        let physical_hash_join = Expr::new_with(
            ExprKind::CoreExpr(CoreData::Struct(
                "PhysicalHashJoin".to_string(),
                vec![
                    // hash_keys: ["key1", "key2"]
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(CoreData::Array(vec![
                            Arc::new(Expr::new_with(
                                ExprKind::CoreExpr(CoreData::Literal(Literal::String(
                                    "key1".to_string(),
                                ))),
                                TypeKind::String.into(),
                                create_test_span(),
                            )),
                            Arc::new(Expr::new_with(
                                ExprKind::CoreExpr(CoreData::Literal(Literal::String(
                                    "key2".to_string(),
                                ))),
                                TypeKind::String.into(),
                                create_test_span(),
                            )),
                        ])),
                        TypeKind::Array(TypeKind::String.into()).into(),
                        create_test_span(),
                    )),
                    // build_side: "left"
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(CoreData::Literal(Literal::String("left".to_string()))),
                        TypeKind::String.into(),
                        create_test_span(),
                    )),
                    // left: PhysicalTableScan
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(CoreData::Struct(
                            "PhysicalTableScan".to_string(),
                            vec![
                                Arc::new(Expr::new_with(
                                    ExprKind::CoreExpr(CoreData::Literal(Literal::String(
                                        "left_table".to_string(),
                                    ))),
                                    TypeKind::String.into(),
                                    create_test_span(),
                                )),
                                Arc::new(Expr::new_with(
                                    ExprKind::CoreExpr(CoreData::Array(vec![])),
                                    TypeKind::Array(TypeKind::String.into()).into(),
                                    create_test_span(),
                                )),
                            ],
                        )),
                        TypeKind::Adt("PhysicalTableScan".to_string()).into(),
                        create_test_span(),
                    )),
                    // right: PhysicalTableScan
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(CoreData::Struct(
                            "PhysicalTableScan".to_string(),
                            vec![
                                Arc::new(Expr::new_with(
                                    ExprKind::CoreExpr(CoreData::Literal(Literal::String(
                                        "right_table".to_string(),
                                    ))),
                                    TypeKind::String.into(),
                                    create_test_span(),
                                )),
                                Arc::new(Expr::new_with(
                                    ExprKind::CoreExpr(CoreData::Array(vec![])),
                                    TypeKind::Array(TypeKind::String.into()).into(),
                                    create_test_span(),
                                )),
                            ],
                        )),
                        TypeKind::Adt("PhysicalTableScan".to_string()).into(),
                        create_test_span(),
                    )),
                ],
            )),
            TypeKind::Adt("PhysicalHashJoin".to_string()).into(),
            create_test_span(),
        );

        let converted = convert_expr(&Arc::new(physical_hash_join), &registry);

        match &converted.kind {
            ExprKind::CoreExpr(CoreData::Physical(Materializable::Materialized(physical_op))) => {
                assert_eq!(physical_op.operator.tag, "PhysicalHashJoin");
                assert_eq!(physical_op.operator.data.len(), 2); // hash_keys and build_side
                assert_eq!(physical_op.operator.children.len(), 2); // left and right

                // Check children are physical operators
                for child in &physical_op.operator.children {
                    match &child.kind {
                        ExprKind::CoreExpr(CoreData::Physical(_)) => {}
                        _ => panic!("Expected physical operator child"),
                    }
                }
            }
            _ => panic!("Expected PhysicalOp"),
        }
    }

    #[test]
    fn test_convert_logical_pattern_to_operator() {
        let registry = create_registry_with_operators();

        // Create LogicalJoin pattern
        let logical_join_pattern = Pattern::new_with(
            PatternKind::Struct(
                "LogicalJoin".to_string(),
                vec![
                    // Pattern for join_type
                    Pattern::new_with(
                        PatternKind::Wildcard,
                        TypeKind::String.into(),
                        create_test_span(),
                    ),
                    // Pattern for condition
                    Pattern::new_with(
                        PatternKind::Literal(Literal::String("a = b".to_string())),
                        TypeKind::String.into(),
                        create_test_span(),
                    ),
                    // Pattern for left child
                    Pattern::new_with(
                        PatternKind::Wildcard,
                        TypeKind::Adt("LogicalScan".to_string()).into(),
                        create_test_span(),
                    ),
                    // Pattern for right child
                    Pattern::new_with(
                        PatternKind::Bind(
                            "right_scan".to_string(),
                            Box::new(Pattern::new_with(
                                PatternKind::Wildcard,
                                TypeKind::Adt("LogicalScan".to_string()).into(),
                                create_test_span(),
                            )),
                        ),
                        TypeKind::Adt("LogicalScan".to_string()).into(),
                        create_test_span(),
                    ),
                ],
            ),
            TypeKind::Adt("LogicalJoin".to_string()).into(),
            create_test_span(),
        );

        let converted = convert_pattern(&logical_join_pattern, &registry);

        match &converted.kind {
            PatternKind::Operator(operator) => {
                assert_eq!(operator.tag, "LogicalJoin");
                assert_eq!(operator.data.len(), 2); // join_type and condition
                assert_eq!(operator.children.len(), 2); // left and right

                // Check that data patterns are in correct order
                match &operator.data[0].kind {
                    PatternKind::Wildcard => {}
                    _ => panic!("Expected wildcard pattern for join_type"),
                }
                match &operator.data[1].kind {
                    PatternKind::Literal(Literal::String(s)) => {
                        assert_eq!(s, "a = b");
                    }
                    _ => panic!("Expected literal pattern for condition"),
                }

                // Check that children patterns are in correct order
                match &operator.children[0].kind {
                    PatternKind::Wildcard => {}
                    _ => panic!("Expected wildcard pattern for left child"),
                }
                match &operator.children[1].kind {
                    PatternKind::Bind(name, _) => {
                        assert_eq!(name, "right_scan");
                    }
                    _ => panic!("Expected bind pattern for right child"),
                }
            }
            _ => panic!("Expected Operator pattern"),
        }
    }

    #[test]
    fn test_convert_normal_struct_remains_unchanged() {
        let mut registry = TypeRegistry::new();

        // Create a normal struct that doesn't inherit from Logical/Physical
        let person = create_product_adt(
            "Person",
            vec![("name", AstType::String), ("age", AstType::Int64)],
        );
        registry.register_adt(&person).unwrap();

        // Create Person struct
        let person_expr = Expr::new_with(
            ExprKind::CoreExpr(CoreData::Struct(
                "Person".to_string(),
                vec![
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(CoreData::Literal(Literal::String("Alice".to_string()))),
                        TypeKind::String.into(),
                        create_test_span(),
                    )),
                    Arc::new(Expr::new_with(
                        ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(30))),
                        TypeKind::I64.into(),
                        create_test_span(),
                    )),
                ],
            )),
            TypeKind::Adt("Person".to_string()).into(),
            create_test_span(),
        );

        let converted = convert_expr(&Arc::new(person_expr), &registry);

        // Should remain a struct, not converted to operator
        match &converted.kind {
            ExprKind::CoreExpr(CoreData::Struct(name, fields)) => {
                assert_eq!(name, "Person");
                assert_eq!(fields.len(), 2);
            }
            _ => panic!("Expected struct to remain unchanged"),
        }
    }

    #[test]
    fn test_complex_nested_tuple_access() {
        let registry = TypeRegistry::new();

        // Create a complex nested tuple expression ((1, "a"), (true, 2.5))
        let inner_tuple1 = Expr::new_with(
            ExprKind::CoreExpr(CoreData::Tuple(vec![
                Arc::new(Expr::new_with(
                    ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(1))),
                    TypeKind::I64.into(),
                    create_test_span(),
                )),
                Arc::new(Expr::new_with(
                    ExprKind::CoreExpr(CoreData::Literal(Literal::String("a".to_string()))),
                    TypeKind::String.into(),
                    create_test_span(),
                )),
            ])),
            TypeKind::Tuple(vec![TypeKind::I64.into(), TypeKind::String.into()]).into(),
            create_test_span(),
        );

        let inner_tuple2 = Expr::new_with(
            ExprKind::CoreExpr(CoreData::Tuple(vec![
                Arc::new(Expr::new_with(
                    ExprKind::CoreExpr(CoreData::Literal(Literal::Bool(true))),
                    TypeKind::Bool.into(),
                    create_test_span(),
                )),
                Arc::new(Expr::new_with(
                    ExprKind::CoreExpr(CoreData::Literal(Literal::Float64(2.5))),
                    TypeKind::F64.into(),
                    create_test_span(),
                )),
            ])),
            TypeKind::Tuple(vec![TypeKind::Bool.into(), TypeKind::F64.into()]).into(),
            create_test_span(),
        );

        let outer_tuple = Expr::new_with(
            ExprKind::CoreExpr(CoreData::Tuple(vec![
                Arc::new(inner_tuple1),
                Arc::new(inner_tuple2),
            ])),
            TypeKind::Tuple(vec![
                TypeKind::Tuple(vec![TypeKind::I64.into(), TypeKind::String.into()]).into(),
                TypeKind::Tuple(vec![TypeKind::Bool.into(), TypeKind::F64.into()]).into(),
            ])
            .into(),
            create_test_span(),
        );

        // Access the second element of the first inner tuple: outer._0._1 (should be "a")
        let field_access1 = Expr::new_with(
            ExprKind::FieldAccess(
                Arc::new(Expr::new_with(
                    ExprKind::FieldAccess(Arc::new(outer_tuple.clone()), "_0".to_string()),
                    TypeKind::Tuple(vec![TypeKind::I64.into(), TypeKind::String.into()]).into(),
                    create_test_span(),
                )),
                "_1".to_string(),
            ),
            TypeKind::String.into(),
            create_test_span(),
        );

        let converted1 = convert_expr(&Arc::new(field_access1), &registry);

        match &converted1.kind {
            ExprKind::Call(func, args) => {
                assert_eq!(args.len(), 1);

                // Check that the index is 1
                match &args[0].kind {
                    ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(index))) => {
                        assert_eq!(*index, 1);
                    }
                    _ => panic!("Expected literal index"),
                }

                // Check that the function is itself a call (nested field access)
                match &func.kind {
                    ExprKind::Call(_, inner_args) => {
                        assert_eq!(inner_args.len(), 1);

                        // Check that the inner index is 0
                        match &inner_args[0].kind {
                            ExprKind::CoreExpr(CoreData::Literal(Literal::Int64(index))) => {
                                assert_eq!(*index, 0);
                            }
                            _ => panic!("Expected literal index"),
                        }
                    }
                    _ => panic!("Expected nested call"),
                }
            }
            _ => panic!("Expected call expression"),
        }
    }
}
