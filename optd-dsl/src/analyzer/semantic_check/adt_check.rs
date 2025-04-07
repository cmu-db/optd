use super::error::SemanticErrorKind;
use crate::{
    analyzer::{
        hir::Identifier,
        types::{Type, TypeRegistry},
    },
    utils::span::Span,
};
use std::collections::HashMap;

/// Exploration status for ADT cycle detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExplorationStatus {
    /// Currently exploring this node.
    Exploring,
    /// Finished exploring, this type terminates.
    Terminates,
}

pub fn adt_check(registry: &TypeRegistry) -> Result<(), SemanticErrorKind> {
    // First, check if types correctly reference each other and do not
    // create any infinite recursive cycles.
    let mut exploration_status = HashMap::new();

    for adt_name in registry.product_fields.keys() {
        if !can_terminate(adt_name.clone(), registry, &mut exploration_status)? {
            let span = registry.spans.get(adt_name).cloned().unwrap();
            return Err(SemanticErrorKind::new_cyclic_adt(adt_name.clone(), span));
        }

        // Clear all "Exploring" statuses before checking the next type.
        exploration_status.retain(|_, status| *status != ExplorationStatus::Exploring);
    }

    Ok(())
}

/// Determines if a type exists and can terminate (no infinite recursion).
/// Returns true if terminates, false if cycle detected.
fn can_terminate(
    adt: Identifier,
    registry: &TypeRegistry,
    exploration_status: &mut HashMap<String, ExplorationStatus>,
) -> Result<bool, SemanticErrorKind> {
    use ExplorationStatus::*;

    match exploration_status.get(&adt) {
        Some(&Terminates) => return Ok(true),
        Some(&Exploring) => return Ok(false), // Cycle detected.
        None => {}
    }
    exploration_status.insert(adt.clone(), Exploring);

    let terminates = if registry.product_fields.contains_key(&adt) {
        // Product type: all fields must terminate.
        check_product_type_terminates(adt.clone(), registry, exploration_status)?
    } else {
        // Sum type: one variant must terminate.
        check_sum_type_terminates(adt.clone(), registry, exploration_status)?
    };

    if terminates {
        exploration_status.insert(adt, Terminates);
    }

    Ok(terminates)
}

/// Checks if a product type terminates - all fields must terminate.
/// Returns true if all fields terminate, false otherwise.
fn check_product_type_terminates(
    adt: Identifier,
    registry: &TypeRegistry,
    exploration_status: &mut HashMap<String, ExplorationStatus>,
) -> Result<bool, SemanticErrorKind> {
    let fields = registry.product_fields.get(&adt).unwrap();
    for field in fields {
        if !check_field_type_terminates(&field.ty.0, &field.ty.1, registry, exploration_status)? {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Checks if a sum type terminates - at least one variant must terminate
/// Returns true if at least one variant terminates, false otherwise.
fn check_sum_type_terminates(
    sum_type: Identifier,
    registry: &TypeRegistry,
    exploration_status: &mut HashMap<String, ExplorationStatus>,
) -> Result<bool, SemanticErrorKind> {
    let variants = registry.subtypes.get(&sum_type).unwrap();
    for variant in variants {
        if can_terminate(variant.clone(), registry, exploration_status)? {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Checks if a type (including complex types like Array, Map, etc.) terminates.
/// Returns true if type terminates, false if a cycle is detected.
fn check_field_type_terminates(
    ty: &Type,
    span: &Span,
    registry: &TypeRegistry,
    exploration_status: &mut HashMap<String, ExplorationStatus>,
) -> Result<bool, SemanticErrorKind> {
    use Type::*;

    match ty {
        Adt(name) => {
            // Check if the type exists.
            // Only field types may not exist, since we have the guaranteed that all
            // sum types have been added to the registry during the conversion phase.
            if !registry.subtypes.contains_key(name) {
                return Err(SemanticErrorKind::new_undefined_type(
                    name.clone(),
                    span.clone(),
                ));
            }

            can_terminate(name.clone(), registry, exploration_status)
        }

        Array(elem_type) => {
            check_field_type_terminates(elem_type, span, registry, exploration_status)
        }

        Tuple(types) => types.iter().try_fold(true, |acc, t| {
            Ok(acc && check_field_type_terminates(t, span, registry, exploration_status)?)
        }),

        Map(key_type, val_type) => {
            if !check_field_type_terminates(key_type, span, registry, exploration_status)? {
                return Ok(false);
            }
            check_field_type_terminates(val_type, span, registry, exploration_status)
        }

        Closure(param_type, return_type) => {
            if !check_field_type_terminates(param_type, span, registry, exploration_status)? {
                return Ok(false);
            }
            check_field_type_terminates(return_type, span, registry, exploration_status)
        }

        Optional(inner_type) | Stored(inner_type) | Costed(inner_type) => {
            check_field_type_terminates(inner_type, span, registry, exploration_status)
        }

        _ => Ok(true), // Primitive types always terminate
    }
}

#[cfg(test)]
mod adt_cycle_tests {
    use crate::analyzer::semantic_check::adt_check::adt_check;
    use crate::analyzer::types::{AdtField, Type, TypeRegistry};
    use crate::utils::span::Span;
    use std::collections::{HashMap, HashSet};

    fn create_test_span() -> Span {
        Span::new("test".to_string(), 0..1)
    }

    // Helper to create a TypeRegistry with predefined types for testing
    fn setup_test_registry(
        subtypes: HashMap<String, HashSet<String>>,
        product_fields: HashMap<String, Vec<AdtField>>,
    ) -> TypeRegistry {
        // Populate spans for all mentioned types
        let mut spans = HashMap::new();
        for name in subtypes.keys() {
            spans.insert(name.clone(), create_test_span());
        }
        for name in product_fields.keys() {
            spans.insert(name.clone(), create_test_span());
        }

        TypeRegistry {
            spans,
            subtypes,
            product_fields,
        }
    }

    // Helper to create an AdtField
    fn create_field(name: &str, ty: Type) -> AdtField {
        AdtField {
            name: (name.to_string(), create_test_span()),
            ty: (ty, create_test_span()),
        }
    }

    #[test]
    fn test_direct_recursion_in_product() {
        // Test a direct recursive product type: type Node(value: Int64, next: Node)
        let mut product_fields = HashMap::new();
        product_fields.insert(
            "Node".to_string(),
            vec![
                create_field("value", Type::Int64),
                create_field("next", Type::Adt("Node".to_string())),
            ],
        );

        let mut subtypes = HashMap::new();
        subtypes.insert("Node".to_string(), HashSet::new());

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err());
    }

    #[test]
    fn test_valid_recursive_sum_type() {
        // Test a valid recursive sum type with a base case:
        // type List =
        //   | Nil
        //   | Cons(value: Int64, next: List)

        // Set up subtypes
        let mut subtypes = HashMap::new();
        let mut list_variants = HashSet::new();
        list_variants.insert("Nil".to_string());
        list_variants.insert("Cons".to_string());
        subtypes.insert("List".to_string(), list_variants);

        // Set up fields
        let mut product_fields = HashMap::new();
        product_fields.insert("Nil".to_string(), vec![]);
        product_fields.insert(
            "Cons".to_string(),
            vec![
                create_field("value", Type::Int64),
                create_field("next", Type::Adt("List".to_string())),
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_recursive_sum_type() {
        // Test an invalid recursive sum type without a base case:
        // type BadList =
        //   | ConsA(next: BadList)
        //   | ConsB(next: BadList)

        // Set up subtypes
        let mut subtypes = HashMap::new();
        let mut bad_list_variants = HashSet::new();
        bad_list_variants.insert("ConsA".to_string());
        bad_list_variants.insert("ConsB".to_string());
        subtypes.insert("BadList".to_string(), bad_list_variants);

        // Set up fields
        let mut product_fields = HashMap::new();
        product_fields.insert(
            "ConsA".to_string(),
            vec![create_field("next", Type::Adt("BadList".to_string()))],
        );
        product_fields.insert(
            "ConsB".to_string(),
            vec![create_field("next", Type::Adt("BadList".to_string()))],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err());
    }

    #[test]
    fn test_indirect_recursion() {
        // Test indirect recursion:
        // type A(b: B)
        // type B(a: A)

        let mut product_fields = HashMap::new();
        product_fields.insert(
            "A".to_string(),
            vec![create_field("b", Type::Adt("B".to_string()))],
        );
        product_fields.insert(
            "B".to_string(),
            vec![create_field("a", Type::Adt("A".to_string()))],
        );

        let mut subtypes = HashMap::new();
        subtypes.insert("A".to_string(), HashSet::new());
        subtypes.insert("B".to_string(), HashSet::new());

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err());
    }

    #[test]
    fn test_nested_type_recursion() {
        // Test recursion in nested types:
        // type Nested(arr: Array[Nested])

        let mut product_fields = HashMap::new();
        product_fields.insert(
            "Nested".to_string(),
            vec![create_field(
                "arr",
                Type::Array(Box::new(Type::Adt("Nested".to_string()))),
            )],
        );

        let mut subtypes = HashMap::new();
        subtypes.insert("Nested".to_string(), HashSet::new());

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err());
    }

    #[test]
    fn test_non_recursive_types() {
        // Test non-recursive types:
        // type Point(x: Int64, y: Int64)
        // type Rectangle(topLeft: Point, bottomRight: Point)

        let mut product_fields = HashMap::new();
        product_fields.insert(
            "Point".to_string(),
            vec![
                create_field("x", Type::Int64),
                create_field("y", Type::Int64),
            ],
        );
        product_fields.insert(
            "Rectangle".to_string(),
            vec![
                create_field("topLeft", Type::Adt("Point".to_string())),
                create_field("bottomRight", Type::Adt("Point".to_string())),
            ],
        );

        let mut subtypes = HashMap::new();
        subtypes.insert("Point".to_string(), HashSet::new());
        subtypes.insert("Rectangle".to_string(), HashSet::new());

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_ok());
    }

    #[test]
    fn test_multi_level_nested_adt() {
        // This test replicates a complex multi-level ADT structure similar to:
        //
        // type Physical =
        //   | Scan(table_name: String)  // Terminating variant
        //   | PhysFilter(child: Physical, cond: Predicate)
        //   | PhysProject(child: Physical, exprs: [Scalar])
        //   \ PhysJoin =
        //       | HashJoin(build_side: Physical, probe_side: Physical, typ: String, cond: Predicate)
        //       | MergeJoin(left: Physical, right: Physical, typ: String, cond: Predicate)
        //       \ NestedLoopJoin(outer: Physical, inner: Physical, typ: String, cond: Predicate)
        //
        // type Predicate =
        //   \ Equals(left: Scalar, right: Scalar)  // Terminating variant
        //
        // type Scalar =
        //   \ Literal(value: Int64)  // Terminating variant

        // Set up the type hierarchy
        let mut subtypes = HashMap::new();

        // Physical has 4 variants
        let mut physical_variants = HashSet::new();
        physical_variants.insert("Scan".to_string());
        physical_variants.insert("PhysFilter".to_string());
        physical_variants.insert("PhysProject".to_string());
        physical_variants.insert("PhysJoin".to_string());
        subtypes.insert("Physical".to_string(), physical_variants);

        // PhysJoin has 3 variants
        let mut phys_join_variants = HashSet::new();
        phys_join_variants.insert("HashJoin".to_string());
        phys_join_variants.insert("MergeJoin".to_string());
        phys_join_variants.insert("NestedLoopJoin".to_string());
        subtypes.insert("PhysJoin".to_string(), phys_join_variants);

        // Set up basic types for Predicate and Scalar
        let mut predicate_variants = HashSet::new();
        predicate_variants.insert("Equals".to_string());
        subtypes.insert("Predicate".to_string(), predicate_variants);

        let mut scalar_variants = HashSet::new();
        scalar_variants.insert("Literal".to_string());
        subtypes.insert("Scalar".to_string(), scalar_variants);

        // Set up the field definitions
        let mut product_fields = HashMap::new();

        // Terminating variants
        product_fields.insert(
            "Scan".to_string(),
            vec![create_field("table_name", Type::String)],
        );

        product_fields.insert(
            "Literal".to_string(),
            vec![create_field("value", Type::Int64)],
        );

        product_fields.insert(
            "Equals".to_string(),
            vec![
                create_field("left", Type::Adt("Scalar".to_string())),
                create_field("right", Type::Adt("Scalar".to_string())),
            ],
        );

        // Recursive variants
        product_fields.insert(
            "PhysFilter".to_string(),
            vec![
                create_field("child", Type::Adt("Physical".to_string())),
                create_field("cond", Type::Adt("Predicate".to_string())),
            ],
        );

        product_fields.insert(
            "PhysProject".to_string(),
            vec![
                create_field("child", Type::Adt("Physical".to_string())),
                create_field(
                    "exprs",
                    Type::Array(Box::new(Type::Adt("Scalar".to_string()))),
                ),
            ],
        );

        // PhysJoin variants
        product_fields.insert(
            "HashJoin".to_string(),
            vec![
                create_field("build_side", Type::Adt("Physical".to_string())),
                create_field("probe_side", Type::Adt("Physical".to_string())),
                create_field("typ", Type::String),
                create_field("cond", Type::Adt("Predicate".to_string())),
            ],
        );

        product_fields.insert(
            "MergeJoin".to_string(),
            vec![
                create_field("left", Type::Adt("Physical".to_string())),
                create_field("right", Type::Adt("Physical".to_string())),
                create_field("typ", Type::String),
                create_field("cond", Type::Adt("Predicate".to_string())),
            ],
        );

        product_fields.insert(
            "NestedLoopJoin".to_string(),
            vec![
                create_field("outer", Type::Adt("Physical".to_string())),
                create_field("inner", Type::Adt("Physical".to_string())),
                create_field("typ", Type::String),
                create_field("cond", Type::Adt("Predicate".to_string())),
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_ok()); // Valid because Scan is a terminating variant
    }

    #[test]
    fn test_nested_list_with_cycles() {
        // This test represents the structure:
        //
        // type A =
        //  \ List =
        //  | Bla(b: List)
        //  \ Vla(a: List)

        // Set up type hierarchy
        let mut subtypes = HashMap::new();

        // A has List as its only variant
        let mut a_variants = HashSet::new();
        a_variants.insert("List".to_string());
        subtypes.insert("A".to_string(), a_variants);

        // List has two variants: Bla and Vla
        let mut list_variants = HashSet::new();
        list_variants.insert("Bla".to_string());
        list_variants.insert("Vla".to_string());
        subtypes.insert("List".to_string(), list_variants);

        // Set up fields
        let mut product_fields = HashMap::new();
        product_fields.insert(
            "Bla".to_string(),
            vec![create_field("b", Type::Adt("List".to_string()))],
        );
        product_fields.insert(
            "Vla".to_string(),
            vec![create_field("a", Type::Adt("List".to_string()))],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err()); // Should detect the cycle
    }

    #[test]
    fn test_complex_expression_language() {
        // Create a simplified expression language:
        //
        // type Expr =
        //   | Literal(value: Int64)
        //   | BinOp(left: Expr, right: Expr, op: String)
        //   | Condition(cond: Predicate)
        //
        // type Predicate =
        //   | Equals(left: Expr, right: Expr)
        //   | And(left: Predicate, right: Predicate)

        // Set up type hierarchy
        let mut subtypes = HashMap::new();

        // Expr variants
        let mut expr_variants = HashSet::new();
        expr_variants.insert("Literal".to_string());
        expr_variants.insert("BinOp".to_string());
        expr_variants.insert("Condition".to_string());
        subtypes.insert("Expr".to_string(), expr_variants);

        // Predicate variants
        let mut predicate_variants = HashSet::new();
        predicate_variants.insert("Equals".to_string());
        predicate_variants.insert("And".to_string());
        subtypes.insert("Predicate".to_string(), predicate_variants);

        // Set up fields
        let mut product_fields = HashMap::new();

        // Terminating variant for Expr
        product_fields.insert(
            "Literal".to_string(),
            vec![create_field("value", Type::Int64)],
        );

        // Recursive variants
        product_fields.insert(
            "BinOp".to_string(),
            vec![
                create_field("left", Type::Adt("Expr".to_string())),
                create_field("right", Type::Adt("Expr".to_string())),
                create_field("op", Type::String),
            ],
        );

        product_fields.insert(
            "Condition".to_string(),
            vec![create_field("cond", Type::Adt("Predicate".to_string()))],
        );

        product_fields.insert(
            "Equals".to_string(),
            vec![
                create_field("left", Type::Adt("Expr".to_string())),
                create_field("right", Type::Adt("Expr".to_string())),
            ],
        );

        product_fields.insert(
            "And".to_string(),
            vec![
                create_field("left", Type::Adt("Predicate".to_string())),
                create_field("right", Type::Adt("Predicate".to_string())),
            ],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_ok()); // Valid because Literal is a terminating variant for Expr
    }

    #[test]
    fn test_nested_sum_only_recursive() {
        // Test a structure with nested sum types where all paths lead to recursion:
        //
        // type A =
        //   | B =
        //     | C(a: A)

        // Set up type hierarchy
        let mut subtypes = HashMap::new();

        // A has B as its only variant
        let mut a_variants = HashSet::new();
        a_variants.insert("B".to_string());
        subtypes.insert("A".to_string(), a_variants);

        // B has C as its only variant
        let mut b_variants = HashSet::new();
        b_variants.insert("C".to_string());
        subtypes.insert("B".to_string(), b_variants);

        // Set up fields
        let mut product_fields = HashMap::new();

        // C references back to A, creating a cycle
        product_fields.insert(
            "C".to_string(),
            vec![create_field("a", Type::Adt("A".to_string()))],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = adt_check(&registry);

        assert!(result.is_err()); // Should detect the cycle and fail
    }
}
