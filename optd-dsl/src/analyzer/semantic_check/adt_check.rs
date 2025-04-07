use super::error::SemanticErrorKind;
use crate::analyzer::types::{Type, TypeRegistry};
use std::collections::HashSet;

/// Performs a cycle check on ADT definitions to prevent non-terminating types.
///
/// In a type system, infinite recursive types can lead to non-terminating behavior at runtime.
/// This check distinguishes between:
/// - Invalid cycles: Where a type directly or indirectly contains itself with no way to terminate
///   (e.g. `type Node(next: Node)`).
/// - Valid recursion: Where a sum type has at least one variant that doesn't reference itself
///   (e.g. `type List = Nil | Cons(value: Int64, next: List)`).
///
/// The algorithm uses depth-first search to detect cycles in the type structure while
/// applying special handling for sum types that may have valid recursive variants.
pub fn check_adt_cycles(registry: &TypeRegistry) -> Result<(), SemanticErrorKind> {
    registry.subtypes.keys().try_for_each(|adt_name| {
        // For sum types, we need to check if at least one variant doesn't cause a cycle.
        if is_sum_type_with_variants(adt_name, registry) {
            check_sum_type(adt_name, registry)
        } else {
            // For product types and empty sum types, check directly for cycles.
            check_product_type(adt_name, registry)
        }
    })
}

/// Checks if the named type is a sum type with one or more variants.
fn is_sum_type_with_variants(type_name: &str, registry: &TypeRegistry) -> bool {
    registry
        .subtypes
        .get(type_name)
        .map_or(false, |variants| !variants.is_empty())
}

/// Checks a sum type for valid recursion, requiring at least one terminating variant.
fn check_sum_type(sum_type: &str, registry: &TypeRegistry) -> Result<(), SemanticErrorKind> {
    let has_terminating_variant = registry.subtypes[sum_type].iter().any(|variant| {
        !variant_references_type_recursively(variant, sum_type, registry, &mut HashSet::new())
    });

    if !has_terminating_variant {
        let span = registry.spans.get(sum_type).cloned().unwrap();
        return Err(SemanticErrorKind::new_cyclic_adt(
            sum_type.to_string(),
            span,
        ));
    }

    Ok(())
}

/// Checks a product type for cycles in its field references.
fn check_product_type(type_name: &str, registry: &TypeRegistry) -> Result<(), SemanticErrorKind> {
    let mut visited = HashSet::new();
    let mut path = Vec::new();

    if contains_cycle(type_name, registry, &mut visited, &mut path) {
        let span = registry.spans.get(type_name).cloned().unwrap();
        return Err(SemanticErrorKind::new_cyclic_adt(
            type_name.to_string(),
            span,
        ));
    }

    Ok(())
}

/// Performs a depth-first search to detect cycles in an ADT's field references.
///
/// This function tracks the current path of ADTs we're examining to detect
/// both direct self-references and indirect cycles that span multiple types.
/// A cycle exists if we encounter an ADT that we're already in the process of checking.
fn contains_cycle(
    adt_name: &str,
    registry: &TypeRegistry,
    visited: &mut HashSet<String>,
    path: &mut Vec<String>,
) -> bool {
    if visited.contains(adt_name) {
        path.push(adt_name.to_string());
        return true;
    }

    visited.insert(adt_name.to_string());
    path.push(adt_name.to_string());

    // Use a scope to ensure we remove the current ADT from tracking regardless of return path.
    let has_cycle = registry
        .product_fields
        .get(adt_name)
        .map_or(false, |fields| {
            fields
                .iter()
                .any(|field| type_contains_adt_cycle(&field.ty.0, registry, visited, path))
        });

    // Backtrack.
    visited.remove(adt_name);
    path.pop();

    has_cycle
}

/// Recursively checks if a type contains an ADT reference that forms a cycle.
///
/// This function handles complex nested types (arrays, tuples, etc.) by
/// recursively examining their component types for potential cycles.
fn type_contains_adt_cycle(
    ty: &Type,
    registry: &TypeRegistry,
    visited: &mut HashSet<String>,
    path: &mut Vec<String>,
) -> bool {
    use Type::*;

    match ty {
        Adt(name) if registry.subtypes.contains_key(name) => {
            contains_cycle(name, registry, visited, path)
        }

        Array(elem_type) => type_contains_adt_cycle(elem_type, registry, visited, path),

        Tuple(types) => types
            .iter()
            .any(|t| type_contains_adt_cycle(t, registry, visited, path)),

        Map(key_type, val_type) => {
            type_contains_adt_cycle(key_type, registry, visited, path)
                || type_contains_adt_cycle(val_type, registry, visited, path)
        }

        Closure(param_type, return_type) => {
            type_contains_adt_cycle(param_type, registry, visited, path)
                || type_contains_adt_cycle(return_type, registry, visited, path)
        }

        Optional(inner_type) => type_contains_adt_cycle(inner_type, registry, visited, path),

        Stored(inner_type) | Costed(inner_type) => {
            type_contains_adt_cycle(inner_type, registry, visited, path)
        }

        // Primitive types cannot form cycles.
        _ => false,
    }
}

/// Checks if a variant ADT transitively references a target type through its fields.
///
/// This function examines if the variant contains any direct or indirect references
/// to the parent sum type, which would make it non-terminating if used alone.
fn variant_references_type_recursively(
    variant: &str,
    target_type: &str,
    registry: &TypeRegistry,
    visited: &mut HashSet<String>,
) -> bool {
    if visited.contains(variant) {
        return false;
    }

    visited.insert(variant.to_string());

    registry
        .product_fields
        .get(variant)
        .map_or(false, |fields| {
            fields.iter().any(|field| {
                type_references_target_recursively(&field.ty.0, target_type, registry, visited)
            })
        })
}

/// Determines if a type directly or indirectly references a specific target type.
///
/// This function recursively examines complex types to find any references to
/// the target type, handling nested structures like arrays, tuples, and maps.
fn type_references_target_recursively(
    ty: &Type,
    target_type: &str,
    registry: &TypeRegistry,
    visited: &mut HashSet<String>,
) -> bool {
    use Type::*;

    match ty {
        // Direct reference to the target type.
        Adt(name) if name == target_type => true,

        // For other ADTs, check their fields recursively.
        Adt(name) => {
            registry.product_fields.contains_key(name)
                && variant_references_type_recursively(name, target_type, registry, visited)
        }

        // Handle nested types recursively.
        Array(elem_type) => {
            type_references_target_recursively(elem_type, target_type, registry, visited)
        }

        Tuple(types) => types
            .iter()
            .any(|t| type_references_target_recursively(t, target_type, registry, visited)),

        Map(key_type, val_type) => {
            type_references_target_recursively(key_type, target_type, registry, visited)
                || type_references_target_recursively(val_type, target_type, registry, visited)
        }

        Closure(param_type, return_type) => {
            type_references_target_recursively(param_type, target_type, registry, visited)
                || type_references_target_recursively(return_type, target_type, registry, visited)
        }

        Optional(inner_type) => {
            type_references_target_recursively(inner_type, target_type, registry, visited)
        }

        Stored(inner_type) | Costed(inner_type) => {
            type_references_target_recursively(inner_type, target_type, registry, visited)
        }

        // Primitive types don't reference other types.
        _ => false,
    }
}

#[cfg(test)]
mod adt_cycle_tests {
    use super::*;
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
        let result = check_adt_cycles(&registry);

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
        let result = check_adt_cycles(&registry);

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
        let result = check_adt_cycles(&registry);

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
        let result = check_adt_cycles(&registry);

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
        let result = check_adt_cycles(&registry);

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

        let registry = setup_test_registry(HashMap::new(), product_fields);
        let result = check_adt_cycles(&registry);

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
        //   | PhysJoin =
        //       | HashJoin(build_side: Physical, probe_side: Physical, typ: String, cond: Predicate)
        //       | MergeJoin(left: Physical, right: Physical, typ: String, cond: Predicate)
        //       \ NestedLoopJoin(outer: Physical, inner: Physical, typ: String, cond: Predicate)

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
        let result = check_adt_cycles(&registry);

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
        let result = check_adt_cycles(&registry);

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
        let result = check_adt_cycles(&registry);

        assert!(result.is_ok()); // Valid because Literal is a terminating variant for Expr
    }

    #[test]
    fn test_deep_nesting_pattern() {
        // Test a structure with several levels of nested sum types:
        //
        // type A =
        //   | B
        //   | C =
        //     | D
        //     | E =
        //       | F(a: A)
        //       | G

        // Set up type hierarchy
        let mut subtypes = HashMap::new();

        // A has variants B and C
        let mut a_variants = HashSet::new();
        a_variants.insert("B".to_string());
        a_variants.insert("C".to_string());
        subtypes.insert("A".to_string(), a_variants);

        // C has variants D and E
        let mut c_variants = HashSet::new();
        c_variants.insert("D".to_string());
        c_variants.insert("E".to_string());
        subtypes.insert("C".to_string(), c_variants);

        // E has variants F and G
        let mut e_variants = HashSet::new();
        e_variants.insert("F".to_string());
        e_variants.insert("G".to_string());
        subtypes.insert("E".to_string(), e_variants);

        // Set up fields
        let mut product_fields = HashMap::new();

        // Only F has a field, the rest are empty products
        product_fields.insert("B".to_string(), vec![]);
        product_fields.insert("D".to_string(), vec![]);
        product_fields.insert("G".to_string(), vec![]);

        // F references back to A
        product_fields.insert(
            "F".to_string(),
            vec![create_field("a", Type::Adt("A".to_string()))],
        );

        let registry = setup_test_registry(subtypes, product_fields);
        let result = check_adt_cycles(&registry);

        assert!(result.is_ok()); // Valid because B, D, G are terminating variants
    }
}
