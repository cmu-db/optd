use super::registry::{LOGICAL_TYPE, PHYSICAL_TYPE, Type, TypeRegistry};
use crate::dsl::analyzer::type_checks::registry::{Generic, TypeKind};
use std::collections::HashSet;

impl TypeRegistry {
    /// Checks if one type is a subtype of another, potentially refining unknown types.
    ///
    /// This method determines if `child` is a subtype of `parent` according to
    /// the language's type system rules. It handles primitive types, complex types
    /// with covariant and contravariant relationships, and user-defined ADTs.
    ///
    /// During type inference, this method can refine unknown types to satisfy
    /// subtyping constraints according to their variance:
    /// - When an `UnknownAsc` type is encountered as a parent, it is updated to the
    ///   least upper bound (LUB) of itself and the child type. These types start
    ///   at `Nothing` and ascend up the type hierarchy as needed.
    /// - When an `UnknownDesc` type is encountered as a child, it is updated to the
    ///   greatest lower bound (GLB) of itself and the parent type. These types start
    ///   at `Universe` and descend down the type hierarchy as needed.
    /// - When an `UnknownAsc` type is encountered as a child, its resolved type is
    ///   checked against the parent type.
    /// - When an `UnknownDesc` type is encountered as a parent, its resolved type is
    ///   checked against the child type.
    ///
    /// The method iteratively performs these checks until it reaches a stable state
    /// where no more unknown types can be refined.
    ///
    /// # Arguments
    ///
    /// * `child` - The potential subtype.
    /// * `parent` - The potential supertype.
    /// * `has_changed` - Mutable reference to a boolean that will be set to true if
    ///   any unknown types were modified to establish the subtyping relationship.
    ///
    /// # Returns
    ///
    /// * `true` if `child` is a subtype of `parent` (possibly after refinements).
    /// * `false` if `child` cannot be made a subtype of `parent`.
    pub(super) fn is_subtype_infer(
        &mut self,
        child: &Type,
        parent: &Type,
        has_changed: &mut bool,
    ) -> bool {
        // Continue checking until we reach stability (no more type updates).
        // Stop if we've reached a stable state.
        loop {
            let mut local_changed = false;
            let is_subtype =
                self.is_subtype_inner(child, parent, &mut HashSet::new(), &mut local_changed);

            // Update the outer has_changed flag if we made changes.
            *has_changed |= local_changed;

            if !local_changed {
                return is_subtype;
            }
        }
    }

    /// Simple wrapper for is_subtype_infer that ignores the has_changed parameter.
    /// Useful for testing and cases where tracking changes isn't needed.
    pub(crate) fn is_subtype(&mut self, child: &Type, parent: &Type) -> bool {
        let mut has_changed = false;
        self.is_subtype_infer(child, parent, &mut has_changed)
    }

    /// Inner implementation of is_subtype_infer with memoization for cycle detection.
    /// Cycles can occur in the type hierarchy with recursive ADTs and EqHash
    /// checks ADTs recursively deep.
    fn is_subtype_inner(
        &mut self,
        child: &Type,
        parent: &Type,
        memo: &mut HashSet<(Type, Type)>,
        has_changed: &mut bool,
    ) -> bool {
        use TypeKind::*;

        // If we've already visited the pair (child, parent).
        if !memo.insert((child.clone(), parent.clone())) {
            return true;
        }

        if child.value == parent.value {
            return true;
        }

        match (&*child.value, &*parent.value) {
            // Universe is the top type - everything is a subtype of Universe.
            (_, Universe) => true,

            // Nothing is the bottom type - it is a subtype of everything.
            (Nothing, _) => true,

            // Here we could take GLB or LUB. For now we take the GLB.
            // Most likely the best approach would be to postpone potential
            // type bumps & dumps here until absolutely no other progress can
            // be made. This would lead to better inference.
            (UnknownDesc(id_desc), UnknownAsc(_)) => {
                let child = self.resolve_type(child);
                let parent = self.resolve_type(parent);
                let glb = self.greatest_lower_bound(&child, &parent, has_changed);
                *has_changed |= self.update_unknown_if_changed(id_desc, &child, glb);
                true
            }

            // Update an unknown ascending parent by computing LUB with child.
            (_, UnknownAsc(id)) => {
                let parent = self.resolve_type(parent);
                let lub = self.least_upper_bound(child, &parent, has_changed);
                *has_changed |= self.update_unknown_if_changed(id, &parent, lub);
                true
            }

            // Update an unknown descending child by computing GLB with parent.
            (UnknownDesc(id), _) => {
                let child = self.resolve_type(child);
                let glb = self.greatest_lower_bound(&child, parent, has_changed);
                *has_changed |= self.update_unknown_if_changed(id, &child, glb);
                true
            }

            // Resolve unknown ascending child and recurse.
            (UnknownAsc(_), _) => {
                self.is_subtype_inner(&self.resolve_type(child), parent, memo, has_changed)
            }

            // Resolve unknown descending parent and recurse.
            (_, UnknownDesc(_)) => {
                self.is_subtype_inner(child, &self.resolve_type(parent), memo, has_changed)
            }

            // Generics only match if they have strictly the same name,
            // or if their bounds are compatible.
            (Gen(Generic(id1, bound1)), Gen(Generic(id2, bound2))) => {
                if id1 == id2 {
                    true
                } else if let (Some(b1), Some(b2)) = (bound1, bound2) {
                    self.is_subtype_inner(b1, b2, memo, has_changed)
                } else {
                    false
                }
            }

            // A type is a subtype of a bounded generic if it's a subtype of the bound,
            // and vice versa.
            (_, Gen(Generic(_, Some(bound)))) => {
                self.is_subtype_inner(child, bound, memo, has_changed)
            }
            (Gen(Generic(_, Some(bound))), _) => {
                self.is_subtype_inner(bound, parent, memo, has_changed)
            }

            // Stored and Costed type handling.
            (Stored(child_inner), Stored(parent_inner)) => {
                self.is_subtype_inner(child_inner, parent_inner, memo, has_changed)
            }
            (Costed(child_inner), Costed(parent_inner)) => {
                self.is_subtype_inner(child_inner, parent_inner, memo, has_changed)
            }
            (Costed(child_inner), Stored(parent_inner)) => {
                // Costed(A) is a subtype of Stored(A).
                self.is_subtype_inner(child_inner, parent_inner, memo, has_changed)
            }
            (Costed(child_inner), _) => {
                // Costed(A) is a subtype of A.
                self.is_subtype_inner(child_inner, parent, memo, has_changed)
            }
            (Stored(child_inner), _) => {
                // Stored(A) is a subtype of A.
                self.is_subtype_inner(child_inner, parent, memo, has_changed)
            }

            // Check transitive inheritance for ADTs.
            (Adt(child_name), Adt(parent_name)) => self.inherits_adt(child_name, parent_name),

            // Array covariance: Array[T] <: Array[U] if T <: U.
            (Array(child_elem), Array(parent_elem)) => {
                self.is_subtype_inner(child_elem, parent_elem, memo, has_changed)
            }

            // Map as a subtype of Function: Map(A, B) <: Closure(A, B?).
            (Map(key_type, val_type), Closure(param_type, ret_type)) => {
                let optional_val = Optional(val_type.clone()).into();
                self.is_subtype_inner(param_type, key_type, memo, has_changed)
                    && self.is_subtype_inner(&optional_val, ret_type, memo, has_changed)
            }

            // Array as a subtype of Function: Array(B) <: Closure(I64, B?).
            (Array(elem_type), Closure(param_type, ret_type)) if matches!(&**param_type, I64) => {
                self.is_subtype_inner(
                    &Optional(elem_type.clone()).into(),
                    ret_type,
                    memo,
                    has_changed,
                )
            }

            // Tuple covariance: (T1, T2, ...) <: (U1, U2, ...) if T1 <: U1, T2 <: U2, ...
            (Tuple(child_types), Tuple(parent_types))
                if child_types.len() == parent_types.len() =>
            {
                child_types
                    .iter()
                    .zip(parent_types.iter())
                    .all(|(c, p)| self.is_subtype_inner(c, p, memo, has_changed))
            }

            // Map covariance on values, contravariance on keys:
            // Map[K1, V1] <: Map[K2, V2] if K2 <: K1 and V1 <: V2.
            (Map(child_key, child_val), Map(parent_key, parent_val)) => {
                self.is_subtype_inner(parent_key, child_key, memo, has_changed)
                    && self.is_subtype_inner(child_val, parent_val, memo, has_changed)
            }

            // Function contravariance on args, covariance on return type:
            // (T1 -> U1) <: (T2 -> U2) if T2 <: T1 and U1 <: U2.
            (Closure(child_param, child_ret), Closure(parent_param, parent_ret)) => {
                self.is_subtype_inner(parent_param, child_param, memo, has_changed)
                    && self.is_subtype_inner(child_ret, parent_ret, memo, has_changed)
            }

            // Optional type covariance: Optional[T] <: Optional[U] if T <: U.
            (Optional(child_ty), Optional(parent_ty)) => {
                self.is_subtype_inner(child_ty, parent_ty, memo, has_changed)
            }
            // None <: Optional[Nothing].
            (None, Optional(_)) => true,
            // Likewise, T <: Optional[T].
            (_, Optional(parent_inner)) => {
                self.is_subtype_inner(child, parent_inner, memo, has_changed)
            }

            // Native trait subtyping relationships

            // Concat trait implementations.
            (String, Concat) => true,
            (Array(_), Concat) => true,
            (Map(_, _), Concat) => true,

            // EqHash trait implementations.
            (I64, EqHash) => true,
            (String, EqHash) => true,
            (Bool, EqHash) => true,
            (Unit, EqHash) => true,
            (None, EqHash) => true,
            (Optional(inner), EqHash) => {
                self.is_subtype_inner(inner, &EqHash.into(), memo, has_changed)
            }
            (Tuple(types), EqHash) => types
                .iter()
                .all(|t| self.is_subtype_inner(t, &EqHash.into(), memo, has_changed)),
            (Adt(name), EqHash) => {
                if let Some(fields) = self.product_fields.get(name).cloned() {
                    // Product ADTs with all fields satisfying EqHash also satisfy EqHash.
                    fields.iter().all(|field| {
                        let ty = self.get_product_field_type(name, &field.name).unwrap();
                        self.is_subtype_inner(&ty, &EqHash.into(), memo, has_changed)
                    })
                } else if let Some(variants) = self.subtypes.get(name).cloned() {
                    // Sum types (enums) satisfy EqHash if all variants satisfy EqHash.
                    variants.iter().all(|variant| {
                        self.is_subtype_inner(
                            &Adt(variant.clone()).into(),
                            &EqHash.into(),
                            memo,
                            has_changed,
                        )
                    })
                } else {
                    false
                }
            }

            // Arithmetic trait implementations.
            (I64, Arithmetic) => true,
            (F64, Arithmetic) => true,

            _ => false,
        }
    }

    fn update_unknown_if_changed(&mut self, id: &usize, old_type: &Type, new_type: Type) -> bool {
        if &new_type != old_type {
            self.resolved_unknown.insert(*id, new_type);
            true
        } else {
            false
        }
    }

    /// Determines if one ADT inherits from another ADT
    ///
    /// Checks if `child_name` is a subtype of `parent_name` in the type hierarchy.
    /// This includes both direct inheritance and transitive inheritance through
    /// intermediary types.
    ///
    /// # Arguments
    ///
    /// * `child_name` - The name of the potential child ADT
    /// * `parent_name` - The name of the potential parent ADT
    ///
    /// # Returns
    ///
    /// `true` if `child_name` inherits from `parent_name`, `false` otherwise
    pub(crate) fn inherits_adt(&self, child_name: &str, parent_name: &str) -> bool {
        if child_name == parent_name {
            return true;
        }
        if let Some(variants) = self.subtypes.get(parent_name) {
            variants
                .iter()
                .any(|subtype_child_name| self.inherits_adt(child_name, subtype_child_name))
        } else {
            false
        }
    }

    /// Checks if an ADT inherits from either the Logical or Physical type
    ///
    /// This is a specialized check for query plan operators, determining whether
    /// a given ADT represents a logical or physical operator in the query plan.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the ADT to check
    ///
    /// # Returns
    ///
    /// `true` if the ADT inherits from either Logical or Physical type, `false` otherwise
    pub(crate) fn is_logical_or_physical(&self, name: &str) -> bool {
        self.inherits_adt(name, LOGICAL_TYPE) || self.inherits_adt(name, PHYSICAL_TYPE)
    }

    /// Determines if a type should be treated as a child in an operator
    ///
    /// When converting structs to operators, this function determines whether a field
    /// should be categorized as a child node (logical/physical operator) or as regular
    /// data. This is important for proper operator structure conversion.
    ///
    /// # Rules
    ///
    /// * ADT types that inherit from Logical or Physical are considered children
    /// * Arrays of ADT types that inherit from Logical or Physical are considered children
    /// * All other types are considered data fields
    ///
    /// # Arguments
    ///
    /// * `ty` - The type to check
    ///
    /// # Returns
    ///
    /// `true` if the type should be treated as a child in an operator, `false` if it should be treated as data
    pub(crate) fn is_child_type(&self, ty: &Type) -> bool {
        use TypeKind::*;
        match &*ty.value {
            Adt(name) => self.is_logical_or_physical(name),
            Array(element_type) => match &*element_type.value {
                Adt(name) => self.is_logical_or_physical(name),
                _ => false,
            },
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{
        analyzer::type_checks::registry::type_registry_tests::{
            create_product_adt, create_sum_adt, spanned,
        },
        parser::ast::Type as AstType,
    };
    use TypeKind::*;

    #[test]
    fn test_stored_and_costed_types() {
        let mut reg = TypeRegistry::default();

        // Test Stored type as a subtype of the inner type
        assert!(reg.is_subtype(&Stored(I64.into()).into(), &I64.into()));

        // Test Costed type as a subtype of Stored type
        assert!(reg.is_subtype(&Costed(I64.into()).into(), &Stored(I64.into()).into()));

        // Test Costed type as a subtype of the inner type (transitivity)
        assert!(reg.is_subtype(&Costed(I64.into()).into(), &I64.into()));

        // Test Stored type covariance
        let mut adts_registry = TypeRegistry::default();
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let animals_enum = create_sum_adt("Animals", vec![animal, dog]);
        adts_registry.register_adt(&animals_enum).unwrap();

        assert!(adts_registry.is_subtype(
            &Stored(Adt("Dog".to_string()).into()).into(),
            &Stored(Adt("Animals".to_string()).into()).into()
        ));

        // Test Costed type covariance
        assert!(adts_registry.is_subtype(
            &Costed(Adt("Dog".to_string()).into()).into(),
            &Costed(Adt("Animals".to_string()).into()).into()
        ));

        // Test the inheritance relationship: Costed(Dog) <: Stored(Animals)
        assert!(adts_registry.is_subtype(
            &Costed(Adt("Dog".to_string()).into()).into(),
            &Stored(Adt("Animals".to_string()).into()).into()
        ));

        // Test nested Stored/Costed types
        assert!(adts_registry.is_subtype(
            &Stored(Costed(Adt("Dog".to_string()).into()).into()).into(),
            &Stored(Adt("Animals".to_string()).into()).into()
        ));

        // Test with Array of Stored/Costed types
        assert!(adts_registry.is_subtype(
            &Array(Costed(Adt("Dog".to_string()).into()).into()).into(),
            &Array(Stored(Adt("Animals".to_string()).into()).into()).into()
        ));
    }

    #[test]
    fn test_primitive_type_equality() {
        let mut reg = TypeRegistry::default();

        // Same primitive types should be subtypes of each other
        assert!(reg.is_subtype(&I64.into(), &I64.into()));
        assert!(reg.is_subtype(&Bool.into(), &Bool.into()));
        assert!(reg.is_subtype(&String.into(), &String.into()));
        assert!(reg.is_subtype(&F64.into(), &F64.into()));
        assert!(reg.is_subtype(&Unit.into(), &Unit.into()));
        assert!(reg.is_subtype(&Universe.into(), &Universe.into()));

        // Different primitive types should not be subtypes
        assert!(!reg.is_subtype(&I64.into(), &Bool.into()));
        assert!(!reg.is_subtype(&String.into(), &I64.into()));
        assert!(!reg.is_subtype(&F64.into(), &I64.into()));
        assert!(!reg.is_subtype(&Unit.into(), &Bool.into()));

        // All types should be subtypes of Universe
        assert!(reg.is_subtype(&I64.into(), &Universe.into()));
        assert!(reg.is_subtype(&Bool.into(), &Universe.into()));
        assert!(reg.is_subtype(&String.into(), &Universe.into()));
        assert!(reg.is_subtype(&F64.into(), &Universe.into()));
        assert!(reg.is_subtype(&Unit.into(), &Universe.into()));
    }

    #[test]
    fn test_array_subtyping() {
        let mut reg = TypeRegistry::default();

        // Same type arrays
        assert!(reg.is_subtype(&Array(I64.into()).into(), &Array(I64.into()).into()));

        // Different type arrays
        assert!(!reg.is_subtype(&Array(I64.into()).into(), &Array(Bool.into()).into()));

        // Test nested arrays
        assert!(reg.is_subtype(
            &Array(Array(I64.into()).into()).into(),
            &Array(Array(I64.into()).into()).into()
        ));

        // Array of any type is subtype of Universe
        assert!(reg.is_subtype(&Array(I64.into()).into(), &Universe.into()));

        // Array with inheritance (will be tested more with ADTs)
        let mut adts_registry = TypeRegistry::default();
        let vehicle = create_product_adt("Vehicle", vec![]);
        let car = create_product_adt("Car", vec![]);
        let vehicles_enum = create_sum_adt("Vehicles", vec![vehicle, car]);
        adts_registry.register_adt(&vehicles_enum).unwrap();

        assert!(adts_registry.is_subtype(
            &Array(Adt("Car".to_string()).into()).into(),
            &Array(Adt("Vehicles".to_string()).into()).into()
        ));
    }

    #[test]
    fn test_tuple_subtyping() {
        let mut reg = TypeRegistry::default();

        // Same type tuples
        assert!(reg.is_subtype(
            &Tuple(vec![I64.into(), Bool.into()]).into(),
            &Tuple(vec![I64.into(), Bool.into()]).into()
        ));

        // Different type tuples
        assert!(!reg.is_subtype(
            &Tuple(vec![I64.into(), Bool.into()]).into(),
            &Tuple(vec![Bool.into(), I64.into()]).into()
        ));

        // Different length tuples
        assert!(!reg.is_subtype(
            &Tuple(vec![I64.into(), Bool.into()]).into(),
            &Tuple(vec![I64.into(), Bool.into(), String.into()]).into()
        ));

        // Empty tuples
        assert!(reg.is_subtype(&Tuple(vec![]).into(), &Tuple(vec![]).into()));

        // All tuples are subtypes of Universe
        assert!(reg.is_subtype(
            &Tuple(vec![I64.into(), Bool.into()]).into(),
            &Universe.into()
        ));

        // Nested tuples
        assert!(
            reg.is_subtype(
                &Tuple(vec![
                    I64.into(),
                    Tuple(vec![Bool.into(), String.into()]).into()
                ])
                .into(),
                &Tuple(vec![
                    I64.into(),
                    Tuple(vec![Bool.into(), String.into()]).into()
                ])
                .into()
            )
        );
    }

    #[test]
    fn test_map_subtyping() {
        // Setup basic registry
        let mut reg = TypeRegistry::default();

        // Same type maps
        assert!(reg.is_subtype(
            &Map(String.into(), I64.into()).into(),
            &Map(String.into(), I64.into()).into()
        ));

        // Different key types
        assert!(!reg.is_subtype(
            &Map(String.into(), I64.into()).into(),
            &Map(I64.into(), I64.into()).into()
        ));

        // Different value types
        assert!(!reg.is_subtype(
            &Map(String.into(), I64.into()).into(),
            &Map(String.into(), Bool.into()).into()
        ));

        // All maps are subtypes of Universe
        assert!(reg.is_subtype(&Map(String.into(), I64.into()).into(), &Universe.into()));

        // Create a registry with ADTs to test variance
        let mut adts_registry = TypeRegistry::default();
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let animals_enum = create_sum_adt("Animals", vec![animal, dog]);
        adts_registry.register_adt(&animals_enum).unwrap();

        // Test contravariance of map keys:
        // Map(Animals, String) <: Map(Dog, String) because Dog <: Animals
        assert!(adts_registry.is_subtype(
            &Map(Adt("Animals".to_string()).into(), String.into()).into(),
            &Map(Adt("Dog".to_string()).into(), String.into()).into()
        ));

        // Test covariance of map values:
        // Map(String, Dog) <: Map(String, Animals) because Dog <: Animals
        assert!(adts_registry.is_subtype(
            &Map(String.into(), Adt("Dog".to_string()).into()).into(),
            &Map(String.into(), Adt("Animals".to_string()).into()).into()
        ));
    }

    #[test]
    fn test_closure_subtyping() {
        let mut reg = TypeRegistry::default();

        // Same function signatures
        assert!(reg.is_subtype(
            &Closure(I64.into(), Bool.into()).into(),
            &Closure(I64.into(), Bool.into()).into()
        ));

        // Contravariant parameter types - narrower param type is not a subtype
        assert!(!reg.is_subtype(
            &Closure(I64.into(), Bool.into()).into(),
            &Closure(F64.into(), Bool.into()).into()
        ));

        // All closures are subtypes of Universe
        assert!(reg.is_subtype(&Closure(I64.into(), Bool.into()).into(), &Universe.into()));

        // Contravariant parameter types - broader param type is a subtype
        let mut adts_registry = TypeRegistry::default();
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let animals_enum = create_sum_adt("Animals", vec![animal, dog]);
        adts_registry.register_adt(&animals_enum).unwrap();

        // (Animals -> Bool) <: (Dog -> Bool) because Dog <: Animals (contravariance)
        assert!(adts_registry.is_subtype(
            &Closure(Adt("Animals".to_string()).into(), Bool.into()).into(),
            &Closure(Adt("Dog".to_string()).into(), Bool.into()).into()
        ));

        // Covariant return types
        // (Int64 -> Dog) <: (Int64 -> Animals) because Dog <: Animals
        assert!(adts_registry.is_subtype(
            &Closure(I64.into(), Adt("Dog".to_string()).into()).into(),
            &Closure(I64.into(), Adt("Animals".to_string()).into()).into()
        ));
    }

    #[test]
    fn test_adt_registration_and_subtyping() {
        let mut reg = TypeRegistry::default();

        // Create a simple Shape hierarchy
        let shape = create_product_adt("Shape", vec![("area", AstType::Float64)]);
        let circle = create_product_adt(
            "Circle",
            vec![("radius", AstType::Float64), ("area", AstType::Float64)],
        );
        let rectangle = create_product_adt(
            "Rectangle",
            vec![
                ("width", AstType::Float64),
                ("height", AstType::Float64),
                ("area", AstType::Float64),
            ],
        );

        // Create an enum for shapes
        let shapes_enum = create_sum_adt("Shapes", vec![shape, circle, rectangle]);

        // Register the ADT
        reg.register_adt(&shapes_enum).unwrap();

        // Test subtypes relationship
        assert!(reg.is_subtype(
            &Adt("Circle".to_string()).into(),
            &Adt("Shapes".to_string()).into()
        ));

        assert!(reg.is_subtype(
            &Adt("Rectangle".to_string()).into(),
            &Adt("Shapes".to_string()).into()
        ));

        assert!(reg.is_subtype(
            &Adt("Shape".to_string()).into(),
            &Adt("Shapes".to_string()).into()
        ));

        // Same type should be a subtype of itself
        assert!(reg.is_subtype(
            &Adt("Shapes".to_string()).into(),
            &Adt("Shapes".to_string()).into()
        ));

        // All ADTs are subtypes of Universe
        assert!(reg.is_subtype(&Adt("Shapes".to_string()).into(), &Universe.into()));

        // Non-subtypes should return false
        assert!(!reg.is_subtype(
            &Adt("Shapes".to_string()).into(),
            &Adt("Circle".to_string()).into()
        ));

        assert!(!reg.is_subtype(
            &Adt("Circle".to_string()).into(),
            &Adt("Rectangle".to_string()).into()
        ));
    }

    #[test]
    fn test_universe_as_top_type() {
        let mut reg = TypeRegistry::default();

        // Check that Universe is a supertype of all primitive types
        assert!(reg.is_subtype(&I64.into(), &Universe.into()));
        assert!(reg.is_subtype(&String.into(), &Universe.into()));
        assert!(reg.is_subtype(&Bool.into(), &Universe.into()));
        assert!(reg.is_subtype(&F64.into(), &Universe.into()));
        assert!(reg.is_subtype(&Unit.into(), &Universe.into()));

        // Check that Universe is a supertype of all complex types
        assert!(reg.is_subtype(&Array(I64.into()).into(), &Universe.into()));
        assert!(reg.is_subtype(
            &Tuple(vec![I64.into(), Bool.into()]).into(),
            &Universe.into()
        ));
        assert!(reg.is_subtype(&Map(String.into(), I64.into()).into(), &Universe.into()));
        assert!(reg.is_subtype(&Closure(I64.into(), Bool.into()).into(), &Universe.into()));

        // Check that Universe is a supertype of Stored and Costed types
        assert!(reg.is_subtype(&Stored(I64.into()).into(), &Universe.into()));
        assert!(reg.is_subtype(&Costed(I64.into()).into(), &Universe.into()));

        // But Universe is not a subtype of any other type
        assert!(!reg.is_subtype(&Universe.into(), &I64.into()));
        assert!(!reg.is_subtype(&Universe.into(), &String.into()));
        assert!(!reg.is_subtype(&Universe.into(), &Array(I64.into()).into()));
    }

    #[test]
    fn test_nothing_as_bottom_type() {
        let mut reg = TypeRegistry::default();

        // Nothing is a subtype of all primitive types
        assert!(reg.is_subtype(&Nothing.into(), &I64.into()));
        assert!(reg.is_subtype(&Nothing.into(), &String.into()));
        assert!(reg.is_subtype(&Nothing.into(), &Bool.into()));
        assert!(reg.is_subtype(&Nothing.into(), &F64.into()));
        assert!(reg.is_subtype(&Nothing.into(), &Unit.into()));
        assert!(reg.is_subtype(&Nothing.into(), &Universe.into()));

        // Nothing is a subtype of complex types
        assert!(reg.is_subtype(&Nothing.into(), &Array(I64.into()).into()));
        assert!(reg.is_subtype(
            &Nothing.into(),
            &Tuple(vec![I64.into(), Bool.into()]).into()
        ));
        assert!(reg.is_subtype(&Nothing.into(), &Closure(I64.into(), Bool.into()).into()));

        // But no type is a subtype of Nothing (except Nothing itself)
        assert!(!reg.is_subtype(&I64.into(), &Nothing.into()));
        assert!(!reg.is_subtype(&Bool.into(), &Nothing.into()));
        assert!(!reg.is_subtype(&Universe.into(), &Nothing.into()));
        assert!(!reg.is_subtype(&Array(I64.into()).into(), &Nothing.into()));
    }

    #[test]
    fn test_generic_subtyping_with_bounds() {
        let mut reg = TypeRegistry::default();

        // Set up a type hierarchy for testing
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let cat = create_product_adt("Cat", vec![]);

        let animals_enum = create_sum_adt("Animals", vec![animal, dog.clone(), cat.clone()]);
        reg.register_adt(&animals_enum).unwrap();

        // Create generic types with and without bounds - using unique IDs
        let generic_1 = Gen(Generic(1, Option::None)).into();
        let generic_2 = Gen(Generic(2, Option::None)).into();

        // Create bounded generics with *unique* IDs - bounds must be ADTs only
        let generic_3_animals = Gen(Generic(3, Some(Adt("Animals".to_string()).into()))).into();
        let generic_4_animals = Gen(Generic(4, Some(Adt("Animals".to_string()).into()))).into();
        let generic_5_dog = Gen(Generic(5, Some(Adt("Dog".to_string()).into()))).into();

        // Test 1: Same generic ID is a subtype of itself
        assert!(reg.is_subtype(&generic_1, &generic_1));
        assert!(reg.is_subtype(&generic_3_animals, &generic_3_animals));

        // Test 2: Different generic IDs with compatible bounds
        // Gen<5: Dog> <: Gen<4: Animals> because Dog <: Animals
        assert!(reg.is_subtype(&generic_5_dog, &generic_4_animals));

        // Test 3: Different generic IDs with incompatible bounds
        // Gen<4: Animals> !<: Gen<5: Dog> because Animals !<: Dog
        assert!(!reg.is_subtype(&generic_4_animals, &generic_5_dog));

        // Test 4: Different generic IDs with no bounds are never subtypes
        assert!(!reg.is_subtype(&generic_1, &generic_2));

        // Test 5: Concrete type vs generic with bound
        let dog_type = Adt("Dog".to_string()).into();
        let animals_type = Adt("Animals".to_string()).into();

        // Dog <: Gen<4: Animals> because Dog <: Animals
        assert!(reg.is_subtype(&dog_type, &generic_4_animals));

        // Animals !<: Gen<5: Dog> because Animals !<: Dog
        assert!(!reg.is_subtype(&animals_type, &generic_5_dog));

        // Test 6: Generic with bound vs concrete type (bidirectional check)
        // Gen<5: Dog> <: Animals because Dog <: Animals
        assert!(reg.is_subtype(&generic_5_dog, &animals_type));

        // Gen<4: Animals> !<: Dog because Animals !<: Dog
        assert!(!reg.is_subtype(&generic_4_animals, &dog_type));
    }

    #[test]
    fn test_none_subtyping() {
        let mut reg = TypeRegistry::default();

        // Test None as a subtype of any Optional type
        assert!(reg.is_subtype(&None.into(), &Optional(I64.into()).into()));
        assert!(reg.is_subtype(&None.into(), &Optional(String.into()).into()));
        assert!(reg.is_subtype(&None.into(), &Optional(Bool.into()).into()));
        assert!(reg.is_subtype(&None.into(), &Optional(F64.into()).into()));
        assert!(reg.is_subtype(&None.into(), &Optional(Unit.into()).into()));

        // Test None with complex Optional types
        assert!(reg.is_subtype(&None.into(), &Optional(Array(I64.into()).into()).into()));

        // Test that None is not a subtype of non-Optional types
        assert!(!reg.is_subtype(&None.into(), &I64.into()));
        assert!(!reg.is_subtype(&None.into(), &String.into()));

        // None is still a subtype of Universe (as all types are)
        assert!(reg.is_subtype(&None.into(), &Universe.into()));

        // None is not equal to Nothing
        assert!(!reg.is_subtype(&None.into(), &Nothing.into()));
        assert!(reg.is_subtype(&Nothing.into(), &None.into()));
    }

    #[test]
    fn test_type_optional_subtyping() {
        let mut reg = TypeRegistry::default();

        // Test that a type is a subtype of its corresponding optional type
        assert!(reg.is_subtype(&I64.into(), &Optional(I64.into()).into()));
        assert!(reg.is_subtype(&String.into(), &Optional(String.into()).into()));
        assert!(reg.is_subtype(&Bool.into(), &Optional(Bool.into()).into()));

        // Test with nested types
        assert!(reg.is_subtype(
            &Array(I64.into()).into(),
            &Optional(Array(I64.into()).into()).into()
        ));

        // Test with inheritance
        let mut adts_reg = TypeRegistry::default();
        let animal = create_product_adt("Animal", vec![]);
        let dog = create_product_adt("Dog", vec![]);
        let animals_enum = create_sum_adt("Animals", vec![animal, dog]);
        adts_reg.register_adt(&animals_enum).unwrap();

        // Dog <: Optional<Dog>
        assert!(adts_reg.is_subtype(
            &Adt("Dog".to_string()).into(),
            &Optional(Adt("Dog".to_string()).into()).into()
        ));

        // Dog <: Optional<Animals> (transitivity)
        assert!(adts_reg.is_subtype(
            &Adt("Dog".to_string()).into(),
            &Optional(Adt("Animals".to_string()).into()).into()
        ));

        // Test that non-subtypes remain non-subtypes when wrapped in Optional
        assert!(!adts_reg.is_subtype(
            &Adt("Animals".to_string()).into(),
            &Optional(Adt("Dog".to_string()).into()).into()
        ));
    }

    #[test]
    fn test_complex_nested_type_hierarchy() {
        let mut reg = TypeRegistry::default();

        // Create a complex type hierarchy for vehicles
        let vehicle = create_product_adt("Vehicle", vec![("wheels", AstType::Int64)]);

        let car = create_product_adt(
            "Car",
            vec![("wheels", AstType::Int64), ("doors", AstType::Int64)],
        );

        let sports_car = create_product_adt(
            "SportsCar",
            vec![
                ("wheels", AstType::Int64),
                ("doors", AstType::Int64),
                ("top_speed", AstType::Float64),
            ],
        );

        let truck = create_product_adt(
            "Truck",
            vec![
                ("wheels", AstType::Int64),
                ("load_capacity", AstType::Float64),
            ],
        );

        // First level enum: Cars
        let cars_enum = create_sum_adt("Cars", vec![car, sports_car]);

        // Second level enum: Vehicles
        let vehicles_enum = create_sum_adt("Vehicles", vec![vehicle, cars_enum, truck]);

        // Register the ADT
        reg.register_adt(&vehicles_enum).unwrap();

        // Test direct subtyping relationships
        assert!(reg.is_subtype(
            &Adt("Car".to_string()).into(),
            &Adt("Cars".to_string()).into()
        ));

        assert!(reg.is_subtype(
            &Adt("SportsCar".to_string()).into(),
            &Adt("Cars".to_string()).into()
        ));

        assert!(reg.is_subtype(
            &Adt("Cars".to_string()).into(),
            &Adt("Vehicles".to_string()).into()
        ));

        assert!(reg.is_subtype(
            &Adt("Truck".to_string()).into(),
            &Adt("Vehicles".to_string()).into()
        ));

        // Test transitive subtyping
        assert!(reg.is_subtype(
            &Adt("SportsCar".to_string()).into(),
            &Adt("Vehicles".to_string()).into()
        ));

        // All vehicle types are subtypes of Universe
        assert!(reg.is_subtype(&Adt("Vehicles".to_string()).into(), &Universe.into()));
        assert!(reg.is_subtype(&Adt("Cars".to_string()).into(), &Universe.into()));
        assert!(reg.is_subtype(&Adt("SportsCar".to_string()).into(), &Universe.into()));

        // Test negative cases
        assert!(!reg.is_subtype(
            &Adt("Vehicles".to_string()).into(),
            &Adt("Cars".to_string()).into()
        ));

        assert!(!reg.is_subtype(
            &Adt("Cars".to_string()).into(),
            &Adt("SportsCar".to_string()).into()
        ));

        assert!(!reg.is_subtype(
            &Adt("Truck".to_string()).into(),
            &Adt("Cars".to_string()).into()
        ));
    }

    #[test]
    fn test_native_trait_concat() {
        let mut reg = TypeRegistry::default();

        // Test types that should implement Concat
        assert!(reg.is_subtype(&String.into(), &Concat.into()));
        assert!(reg.is_subtype(&Array(I64.into()).into(), &Concat.into()));
        assert!(reg.is_subtype(&Map(String.into(), I64.into()).into(), &Concat.into()));

        // Test nested types that implement Concat
        assert!(reg.is_subtype(&Array(Array(I64.into()).into()).into(), &Concat.into()));

        // Test types that should not implement Concat
        assert!(!reg.is_subtype(&I64.into(), &Concat.into()));
        assert!(!reg.is_subtype(&Bool.into(), &Concat.into()));
        assert!(!reg.is_subtype(&F64.into(), &Concat.into()));
        assert!(!reg.is_subtype(&Unit.into(), &Concat.into()));
        assert!(!reg.is_subtype(&None.into(), &Concat.into()));
        assert!(!reg.is_subtype(
            &Tuple(vec![I64.into(), String.into()]).into(),
            &Concat.into()
        ));
        assert!(!reg.is_subtype(&Closure(I64.into(), String.into()).into(), &Concat.into()));

        // Special types
        assert!(reg.is_subtype(&Nothing.into(), &Concat.into()));
        assert!(!reg.is_subtype(&Concat.into(), &Nothing.into()));
        assert!(reg.is_subtype(&Concat.into(), &Universe.into()));

        // Stored/Costed with Concat-compatible inner types
        assert!(reg.is_subtype(&Stored(String.into()).into(), &String.into()));
        assert!(reg.is_subtype(&Stored(String.into()).into(), &Concat.into()));
        assert!(reg.is_subtype(&Costed(String.into()).into(), &Concat.into()));
    }

    #[test]
    fn test_native_trait_eqhash() {
        let mut reg = TypeRegistry::default();

        // Test primitive types that should implement EqHash
        assert!(reg.is_subtype(&I64.into(), &EqHash.into()));
        assert!(reg.is_subtype(&String.into(), &EqHash.into()));
        assert!(reg.is_subtype(&Bool.into(), &EqHash.into()));
        assert!(reg.is_subtype(&Unit.into(), &EqHash.into()));
        assert!(reg.is_subtype(&None.into(), &EqHash.into()));

        // Test tuple types with all EqHash elements
        assert!(reg.is_subtype(
            &Tuple(vec![I64.into(), String.into(), Bool.into()]).into(),
            &EqHash.into()
        ));

        // Mixed tuple with a non-EqHash type should not implement EqHash
        assert!(!reg.is_subtype(
            &Tuple(vec![I64.into(), Closure(I64.into(), Bool.into()).into()]).into(),
            &EqHash.into()
        ));

        // Test empty tuple (should implement EqHash)
        assert!(reg.is_subtype(&Tuple(vec![]).into(), &EqHash.into()));

        // Test types that should not implement EqHash
        assert!(!reg.is_subtype(&F64.into(), &EqHash.into())); // Floating point is not guaranteed equality
        assert!(!reg.is_subtype(&Closure(I64.into(), Bool.into()).into(), &EqHash.into()));
        assert!(!reg.is_subtype(&Map(String.into(), I64.into()).into(), &EqHash.into()));

        // Special types
        assert!(reg.is_subtype(&Nothing.into(), &EqHash.into()));
        assert!(!reg.is_subtype(&EqHash.into(), &Nothing.into()));
        assert!(reg.is_subtype(&EqHash.into(), &Universe.into()));
    }

    #[test]
    fn test_native_trait_arithmetic() {
        let mut reg = TypeRegistry::default();

        // Test types that should implement Arithmetic
        assert!(reg.is_subtype(&I64.into(), &Arithmetic.into()));
        assert!(reg.is_subtype(&F64.into(), &Arithmetic.into()));

        // Test types that should not implement Arithmetic
        assert!(!reg.is_subtype(&String.into(), &Arithmetic.into()));
        assert!(!reg.is_subtype(&Bool.into(), &Arithmetic.into()));
        assert!(!reg.is_subtype(&Unit.into(), &Arithmetic.into()));
        assert!(!reg.is_subtype(&None.into(), &Arithmetic.into()));
        assert!(!reg.is_subtype(
            &Tuple(vec![I64.into(), F64.into()]).into(),
            &Arithmetic.into()
        ));
        assert!(!reg.is_subtype(&Array(I64.into()).into(), &Arithmetic.into()));
        assert!(!reg.is_subtype(&Map(String.into(), I64.into()).into(), &Arithmetic.into()));

        // Special types
        assert!(reg.is_subtype(&Nothing.into(), &Arithmetic.into()));
        assert!(!reg.is_subtype(&Arithmetic.into(), &Nothing.into()));
        assert!(reg.is_subtype(&Arithmetic.into(), &Universe.into()));

        // Stored/Costed with Arithmetic-compatible inner types
        assert!(reg.is_subtype(&Stored(I64.into()).into(), &I64.into()));
        assert!(reg.is_subtype(&Stored(I64.into()).into(), &Arithmetic.into()));
        assert!(reg.is_subtype(&Costed(F64.into()).into(), &Arithmetic.into()));
    }

    #[test]
    fn test_adt_eqhash() {
        // Create a registry with ADTs
        let mut reg = TypeRegistry::default();

        // Create ADTs with EqHash-compatible fields
        let point = create_product_adt("Point", vec![("x", AstType::Int64), ("y", AstType::Int64)]);

        // Create ADT with mixed field types (some EqHash, some not)
        let complex_shape = create_product_adt(
            "ComplexShape",
            vec![
                ("name", AstType::String),
                (
                    "transform",
                    AstType::Closure(spanned(AstType::Int64), spanned(AstType::Int64)),
                ),
            ],
        );

        // Create sum type with all variants satisfying EqHash
        let shape1 = create_product_adt("Circle", vec![("radius", AstType::Int64)]);
        let shape2 = create_product_adt(
            "Rectangle",
            vec![("width", AstType::Int64), ("height", AstType::Int64)],
        );
        let shapes = create_sum_adt("Shape", vec![shape1, shape2]);

        // Register the ADTs
        reg.register_adt(&point).unwrap();
        reg.register_adt(&complex_shape).unwrap();
        reg.register_adt(&shapes).unwrap();

        // Test EqHash relationships

        // Point should satisfy EqHash since all fields (x, y) are Int64 which satisfies EqHash
        assert!(reg.is_subtype(&Adt("Point".to_string()).into(), &EqHash.into()));

        // ComplexShape should not satisfy EqHash since it has a Closure field which doesn't satisfy EqHash
        assert!(!reg.is_subtype(&Adt("ComplexShape".to_string()).into(), &EqHash.into()));

        // Circle and Rectangle should satisfy EqHash
        assert!(reg.is_subtype(&Adt("Circle".to_string()).into(), &EqHash.into()));
        assert!(reg.is_subtype(&Adt("Rectangle".to_string()).into(), &EqHash.into()));

        // Shape (sum type) should satisfy EqHash since all variants satisfy EqHash
        assert!(reg.is_subtype(&Adt("Shape".to_string()).into(), &EqHash.into()));
    }

    #[test]
    fn test_recursive_adt_with_native_traits() {
        // Create a registry with a recursive ADT
        let mut reg = TypeRegistry::default();

        // Create a recursive List type
        let list_node = create_product_adt(
            "ListNode",
            vec![
                ("value", AstType::Int64),
                (
                    "next",
                    AstType::Questioned(spanned(AstType::Identifier("ListNode".to_string()))),
                ),
            ],
        );

        // Register the ADT
        reg.register_adt(&list_node).unwrap();

        // Test relationships with native traits

        // ListNode should satisfy EqHash since both Int64 and Optional<ListNode> satisfy EqHash
        assert!(reg.is_subtype(&Adt("ListNode".to_string()).into(), &EqHash.into()));

        // ListNode should not satisfy Concat or Arithmetic
        assert!(!reg.is_subtype(&Adt("ListNode".to_string()).into(), &Concat.into()));
        assert!(!reg.is_subtype(&Adt("ListNode".to_string()).into(), &Arithmetic.into()));
    }

    #[test]
    fn test_multiple_native_traits() {
        let mut reg = TypeRegistry::default();

        // Test which types satisfy multiple traits

        // Int64 satisfies both EqHash and Arithmetic
        assert!(reg.is_subtype(&I64.into(), &EqHash.into()));
        assert!(reg.is_subtype(&I64.into(), &Arithmetic.into()));
        assert!(!reg.is_subtype(&I64.into(), &Concat.into()));

        // String satisfies both EqHash and Concat
        assert!(reg.is_subtype(&String.into(), &EqHash.into()));
        assert!(reg.is_subtype(&String.into(), &Concat.into()));
        assert!(!reg.is_subtype(&String.into(), &Arithmetic.into()));

        // Float64 only satisfies Arithmetic
        assert!(reg.is_subtype(&F64.into(), &Arithmetic.into()));
        assert!(!reg.is_subtype(&F64.into(), &EqHash.into()));
        assert!(!reg.is_subtype(&F64.into(), &Concat.into()));

        // Bool only satisfies EqHash
        assert!(reg.is_subtype(&Bool.into(), &EqHash.into()));
        assert!(!reg.is_subtype(&Bool.into(), &Arithmetic.into()));
        assert!(!reg.is_subtype(&Bool.into(), &Concat.into()));

        // Array satisfies only Concat
        assert!(reg.is_subtype(&Array(I64.into()).into(), &Concat.into()));
        assert!(!reg.is_subtype(&Array(I64.into()).into(), &EqHash.into()));
        assert!(!reg.is_subtype(&Array(I64.into()).into(), &Arithmetic.into()));
    }

    #[test]
    fn test_collection_function_subtyping() {
        let mut reg = TypeRegistry::default();

        // Test Map as a subtype of Function
        // Map(String, I64) <: Closure(String, Optional<I64>)
        assert!(reg.is_subtype(
            &Map(String.into(), I64.into()).into(),
            &Closure(String.into(), Optional(I64.into()).into()).into()
        ));

        // Function contravariance on parameters
        // Map(EqHash, I64) <: Closure(String, Optional<I64>)
        assert!(reg.is_subtype(
            &Map(EqHash.into(), I64.into()).into(),
            &Closure(String.into(), Optional(I64.into()).into()).into()
        ));

        // Function covariance on return type
        // Map(String, I64) <: Closure(String, Optional<Universe>)
        assert!(
            reg.is_subtype(
                &Map(String.into(), I64.into()).into(),
                &Closure(
                    String.into(),
                    Optional(Universe.into()).into() // Supertype of I64 (covariance)
                )
                .into()
            )
        );

        // Negative test - parameter type doesn't match
        assert!(
            !reg.is_subtype(
                &Map(String.into(), I64.into()).into(),
                &Closure(
                    I64.into(), // String is not a subtype of I64
                    Optional(I64.into()).into()
                )
                .into()
            )
        );

        // Test Array as a subtype of Function
        // Array(String) <: Closure(I64, Optional<String>)
        assert!(reg.is_subtype(
            &Array(String.into()).into(),
            &Closure(I64.into(), Optional(String.into()).into()).into()
        ));

        // Function covariance on return type
        // Array(String) <: Closure(I64, Optional<Universe>)
        assert!(
            reg.is_subtype(
                &Array(String.into()).into(),
                &Closure(
                    I64.into(),
                    Optional(Universe.into()).into() // Supertype of String (covariance)
                )
                .into()
            )
        );

        // Negative test - parameter type must be I64
        assert!(
            !reg.is_subtype(
                &Array(String.into()).into(),
                &Closure(
                    String.into(), // Must be I64
                    Optional(String.into()).into()
                )
                .into()
            )
        );

        // Negative test - non-optional return type not compatible
        assert!(
            !reg.is_subtype(
                &Array(String.into()).into(),
                &Closure(
                    I64.into(),
                    String.into() // Not Optional<String>
                )
                .into()
            )
        );

        // Tuples are not subtypes of functions
        assert!(!reg.is_subtype(
            &Tuple(vec![String.into(), String.into()]).into(),
            &Closure(I64.into(), Optional(String.into()).into()).into()
        ));
    }
}
