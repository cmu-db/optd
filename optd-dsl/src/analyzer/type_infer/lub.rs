use crate::analyzer::{
    hir::Identifier,
    types::{Type, TypeRegistry},
};
use std::collections::HashSet;

impl TypeRegistry {
    /// Finds the least upper bound (LUB) of two types.
    ///
    /// The least upper bound is the most specific type that is a supertype of both input types.
    /// This operation is also known as the "join" in type theory.
    ///
    /// The LUB follows these principles:
    /// 1. For container types (Array, Tuple, Map, etc.), it applies covariance rules.
    /// 2. For function types, it uses contravariance for parameters and covariance for return types.
    /// 3. For native trait types (Concat, EqHash, Arithmetic), the result is the trait if both types implement it.
    /// 4. For class/ADT types, it finds the closest common ancestor in the type hierarchy.
    /// 5. For special wrapper types (Optional, Stored, Costed), it preserves the wrapper and computes LUB of inner types.
    /// 6. The ultimate fallback is Type::Universe, the top type of the hierarchy.
    ///
    /// Note on trait types: The type system has three special trait types (Concat, EqHash, Arithmetic)
    /// that represent capabilities rather than data structures. There isn't a strict hierarchy among
    /// these traits, and a type can implement multiple traits (so there might be confusion).
    /// LUB will return a trait type only if one of the types is the trait.
    /// This trick works well enough for our type inference.
    ///
    /// # Arguments
    ///
    /// * `type1` - The first type
    /// * `type2` - The second type
    ///
    /// # Returns
    ///
    /// The least upper bound of the two types. Returns `Type::Universe` if no more specific common
    /// supertype exists.
    pub(super) fn least_upper_bound(&self, type1: &Type, type2: &Type) -> Type {
        use Type::*;

        match (type1, type2) {
            // Nothing is the bottom type - LUB(Nothing, T) = T.
            (Nothing, other) | (other, Nothing) => other.clone(),

            // Array covariance: LUB(Array<T1>, Array<T2>) = Array<LUB(T1, T2)>.
            (Array(elem1), Array(elem2)) => {
                let lub_elem = self.least_upper_bound(elem1, elem2);
                Array(lub_elem.into())
            }

            // Tuple covariance: LUB((T1,T2,...), (U1,U2,...)) = (LUB(T1,U1), LUB(T2,U2), ...).
            (Tuple(elems1), Tuple(elems2)) if elems1.len() == elems2.len() => {
                let lub_elems = elems1
                    .iter()
                    .zip(elems2.iter())
                    .map(|(e1, e2)| self.least_upper_bound(e1, e2))
                    .collect();
                Tuple(lub_elems)
            }

            // Map with contravariant keys and covariant values.
            (Map(key1, val1), Map(key2, val2)) => {
                let glb_key = self.greatest_lower_bound(key1, key2);
                let lub_val = self.least_upper_bound(val1, val2);
                Map(glb_key.into(), lub_val.into())
            }

            // Optional type handling.
            (Optional(inner1), Optional(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2);
                Optional(lub_inner.into())
            }
            (None, Optional(inner)) | (Optional(inner), None) => Optional(inner.clone()),
            (Optional(inner), other) | (other, Optional(inner)) => {
                Optional(self.least_upper_bound(inner, other).into())
            }

            // Stored type handling.
            (Stored(inner1), Stored(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2);
                Stored(lub_inner.into())
            }

            // Costed type handling.
            (Costed(inner1), Costed(inner2)) => {
                let lub_inner = self.least_upper_bound(inner1, inner2);
                Costed(lub_inner.into())
            }

            // Mixed Stored and Costed - result is Stored (Costed is more specific, Stored is more general).
            (Costed(costed), Stored(stored)) | (Stored(stored), Costed(costed)) => {
                let lub_inner = self.least_upper_bound(costed, stored);
                Stored(lub_inner.into())
            }

            // Unwrap Stored/Costed for LUB with other types.
            (Stored(stored), other) | (other, Stored(stored)) => {
                self.least_upper_bound(stored, other)
            }
            (Costed(costed), other) | (other, Costed(costed)) => {
                self.least_upper_bound(costed, other)
            }

            // Function type handling - contravariant parameters, covariant return types.
            (Closure(param1, ret1), Closure(param2, ret2)) => {
                let param_type = self.greatest_lower_bound(param1, param2);
                let ret_type = self.least_upper_bound(ret1, ret2);
                Closure(param_type.into(), ret_type.into())
            }

            // Map/Function compatibility - a Map can be seen as a function from keys to optional values.
            (Map(key_type, val_type), Closure(param_type, ret_type))
            | (Closure(param_type, ret_type), Map(key_type, val_type)) => {
                let param_glb = self.greatest_lower_bound(param_type, key_type);
                let ret_lub = self.least_upper_bound(ret_type, &Optional(val_type.clone()));
                Closure(param_glb.into(), ret_lub.into())
            }

            // Array/Function compatibility - an Array can be seen as a function from indices to values.
            (Array(elem_type), Closure(param_type, ret_type))
            | (Closure(param_type, ret_type), Array(elem_type)) => {
                if matches!(&**param_type, I64) {
                    let ret_lub = self.least_upper_bound(ret_type, &Optional(elem_type.clone()));
                    Closure(I64.into(), ret_lub.into())
                } else {
                    Universe
                }
            }

            // ADT types - find their common supertype in the hierarchy.
            (Adt(name1), Adt(name2)) => {
                if let Some(common_ancestor) = self.find_common_supertype(name1, name2) {
                    Adt(common_ancestor)
                } else {
                    Universe
                }
            }

            // Native trait types trick, check function documentation for more details.
            (_, Concat) | (Concat, _)
                if self.is_subtype(type1, &Concat) && self.is_subtype(type2, &Concat) =>
            {
                Concat
            }

            (_, EqHash) | (EqHash, _)
                if self.is_subtype(type1, &EqHash) && self.is_subtype(type2, &EqHash) =>
            {
                EqHash
            }

            (_, Arithmetic) | (Arithmetic, _)
                if self.is_subtype(type1, &Arithmetic) && self.is_subtype(type2, &Arithmetic) =>
            {
                Arithmetic
            }

            // Default case - universe is the ultimate fallback.
            _ => Universe,
        }
    }

    /// Finds a common supertype of two ADT identifiers in the type hierarchy.
    ///
    /// # Arguments
    ///
    /// * `name1` - The first ADT identifier
    /// * `name2` - The second ADT identifier
    ///
    /// # Returns
    ///
    /// Some(identifier) if a common supertype is found, None otherwise.
    fn find_common_supertype(&self, name1: &Identifier, name2: &Identifier) -> Option<Identifier> {
        let supertypes1 = self.get_all_supertypes(name1);
        let supertypes2 = self.get_all_supertypes(name2);

        let common_supertypes: Vec<_> = supertypes1
            .iter()
            .filter(|&st| supertypes2.contains(st))
            .collect();

        if common_supertypes.is_empty() {
            return None;
        }

        let mut most_specific = common_supertypes[0];
        for &supertype in &common_supertypes[1..] {
            if self.is_subtype(
                &Type::Adt(supertype.clone()),
                &Type::Adt(most_specific.clone()),
            ) {
                most_specific = supertype;
            }
        }

        Some(most_specific.clone())
    }

    /// Gets all supertypes of an ADT identifier in the type hierarchy.
    ///
    /// # Arguments
    ///
    /// * `name` - The ADT identifier
    ///
    /// # Returns
    ///
    /// A HashSet containing all supertypes of the given ADT, including itself.
    fn get_all_supertypes(&self, name: &Identifier) -> HashSet<Identifier> {
        let mut result = HashSet::new();
        result.insert(name.clone());

        // Inner recursive function to collect supertypes.
        fn collect_supertypes(
            registry: &TypeRegistry,
            name: &Identifier,
            result: &mut HashSet<Identifier>,
        ) {
            for (parent, children) in &registry.subtypes {
                if children.contains(name) {
                    result.insert(parent.clone());
                    collect_supertypes(registry, parent, result);
                }
            }
        }

        collect_supertypes(self, name, &mut result);
        result
    }
}
