use crate::analyzer::types::{Type, TypeRegistry};

impl TypeRegistry {
    /// Finds the greatest lower bound (GLB) of two types.
    /// The greatest lower bound is the most general type that is a subtype of both input types.
    ///
    /// The GLB follows these principles:
    /// 1. For container types (Array, Tuple), it applies covariance rules in reverse.
    /// 2. For function (and Map) types, it uses contravariance in reverse: covariance for parameters,
    ///    contravariance for return types.
    /// 3. For ADTs, it finds the more specific of the two types if one is a subtype of the other, given
    ///    the absence of multiple inheritance within ADTs.
    /// 4. For wrapper types:
    ///    - Optional preserves the wrapper for all GLB operations, computing GLB of inner type where applicable.
    ///    - None is the GLB of None and any Optional type.
    ///    - For Stored/Costed: Costed is more specific than Stored, so mixed operations yield Costed.
    /// 5. For compatibility between collections and functions:
    ///    - Map and Function: GLB results in a Function with appropriate parameter and return types.
    ///    - Array and Function: GLB results in a Function if the parameter type is I64.
    /// 6. Nothing is the bottom type, Universe is the top type, so GLB(Universe, T) = T and GLB(Nothing, T) = Nothing.
    /// 7. If no common subtype exists, Nothing is returned as the fallback.
    ///
    /// # Arguments
    ///
    /// * `type1` - The first type
    /// * `type2` - The second type
    ///
    /// # Returns
    ///
    /// The greatest lower bound of the two types. Returns `Type::Nothing` if no common
    /// subtype exists.
    pub(super) fn greatest_lower_bound(&self, type1: &Type, type2: &Type) -> Type {
        use Type::*;

        match (type1, type2) {
            // Universe is the top type - GLB(Universe, T) = T.
            (Universe, other) | (other, Universe) => other.clone(),

            // Array covariance: GLB(Array<T1>, Array<T2>) = Array<GLB(T1, T2)>.
            (Array(elem1), Array(elem2)) => {
                let glb_elem = self.greatest_lower_bound(elem1, elem2);
                Array(glb_elem.into())
            }

            // Tuple covariance: GLB((T1,T2,...), (U1,U2,...)) = (GLB(T1,U1), GLB(T2,U2), ...).
            (Tuple(elems1), Tuple(elems2)) if elems1.len() == elems2.len() => {
                let glb_elems: Vec<_> = elems1
                    .iter()
                    .zip(elems2.iter())
                    .map(|(e1, e2)| self.greatest_lower_bound(e1, e2))
                    .collect();

                Tuple(glb_elems)
            }

            // Map with contravariant keys and covariant values.
            (Map(key1, val1), Map(key2, val2)) => {
                let lub_key = self.least_upper_bound(key1, key2);
                let glb_val = self.greatest_lower_bound(val1, val2);

                Map(lub_key.into(), glb_val.into())
            }

            // Optional type handling.
            (Optional(inner1), Optional(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2);
                Optional(glb_inner.into())
            }
            (None, Optional(_)) | (Optional(_), None) => None,
            (None, None) => None,
            (Optional(inner), other) | (other, Optional(inner)) => {
                let glb = self.greatest_lower_bound(inner, other);
                Optional(glb.into())
            }

            // Stored type handling.
            (Stored(inner1), Stored(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2);
                Stored(glb_inner.into())
            }

            // Costed type handling.
            (Costed(inner1), Costed(inner2)) => {
                let glb_inner = self.greatest_lower_bound(inner1, inner2);
                Costed(glb_inner.into())
            }

            // Mixed Stored and Costed - result is Costed (more specific).
            (Costed(costed), Stored(stored)) | (Stored(stored), Costed(costed)) => {
                let glb_inner = self.greatest_lower_bound(costed, stored);
                Costed(glb_inner.into())
            }

            // Function type handling - covariant parameters, contravariant return types (reversed from LUB).
            (Closure(param1, ret1), Closure(param2, ret2)) => {
                let param_type = self.least_upper_bound(param1, param2);
                let ret_type = self.greatest_lower_bound(ret1, ret2);

                Closure(param_type.into(), ret_type.into())
            }

            // Map/Function compatibility - a Map can be seen as a function from keys to optional values.
            (Map(key_type, val_type), Closure(param_type, ret_type))
            | (Closure(param_type, ret_type), Map(key_type, val_type)) => {
                let param_glb = self.least_upper_bound(param_type, key_type);
                let ret_lub = self.greatest_lower_bound(ret_type, &Optional(val_type.clone()));
                Closure(param_glb.into(), ret_lub.into())
            }

            // Array/Function compatibility - an Array can be seen as a function from indices to values.
            (Array(elem_type), Closure(param_type, ret_type))
            | (Closure(param_type, ret_type), Array(elem_type)) => {
                if matches!(&**param_type, I64) {
                    let ret_lub = self.greatest_lower_bound(ret_type, &Optional(elem_type.clone()));
                    Closure(I64.into(), ret_lub.into())
                } else {
                    Nothing
                }
            }

            // ADT types - find subtype relationship if it exists.
            (adt1 @ Adt(_), adt2 @ Adt(_)) => {
                if self.is_subtype(adt1, adt2) {
                    adt1.clone()
                } else if self.is_subtype(adt2, adt1) {
                    adt2.clone()
                } else {
                    Nothing
                }
            }

            // Native trait handling.
            (trait_type @ (Concat | EqHash | Arithmetic), other)
                if self.is_subtype(other, trait_type) =>
            {
                other.clone()
            }
            (other, trait_type @ (Concat | EqHash | Arithmetic))
                if self.is_subtype(other, trait_type) =>
            {
                other.clone()
            }

            // Default case - return Nothing if no common subtype exists.
            _ => Nothing,
        }
    }
}
