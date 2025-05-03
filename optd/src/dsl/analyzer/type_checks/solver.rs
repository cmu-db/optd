//! # Type Inference and Constraint Solving
//!
//! This module implements the type inference system for our DSL. Type inference
//! is crucial for both ergonomics and correctness in the optimizer, as it allows
//! the engine to know which types are `Logical` or `Physical` (which behave
//! differently), and to verify if field accesses and function calls are valid.
//!
//! ## Type Inference Strategy
//!
//! The type inference works in three phases:
//!
//! 1. **Initial Type Creation**: During the `AST -> HIR<TypedSpan>` transformation,
//!    we create and add all implicit and explicit type information from the program
//!    (e.g., literals like `1` or `"hello"`, function annotations, etc.). For unknown
//!    types, we generate a new ID and assign the type to either `UnknownDesc` (for
//!    closure parameters and map keys) or `UnknownAsc` (for everything else).
//!
//! 2. **Constraint Generation**: Constraints are generated in the `generate.rs` file,
//!    which also performs scope-checking. Constraints indicate subtype relationships,
//!    field accesses, and function calls. For example, `let a: Logical = expr` generates
//!    the constraint `Logical :> typeof(expr)`.
//!
//! 3. **Constraint Solving**: The final step uses a constraint solver that iteratively
//!    refines unknown types until reaching a fixed point where no more refinements
//!    can be made.
//!
//! ## Constraint Solving Algorithm
//!
//! The constraint solving algorithm makes monotonic progress by tracking whether any
//! types changed during each iteration and continuing until reaching a fixed point.
//! Unknown types are refined according to their variance:
//!
//! - `UnknownAsc`: These types start at `Nothing` and ascend up the type hierarchy
//!   as needed. When encountered as a parent, they are updated to the least upper
//!   bound (LUB) of themselves and the child type.
//!
//! - `UnknownDesc`: These types start at `Universe` and descend down the type
//!   hierarchy as needed. When encountered as a child, they are updated to the
//!   greatest lower bound (GLB) of themselves and the parent type.
//!
//! The solver continues until no more changes can be made, at which point it either
//! reports success or returns the most relevant type error.

use super::registry::{Constraint, Type, TypeRegistry};
use crate::dsl::{
    analyzer::{
        errors::AnalyzerErrorKind,
        hir::{Identifier, Pattern, PatternKind, TypedSpan},
        type_checks::registry::{Generic, TypeKind},
    },
    utils::span::Span,
};
use std::mem;

impl TypeRegistry {
    /// Resolves all collected constraints and fills in the concrete types.
    ///
    /// This method implements the main constraint solving algorithm, iterating through
    /// all constraints and refining unknown types until either all constraints are
    /// satisfied or we reach a fixed point where no more progress can be made.
    ///
    /// # Algorithm
    ///
    /// 1. The algorithm iteratively processes all constraints, tracking whether any
    ///    type refinements were made in each iteration.
    /// 2. For each constraint, we attempt to satisfy it by refining unknown types.
    /// 3. We continue until reaching a fixed point (no more changes) or until all
    ///    constraints are satisfied.
    /// 4. If constraints remain unsatisfied at the fixed point, we return the most
    ///    relevant type error.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if all constraints are successfully resolved.
    /// * `Err(error)` containing the last encountered type error when no further progress could be made.
    pub fn resolve(&mut self) -> Result<(), Box<AnalyzerErrorKind>> {
        let mut any_changed = true;
        let mut last_error = None;

        while any_changed {
            any_changed = false;
            last_error = None;

            // Temporarily take ownership of constraints to avoid borrow checker issues.
            let constraints = mem::take(&mut self.constraints);

            for constraint in &constraints {
                let mut constraint_changed = false;
                match self.check_constraint(constraint, &mut constraint_changed) {
                    Ok(()) => {
                        any_changed |= constraint_changed;
                    }
                    Err(err) => {
                        // Store the error but continue processing other constraints.
                        any_changed |= constraint_changed;
                        last_error = Some(err);
                    }
                }
            }

            // Put constraints back.
            self.constraints = constraints;
        }

        // Only return an error if no more progress can be made and we have an error.
        match last_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Checks if a single constraint is satisfied, potentially refining unknown types.
    ///
    /// This method dispatches to type-specific constraint checkers based on the
    /// constraint kind (subtype, call, scrutinee, or field access).
    ///
    /// # Arguments
    ///
    /// * `constraint` - The constraint to check
    /// * `changed` - Mutable flag that is set to true if any types were refined
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the constraint is satisfied or could be satisfied through refinement.
    /// * `Err(error)` if the constraint cannot be satisfied.
    fn check_constraint(
        &mut self,
        constraint: &Constraint,
        changed: &mut bool,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        match constraint {
            Constraint::Subtype { child, parent } => {
                self.check_subtype_constraint(child, parent, changed)
            }

            Constraint::Call {
                id,
                inner,
                args,
                outer,
            } => self.check_call_constraint(*id, inner, args, outer, changed),

            Constraint::Scrutinee { scrutinee, pattern } => {
                self.check_scrutinee_constraint(scrutinee, pattern, changed)
            }

            Constraint::FieldAccess {
                inner,
                field,
                outer,
            } => self.check_field_access_constraint(inner, field, outer, changed),
        }
    }

    /// Checks if a subtyping constraint is satisfied, refining unknown types if needed.
    ///
    /// This method verifies that `child` is a subtype of `parent_ty`, potentially
    /// refining unknown types to satisfy this relationship.
    ///
    /// # Arguments
    ///
    /// * `child` - The child type with its source location
    /// * `parent_ty` - The parent type
    /// * `changed` - Mutable flag that is set to true if any types were refined
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the subtyping relationship is satisfied or could be satisfied.
    /// * `Err(error)` if the subtyping relationship cannot be satisfied.
    fn check_subtype_constraint(
        &mut self,
        child: &TypedSpan,
        parent_ty: &Type,
        changed: &mut bool,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        self.is_subtype_infer(&child.ty, parent_ty, changed)
            .then_some(())
            .ok_or_else(|| {
                AnalyzerErrorKind::new_invalid_subtype(
                    &child.ty,
                    parent_ty,
                    &child.span,
                    self.resolved_unknown.clone(),
                )
            })
    }

    /// Instantiates a type by replacing all generic type references with concrete types
    /// specific to the given constraint.
    ///
    /// This method handles generics by creating unique type instantiations for each
    /// constraint context. When encountering a generic type, it either retrieves an
    /// existing instantiation or creates a new one based on the variance direction.
    /// If the generic has a bound, it ensures the instantiated type satisfies that bound.
    ///
    /// # Arguments
    ///
    /// * `ty` - The type to instantiate
    /// * `constraint_id` - The ID of the constraint that needs this instantiation
    /// * `span` - The span where the expression occurs for error reporting
    /// * `ascending` - Whether unknown types should be created as ascending (true) or descending (false)
    ///
    /// # Returns
    ///
    /// A Result containing either the instantiated type or an error if bound constraints cannot be satisfied.
    fn instantiate_type(
        &mut self,
        ty: &Type,
        constraint_id: usize,
        span: &Span,
        ascending: bool,
    ) -> Result<Type, Box<AnalyzerErrorKind>> {
        use TypeKind::*;

        let kind = match &*ty.value {
            Gen(generic @ Generic(_, bound)) => {
                // Try to find an existing instantiation for this generic in this constraint.
                let existing = self
                    .instantiated_generics
                    .get(&constraint_id)
                    .and_then(|vec| {
                        vec.iter()
                            .find(|(g, _)| g == generic)
                            .map(|(_, ty)| ty.clone())
                    });

                // If found, return the existing instantiation.
                if let Some(existing_type) = existing {
                    existing_type
                } else {
                    // Otherwise, create a new instantiation.
                    let next_id = if ascending {
                        self.new_unknown_asc()
                    } else {
                        self.new_unknown_desc()
                    };

                    // Get or create the vector for this constraint_id.
                    let entry = self.instantiated_generics.entry(constraint_id).or_default();
                    entry.push((generic.clone(), next_id.clone()));

                    // Check the bound constraint.
                    if let Some(bound) = bound {
                        let next_id_ty: Type = next_id.clone().into();
                        self.check_subtype_constraint(
                            &TypedSpan::new(next_id_ty, span.clone()),
                            bound,
                            &mut false, // The unknown type appears no-where else as it has been introduced here.
                        )
                        .unwrap();
                    }

                    next_id
                }
            }

            // For composite types, recursively instantiate their component types.
            Array(elem_type) => {
                Array(self.instantiate_type(elem_type, constraint_id, span, ascending)?)
            }

            Closure(param_type, return_type) => Closure(
                self.instantiate_type(param_type, constraint_id, span, !ascending)?,
                self.instantiate_type(return_type, constraint_id, span, ascending)?,
            ),

            Tuple(types) => {
                let instantiated_types = types
                    .iter()
                    .map(|t| self.instantiate_type(t, constraint_id, span, ascending))
                    .collect::<Result<Vec<_>, _>>()?;
                Tuple(instantiated_types)
            }

            Map(key_type, value_type) => Map(
                self.instantiate_type(key_type, constraint_id, span, !ascending)?,
                self.instantiate_type(value_type, constraint_id, span, ascending)?,
            ),

            Optional(inner_type) => {
                Optional(self.instantiate_type(inner_type, constraint_id, span, ascending)?)
            }
            Stored(inner_type) => {
                Stored(self.instantiate_type(inner_type, constraint_id, span, ascending)?)
            }
            Costed(inner_type) => {
                Costed(self.instantiate_type(inner_type, constraint_id, span, ascending)?)
            }

            // Primitive types, special types, and ADT references don't need instantiation.
            _ => *ty.value.clone(),
        };

        Ok(Type {
            value: kind.into(),
            span: ty.span.clone(),
        })
    }

    /// Checks a function call constraint, ensuring parameter and return types match.
    ///
    /// This method handles function calls, array indexing, and map lookups by verifying
    /// that the arguments match the expected parameter types and that the return type
    /// is compatible with the expected output type.
    ///
    /// # Arguments
    ///
    /// * `constraint_id` - The ID of the constraint for instantiating generic functions
    /// * `inner` - The function/container being called/indexed
    /// * `args` - The arguments/indices provided to the call
    /// * `outer` - The expected return type
    /// * `changed` - Mutable flag that is set to true if any types were refined
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the call constraint is satisfied or could be satisfied.
    /// * `Err(error)` if the call constraint cannot be satisfied.
    fn check_call_constraint(
        &mut self,
        constraint_id: usize,
        inner: &TypedSpan,
        args: &[TypedSpan],
        outer: &TypedSpan,
        changed: &mut bool,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let inner_resolved = self.resolve_type(&inner.ty);

        match &*inner_resolved.value {
            TypeKind::Nothing => Ok(()),

            TypeKind::Closure(param, ret) => {
                // Instantiate potential generics.
                let ret = self.instantiate_type(ret, constraint_id, &inner.span, true)?;

                let (param_len, param_types) = match &*param.value {
                    TypeKind::Tuple(types) => {
                        let instantiated_types = types
                            .iter()
                            .zip(args.iter())
                            .map(|(param_type, arg)| {
                                self.instantiate_type(param_type, constraint_id, &arg.span, true)
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        (types.len(), instantiated_types)
                    }
                    TypeKind::Unit => (0, vec![]),
                    _ => {
                        let param_inst =
                            self.instantiate_type(param, constraint_id, &args[0].span, true)?;
                        (1, vec![param_inst])
                    }
                };

                if param_len != args.len() {
                    return Err(AnalyzerErrorKind::new_argument_number_mismatch(
                        &inner.span,
                        param_len,
                        args.len(),
                    ));
                }

                args.iter()
                    .zip(param_types.iter())
                    .try_for_each(|(arg, param_type)| {
                        self.check_subtype_constraint(arg, param_type, changed)
                    })?;

                self.check_subtype_constraint(
                    &TypedSpan::new(ret.clone(), inner.span.clone()),
                    &outer.ty,
                    changed,
                )
            }

            TypeKind::Map(key_type, val_type) => {
                self.check_indexable(&inner.span, args, key_type, val_type, &outer.ty, changed)
            }

            TypeKind::Array(elem_type) => {
                let index_type = TypeKind::I64.into();
                self.check_indexable(
                    &inner.span,
                    args,
                    &index_type,
                    elem_type,
                    &outer.ty,
                    changed,
                )
            }

            _ => Err(AnalyzerErrorKind::new_invalid_call_receiver(
                &inner_resolved,
                &inner.span,
                self.resolved_unknown.clone(),
            )),
        }
    }

    /// Helper method for checking indexing operations on arrays and maps.
    ///
    /// This method verifies that an indexing operation is valid by checking that:
    /// 1. Exactly one argument/index is provided
    /// 2. The index type matches the expected key type
    /// 3. The resulting element type (wrapped in Optional) is compatible with the expected output
    ///
    /// # Arguments
    ///
    /// * `span` - The source location of the indexing operation
    /// * `args` - The arguments/indices provided (should be exactly one)
    /// * `key_type` - The expected type of the index
    /// * `elem_type` - The type of elements in the container
    /// * `outer_ty` - The expected output type
    /// * `changed` - Mutable flag that is set to true if any types were refined
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the indexing constraint is satisfied or could be satisfied.
    /// * `Err(error)` if the indexing constraint cannot be satisfied.
    fn check_indexable(
        &mut self,
        span: &Span,
        args: &[TypedSpan],
        key_type: &Type,
        elem_type: &Type,
        outer_ty: &Type,
        changed: &mut bool,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        if args.len() != 1 {
            return Err(AnalyzerErrorKind::new_argument_number_mismatch(
                span,
                1,
                args.len(),
            ));
        }

        // Check index type.
        self.check_subtype_constraint(&args[0], key_type, changed)?;

        // Check element type (wrapped in Optional).
        let optional_elem_type = TypeKind::Optional(elem_type.clone()).into();
        self.check_subtype_constraint(
            &TypedSpan::new(optional_elem_type, span.clone()),
            outer_ty,
            changed,
        )
    }

    /// Checks a field access constraint, ensuring the field exists and has the correct type.
    ///
    /// This method verifies field access on ADTs and tuples by checking that:
    /// 1. For ADTs: the field exists and its type is compatible with the expected output
    /// 2. For tuples: the _N syntax is used with a valid index, and the type matches
    ///
    /// # Arguments
    ///
    /// * `inner` - The object whose field is being accessed
    /// * `field` - The name of the field being accessed
    /// * `outer` - The expected type of the field
    /// * `changed` - Mutable flag that is set to true if any types were refined
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the field access constraint is satisfied or could be satisfied.
    /// * `Err(error)` if the field access constraint cannot be satisfied.
    fn check_field_access_constraint(
        &mut self,
        inner: &TypedSpan,
        field: &Identifier,
        outer: &Type,
        changed: &mut bool,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        let inner_resolved = self.resolve_type(&inner.ty);

        // Function to create the standard field access error.
        let field_error = || {
            AnalyzerErrorKind::new_invalid_field_access(
                &inner_resolved,
                &inner.span,
                field,
                self.resolved_unknown.clone(),
            )
        };

        match &*inner_resolved.value {
            // Wait for the field access to be resolved.
            TypeKind::Nothing => Ok(()),

            // Handle tuple field access with _N pattern.
            TypeKind::Tuple(types) => {
                // Parse _N pattern and check if index is valid.
                match field
                    .strip_prefix('_')
                    .and_then(|idx| idx.parse::<usize>().ok())
                {
                    Some(index) if index < types.len() => {
                        let field_ty = &types[index];
                        self.check_subtype_constraint(
                            &TypedSpan::new(field_ty.clone(), inner.span.clone()),
                            outer,
                            changed,
                        )
                    }
                    _ => Err(field_error()),
                }
            }

            // Handle ADT field access.
            TypeKind::Adt(name) => match self.get_product_field_type(name, field) {
                Some(field_ty) => self.check_subtype_constraint(
                    &TypedSpan::new(field_ty, inner.span.clone()),
                    outer,
                    changed,
                ),
                None => Err(field_error()),
            },

            // Any other type cannot have fields accessed.
            _ => Err(field_error()),
        }
    }

    /// Checks a pattern matching constraint, ensuring the scrutinee matches the pattern.
    ///
    /// This method handles various pattern matching constructs including binding,
    /// struct destructuring, array decomposition, and wildcards by ensuring type
    /// compatibility between the scrutinee and pattern.
    ///
    /// # Arguments
    ///
    /// * `scrutinee` - The expression being matched
    /// * `pattern` - The pattern to match against
    /// * `changed` - Mutable flag that is set to true if any types were refined
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the pattern matching constraint is satisfied or could be satisfied.
    /// * `Err(error)` if the pattern matching constraint cannot be satisfied.
    fn check_scrutinee_constraint(
        &mut self,
        scrutinee: &TypedSpan,
        pattern: &Pattern<TypedSpan>,
        changed: &mut bool,
    ) -> Result<(), Box<AnalyzerErrorKind>> {
        use PatternKind::*;
        use TypeKind::*;

        let scrutinee_ty = self.resolve_type(&scrutinee.ty);
        if matches!(*scrutinee_ty.value, TypeKind::Nothing) {
            // Wait for the scrutinee to be resolved.
            return Ok(());
        }

        match &pattern.kind {
            Bind(_, sub_pattern) => {
                self.check_scrutinee_constraint(scrutinee, sub_pattern, changed)
            }

            Struct(name, field_patterns) => {
                field_patterns
                    .iter()
                    .enumerate()
                    .try_for_each(|(i, field_pat)| {
                        let field_type = self.get_product_field_type_by_index(name, i).unwrap();
                        let field_scrutinee =
                            TypedSpan::new(field_type, field_pat.metadata.span.clone());

                        self.check_scrutinee_constraint(&field_scrutinee, field_pat, changed)
                    })?;

                self.check_subtype_constraint(&pattern.metadata, &scrutinee.ty, changed)
            }

            Operator(_) => panic!("Operators may not be in the HIR yet"),

            ArrayDecomp(head, tail) => {
                if let Array(ty) = &*scrutinee_ty.value {
                    // First check head pattern against element type.
                    let inner_scrutinee = TypedSpan::new(ty.clone(), scrutinee.span.clone());
                    self.check_scrutinee_constraint(&inner_scrutinee, head, changed)?;

                    // Then check tail pattern.
                    self.check_scrutinee_constraint(scrutinee, tail, changed)
                } else {
                    Err(AnalyzerErrorKind::new_invalid_array_decomposition(
                        &scrutinee.span,
                        &pattern.metadata.span,
                        &scrutinee_ty,
                        self.resolved_unknown.clone(),
                    ))
                }
            }

            EmptyArray | Literal(_) => {
                self.check_subtype_constraint(&pattern.metadata, &scrutinee.ty, changed)
            }

            Wildcard => {
                // First check scrutinee against wildcard type.
                self.check_subtype_constraint(scrutinee, &pattern.metadata.ty, changed)?;

                // Then check wildcard pattern against scrutinee type.
                self.check_subtype_constraint(&pattern.metadata, &scrutinee_ty, changed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dsl::{
        compile::{Config, compile_hir},
        utils::errors::CompileError,
    };
    use std::collections::HashMap;

    fn run_type_inference(source: &str, source_path: &str) -> Result<(), Vec<CompileError>> {
        // Append the required core type declarations to each test program.
        let core_types = r#"
            data Logical
            data Physical
            data LogicalProperties
            data PhysicalProperties
        "#;

        let full_source = format!("{}\n{}", core_types, source);

        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let file_path = temp_dir.path().join(source_path);

        std::fs::write(&file_path, full_source).expect("Failed to write to temp file");

        let config = Config::new(file_path);
        let udfs = HashMap::new();
        let _ = compile_hir(config, udfs)?;

        Ok(())
    }

    #[test]
    fn test_valid_higher_order_function() {
        let source = r#"
        data Expr = 
            | BinOp(left: Expr, op: String, right: Expr)
            | Num(value: I64)
            \ Var(name: String)

        fn (e: Expr) map(f: (Expr) -> Expr) = match e
            | BinOp(l, o, r) -> BinOp(f(l), o, f(r))
            | Num(v) -> Num(v)
            \ Var(n) -> Var(n)

        fn main(): Expr = 
            let 
                expr = BinOp(Num(10), "+", Num(20)),
                add_one = (e: Expr) -> match e
                    | Num(v) -> Num(v + 1)
                    \ other -> other
            in
                expr.map(add_one)
        "#;

        let result = run_type_inference(source, "higher_order.opt");
        assert!(
            result.is_ok(),
            "Valid higher-order function program failed type inference"
        );
    }

    #[test]
    fn test_pattern_matching_with_adts() {
        let source = r#"
        data Shape =
            | Circle(radius: F64)
            | Rectangle(width: F64, height: F64)
            \ Triangle(a: F64, b: F64, c: F64)

        fn (s: Shape?) area = match s
            | Circle(r) -> 3.14 * r * r
            | Rectangle(w, h) -> w * h
            \ Triangle(a, b, c) -> 
                let p = (a + b + c) / 2.0 in
                (p * (p - a) * (p - b) * (p - c))

        fn main(): F64 = 
            let shapes = [Circle(5.0), Rectangle(4.0, 6.0), Triangle(3.0, 4.0, 5.0)] in
            shapes(0).area()
        "#;

        let result = run_type_inference(source, "pattern_matching.opt");
        assert!(
            result.is_ok(),
            "Valid pattern matching program failed type inference"
        );
    }

    #[test]
    fn test_type_inheritance() {
        let source = r#"
        data Animal = 
            | Mammal =
            | Dog(name: String, breed: String)
            \ Cat(name: String, lives: String)
                \ Bird(name: String, can_fly: Bool)
        
            fn describe(animal: Animal): String = match animal
                | Dog(name, breed) -> name ++ " is a " ++ breed ++ " dog"
                | Cat(name, lives) -> name ++ " is a cat with " ++ lives ++ " lives"
                \ Bird(name, can_fly) -> 
                    if can_fly then 
                        name ++ " is a bird that can fly" 
                    else 
                        name ++ " is a bird that cannot fly"
        
            fn main2(): String = 
                let 
                    dog = Dog("Rex", "German Shepherd"),
                    cat = Cat("Whiskers", "9"),
                    bird = Bird("Tweety", true)
                in
                    describe(dog) ++ "\n" ++ describe(cat) ++ "\n" ++ describe(bird)
        "#;

        let result = run_type_inference(source, "inheritance.opt");
        assert!(
            result.is_ok(),
            "Valid inheritance program failed type inference"
        );
    }

    #[test]
    fn test_type_mismatch_error() {
        let source = r#"
        data Counter(value: I64)

        fn increment(c: Counter): Counter = Counter(c#value + 1)
        fn add(a: I64, b: I64): I64 = a + b

        // Error: add returns I64 but increment expects Counter
        fn main(): Counter = increment(add(5, 3))
        "#;

        let result = run_type_inference(source, "type_mismatch.opt");
        assert!(
            result.is_err(),
            "Invalid program with type mismatch didn't fail type inference"
        );
    }

    #[test]
    fn test_incompatible_inheritance_error() {
        let source = r#"
        data Vehicle = 
            | Car(make: String, model: String)
            \ Boat(name: String, length: F64)

        data Animal = 
            | Dog(name: String)
            \ Cat(name: String)

        fn drive(v: Car) = v#make ++ " " ++ v#model ++ " is being driven"

        fn main(): String = 
            let 
                myCar = Car("Toyota", "Camry"),
                myDog = Dog("Rex")
            in
                drive(myDog)  // Error: Dog is not a Car
        "#;

        let result = run_type_inference(source, "incompatible_inheritance.opt");
        assert!(
            result.is_err(),
            "Invalid program with incompatible inheritance didn't fail type inference"
        );
    }

    #[test]
    fn test_complex_higher_order_functions() {
        let source = r#"
        data Expr = 
            | BinOp(left: Expr, op: String, right: Expr)
            | Num(value: I64)
            \ Var(name: String)

        fn (e: Expr) fold(f: (I64, String, I64) -> I64): I64 = match e
            | BinOp(Num(l), o, Num(r)) -> f(l, o, r)
            | BinOp(l, o, r) -> l.fold(f) + r.fold(f)
            | Num(v) -> v
            \ Var(_) -> 0

        fn calculate(left: I64, op: String, right: I64) = match op
            | "+" -> left + right
            | "-" -> left - right
            | "*" -> left * right
            \ "/" -> left / right

        fn main(): I64 = 
            let 
                expr = BinOp(BinOp(Num(5), "+", Num(3)), "*", Num(2)),
                result = expr.fold(calculate)
            in
                result
        "#;

        let result = run_type_inference(source, "complex_higher_order.opt");
        assert!(
            result.is_ok(),
            "Valid complex higher-order functions program failed type inference"
        );
    }

    #[test]
    fn test_function_type_inference() {
        let source = r#"
        data Num(value: I64)

        fn apply(f: (I64) -> I64, n: I64) = f(n)
        
        fn main() = 
            let 
                double = (x: I64) -> x * 2,
                triple = (x: I64) -> x * 3,
                compose = (f: (I64) -> I64, g: (I64) -> I64) -> 
                    (x: I64) -> f(g(x))
            in
                apply(compose(double, triple), 5)  // Should be 2 * (3 * 5) = 30
        "#;

        let result = run_type_inference(source, "function_inference.opt");
        assert!(
            result.is_ok(),
            "Valid function type inference program failed"
        );
    }

    #[test]
    fn test_type_inference_in_recursive_functions() {
        let source = r#"
        data List = 
            | Cons(head: I64, tail: List)
            \ Nil

        fn sum(list: List) = match list
            | Cons(head, tail) -> head + sum(tail)
            \ Nil -> 0

        fn factorial(n: I64) = 
            if n <= 1 then 
                1 
            else 
                n * factorial(n - 1)

        fn main(): I64 = 
            let 
                list = Cons(1, Cons(2, Cons(3, Nil))),
                fact5 = factorial(5)
            in
                sum(list) + fact5
        "#;

        let result = run_type_inference(source, "recursive_functions.opt");
        assert!(
            result.is_ok(),
            "Valid recursive functions program failed type inference"
        );
    }

    #[test]
    fn test_unannotated_function_parameters() {
        let source = r#"
        data Num(value: I64)
        
        fn add(a: I64, b: I64) = a + b
        
        fn apply(f: I64 -> I64, n: I64) = f(n)
        
        fn main() = 
            let
                double = x -> 2,
                number = 5,
                result = add(number, apply(double, 10))
            in
                result
        "#;

        let result = run_type_inference(source, "unannotated_parameters.opt");
        assert!(
            result.is_ok(),
            "Type inference for unannotated parameters failed"
        );
    }

    #[test]
    fn test_complex_adt_hierarchy() {
        let source = r#"
        data Expression = 
            | Literal = 
                | IntLiteral(value: I64)
                | BoolLiteral(value: Bool)
                \ StringLiteral(value: String)
            | BinaryOp = 
                | Arith = 
                    | Add(left: Expression, right: Expression)
                    | Subtract(left: Expression, right: Expression)
                    | Multiply(left: Expression, right: Expression)
                    \ Divide(left: Expression, right: Expression)
                \ Logic = 
                    | And(left: Expression, right: Expression)
                    | Or(left: Expression, right: Expression)
                    \ Not(expr: Expression)
            \ Variable(name: String)

        fn evaluate(expr: Expression): I64 = match expr
            | IntLiteral(value) -> value
            | Add(left, right) -> evaluate(left) + evaluate(right)
            | Subtract(left, right) -> evaluate(left) - evaluate(right)
            | Multiply(left, right) -> evaluate(left) * evaluate(right)
            | Divide(left, right) -> evaluate(left) / evaluate(right)
            \ _ -> 0  // Simplified for other cases

        fn main(): I64 = 
            evaluate(Add(IntLiteral(10), Multiply(IntLiteral(3), IntLiteral(4))))
        "#;

        let result = run_type_inference(source, "complex_adt_hierarchy.opt");
        assert!(
            result.is_ok(),
            "Valid complex ADT hierarchy program failed type inference"
        );
    }

    #[test]
    fn test_list_pattern_matching() {
        let source = r#"
        data Foo = 
            | Add(left: Foo, right: Foo)
            | List(bla: [Foo])
            \ Const(val: I64)
            
        fn (log: Foo) fold = x -> x
        
        fn arr_as_function(other: Foo, idx: I64, closure: I64 -> Foo?) = closure(idx)
        
        fn main(log: Foo): Foo? = match log
            | Add(Add(_,_), right: Const(val)) -> {
                let closure = x -> right in
                closure(5)
            }
            | fooo -> arr_as_function(fooo, 0, [log])
            | List(bla) -> match bla
                | vla -> vla(0)
                \ [] -> none
            | Add(left, right) -> left
            \ _ -> fail("brru")
            
        fn (log: [Foo]) vla: [Foo] = match log
            \ [Const(5) .. [List([Const(5) .. _]) .. v]] -> v
        "#;

        let result = run_type_inference(source, "list_pattern_matching.opt");
        assert!(
            result.is_ok(),
            "Valid list pattern matching program failed type inference"
        );
    }

    #[test]
    fn test_complex_list_decomposition() {
        let source = r#"
        data Foo = 
            | Add(left: Foo, right: Foo)
            | List(bla: [Foo])
            \ Const(val: I64)
            
        // Test different array decomposition patterns
        fn process_array(arr: [Foo]): I64 = match arr
            | [] -> 0
            | [x .. _] -> match x
                | Const(val) -> val
                \ _ -> 1
            | [x .. [y .. _]] -> match x
                | Const(a) -> a
                \ _ -> 2
            | [Const(a) .. rest] -> a + process_array(rest)
            \ _ -> -1
            
        fn main(): I64 = 
            let array = [Const(1), Const(2), Const(3), Const(4)] in
            process_array(array)
        "#;

        let result = run_type_inference(source, "complex_list_decomposition.opt");
        assert!(
            result.is_ok(),
            "Valid complex list decomposition program failed type inference"
        );
    }

    #[test]
    fn test_nested_list_patterns() {
        let source = r#"
        data Foo = 
            | Add(left: Foo, right: Foo)
            | List(bla: [Foo])
            \ Const(val: I64)
            
        fn process_nested(nested: [[Foo]]): I64 = match nested
            | [] -> 0
            | [[] .. _] -> 1
            | [[Const(x) .. _] .. rest] -> x + process_nested(rest)
            | [[Add(Const(a), Const(b)) .. _] .. rest] -> a + b + process_nested(rest)
            \ _ -> -1
            
        fn main(): I64 = process_nested([[Add(Const(2), Const(3))]])
        "#;

        let result = run_type_inference(source, "nested_list_patterns.opt");
        assert!(
            result.is_ok(),
            "Valid nested list patterns program failed type inference"
        );
    }

    #[test]
    fn test_list_pattern_type_mismatch() {
        let source = r#"
        data Foo = 
        | Add(left: Foo, right: Foo)
        | List(bla: [Foo])
        \ Const(val: I64)
    
        data Bar(value: String)
        
        // This should fail because we're trying to match a list of Bar 
        // with a pattern expecting a list of Foo
        fn process_incorrect_types(bars: [Bar]): I64 = match bars
            | [Const(5) .. _] -> 1  // Type mismatch: Bar is not compatible with Const
            \ _ -> 0
            
        fn main(): I64 = 
            let bars = [Bar("test")] in
            process_incorrect_types(bars)
        "#;

        let result = run_type_inference(source, "list_pattern_type_mismatch.opt");
        assert!(
            result.is_err(),
            "Type mismatch in list pattern should have failed type inference"
        );
    }

    #[test]
    fn test_array_decomp_on_non_array() {
        let source = r#"
        data Foo = 
            | Add(left: Foo, right: Foo)
            | List(bla: [Foo])
            \ Const(val: I64)
        
        // This should fail because we're using array decomposition on a non-array type
        fn process_non_array(foo: Foo): I64 = match foo
            | [head .. tail] -> 1  // Error: Foo is not an array type
            \ _ -> 0
            
        fn main(): I64 = 
            let foo = Const(42) in
            process_non_array(foo)
        "#;

        let result = run_type_inference(source, "array_decomp_on_non_array.opt");
        assert!(
            result.is_err(),
            "Array decomposition on non-array type should have failed type inference"
        );
    }

    #[test]
    fn test_inconsistent_list_pattern() {
        let source = r#"
        data Foo = 
        | Add(left: Foo, right: Foo)
        \ Const(val: I64)
    
        // This should fail because the pattern is inconsistent in its typing
        fn process_mixed_list(mixed: [I64]): I64 = match mixed
            | [x .. [Const(y) .. rest]] -> x + y  // Error: Const is Foo type but we're matching against [I64]
            \ _ -> 0
            
        fn main(): I64 = 
            let nums = [1, 2, 3] in
            process_mixed_list(nums)
        "#;

        let result = run_type_inference(source, "inconsistent_list_pattern.opt");
        assert!(
            result.is_err(),
            "Inconsistent types in list pattern should have failed type inference"
        );
    }

    #[test]
    fn test_nested_pattern_type_mismatch() {
        let source = r#"
        data Foo = 
        | Add(left: Foo, right: Foo)
        | List(bla: [Foo])
        \ Const(val: I64)
    
        data Bar = 
            | Text(value: String)
            \ Number(value: F64)
        
        // This should fail due to type mismatch in nested patterns
        fn process_nested(items: [[Foo]]): I64 = match items
            | [] -> 0
            | [[Text("w") .. []] .. rest] -> 1  // Error: Text is not compatible with Foo
            \ _ -> -1
            
        fn main(): I64 =
            let dat = [
                [Const(5)],
                [Add(Const(2), Const(3))]
            ] in
            process_nested(dat)
        "#;

        let result = run_type_inference(source, "nested_pattern_type_mismatch.opt");
        assert!(
            result.is_err(),
            "Type mismatch in nested pattern should have failed type inference"
        );
    }

    #[test]
    fn test_incompatible_array_usage() {
        let source = r#"
        data Foo = 
            | Add(left: Foo, right: Foo)
            | List(bla: [Foo])
            \ Const(val: I64)
        
        // This should fail because we're trying to use the array as a function with wrong parameter type
        fn main(): Foo = 
            let arr = [Const(1), Const(2)] in
            arr("index")  // Error: String index for array that expects I64
        "#;

        let result = run_type_inference(source, "incompatible_array_usage.opt");
        assert!(
            result.is_err(),
            "Using string index on array should have failed type inference"
        );
    }

    #[test]
    fn test_basic_generic_function() {
        let source = r#"
        // Basic generic function for array mapping
        fn <T, U> (input: [T]) map(transform: T -> U): [U] = match input
            | [] -> []
            \ [x .. xs] -> [transform(x)] ++ xs.map(transform)

        fn main(): [I64] = 
            let input = [1, 2, 3] in
            input.map(x: I64 -> x + 1)  // Should return [2, 3, 4]
        "#;

        let result = run_type_inference(source, "basic_generics.opt");
        assert!(
            result.is_ok(),
            "Basic generic function failed type inference: {:?}",
            result
        );
    }

    #[test]
    fn test_complex_generic_function_composition() {
        let source = r#"
        // Generic function that takes another function as argument
        fn <A, B, C> compose(f: B -> C, g: A -> B): A -> C = 
            x: A -> f(g(x))
       
        // Generic function that maps over arrays
        fn <T, U> (input: [T]) map(transform: T -> U): [U] = match input
            | [] -> []
            \ [x .. xs] -> [transform(x)] ++ xs.map(transform)
        
        // Generic function that filters array elements
        fn <T> (input: [T]) filter(predicate: T -> Bool): [T] = match input
            | [] -> []
            \ [x .. xs] -> 
                if predicate(x) then 
                    [x] ++ xs.filter(predicate)
                else 
                    xs.filter(predicate)
        
        fn main(): [I64] = 
            let 
                numbers = [1, 2, 3, 4, 5, 6],
                is_greater_two = x: I64 -> x > 2,
                double = x: I64 -> x * 2,
                
                // Compose filter and map
                process = compose(
                    xs: [I64] -> xs.map(double),
                    xs: [I64] -> xs.filter(is_greater_two)
                )
            in
                process(numbers)
        "#;

        let result = run_type_inference(source, "complex_generics.opt");
        assert!(
            result.is_ok(),
            "Complex generic function composition failed type inference: {:?}",
            result
        );
    }

    #[test]
    fn test_generic_function_with_recursive_closures() {
        let source = r#"
        // Generic functions with recursive closures
        fn <T, U> memoize(f: T -> U): T -> U =
            let
                cache = [] // Simplified cache
            in
                (x: T) -> {
                    // In a real implementation, we would check the cache
                    // and only compute if needed
                    f(x)
                }
    
        fn <T> (array: [T]) quicksort(cmp: (T, T) -> I64) =
            match array
                | [] -> []
                \ [pivot .. rest] -> {
                    let
                        partition = (arr: [T], pivot: T, cmp: (T, T) -> I64) -> {
                            let
                                less = arr.filter(x: T -> cmp(x, pivot) < 0),
                                greater = arr.filter(x: T -> cmp(x, pivot) >= 0)
                            in
                                (less, greater)
                        },
                        result = partition(rest, pivot, cmp),
                        less = result#_0,
                        greater = result#_1,
                        sorted_less = less.quicksort(cmp),
                        sorted_greater = greater.quicksort(cmp)
                    in
                        sorted_less ++ [pivot] ++ sorted_greater
                }
    
        fn <T> (array: [T]) filter(predicate: T -> Bool) = match array
            | [] -> []
            \ [x .. xs] ->
                if predicate(x) then
                    [x] ++ xs.filter(predicate)
                else
                    xs.filter(predicate)
    
        // Fixed recursive function implementation
        fn factorial(n: I64): I64 = if n <= 1 then 1 else n * factorial(n - 1)
    
        fn main(): I64 =
            let
                numbers = [5, 3, 8, 1, 2, 9, 4, 7, 6],
                compare = (a: I64, b: I64) -> a - b,
                
                // Use regular factorial function instead of the Y-combinator approach
                memoized_factorial = memoize(factorial),
                
                // Sort numbers and filter
                sorted = numbers.quicksort(compare),
                filtered = sorted.filter(x: I64 -> x > 2),
                result = memoized_factorial(7)
            in
                result
        "#;

        let result = run_type_inference(source, "generic_recursive_closures.opt");
        assert!(
            result.is_ok(),
            "Generic functions with recursive closures failed type inference: {:?}",
            result
        );
    }

    #[test]
    fn test_generic_type_constraints() {
        // This should fail since we don't properly support type constraints/bounds yet
        let source = r#"        
        // Generic function that assumes numeric operations
        fn <T> sum(a: T, b: T): T = a + b  // This assumes T supports addition
        
        // Generic function that assumes comparison
        fn <T> max(a: T, b: T): T = 
            if a > b then a else b  // This assumes T supports comparison
        
        fn main(): I64 = 
            let 
                sum_int = sum(1, 2),
                sum_float = sum(1.5, 2.5),
                
                max_int = max(5, 3),
                max_float = max(5.5, 3.5)
            in
                sum_int + max_int
        "#;

        let result = run_type_inference(source, "generic_constraints.opt");
        assert!(
            result.is_err(),
            "Generic type constraints should have failed type inference"
        );
    }

    #[test]
    fn test_generic_with_supertype_bound() {
        let source = r#"
        // Small ADT type hierarchy
        data Animal = 
           | Mammal = 
               | Dog(name: String)
               \ Cat(name: String)
           \ Bird(name: String)
       
       // Generic function with a bound requiring type to be an Animal
       fn <E: Animal> describe(entity: E): String = 
           match entity
               | Dog(name) -> name ++ " is a dog"
               | Cat(name) -> name ++ " is a cat"
               \ Bird(name) -> name ++ " is a bird"
       
       
       fn main(): String = 
           let 
               // Create specific animal types
               dog = Dog("Rex"),
               cat = Cat("Whiskers"),
               bird = Bird("Tweety"),
           in        
               describe(dog)
       
        "#;

        let result = run_type_inference(source, "generic_with_supertype_bound.opt");
        assert!(
            result.is_ok(),
            "Generic function with supertype bound failed type inference: {:?}",
            result
        );
    }

    #[test]
    fn test_pairs_to_map() {
        let source = r#"
        // Convert array of key-value pairs to a map
        fn <K: EqHash, V> (pairs: [(K, V)]) to_map(): {K: V} = match pairs
            | [head .. tail] -> {head#_0: head#_1} ++ tail.to_map()
            \ [] -> {}
        
        fn main(): {String : I64} = 
        let 
            // Create pairs and convert to map
            pairs = [("a", 1), ("b", 2), ("c", 3)],
            map = pairs.to_map(),
        in
            map
        "#;

        let result = run_type_inference(source, "simple_to_map.opt");
        assert!(
            result.is_ok(),
            "Basic pairs to map conversion failed: {:?}",
            result
        );
    }

    #[test]
    fn test_pairs_to_map_without_eqhash() {
        let source = r#"
        // Convert array of key-value pairs to a map, but missing EqHash bound
        fn <K, V> (pairs: [(K, V)]) to_map(): {K: V} = match pairs
            | [head .. tail] -> {head#_0: head#_1} ++ tail.to_map()
            \ [] -> {}
        
        fn main(): {String: I64} = 
            let 
                // Create pairs and convert to map
                pairs = [("a", 1), ("b", 2), ("c", 3)],
                map = pairs.to_map()
            in
                map
        "#;

        let result = run_type_inference(source, "missing_eqhash.opt");
        assert!(
            result.is_err(),
            "Map creation without EqHash bound should have failed"
        );
    }

    #[test]
    fn test_with_exact_source() {
        let source = r#"
        // Convert array of key-value pairs to a map with EqHash constraint
        fn <K: EqHash, V> (pairs: [(K, V)]) to_map(): {K: V} = match pairs
            | [head .. tail] -> {head#_0: head#_1} ++ tail.to_map()
            \ [] -> {}
            
        // Simple filter function for arrays
        fn <T> (array: [T]) filter(pred: T -> Bool): [T] = match array
            | [] -> []
            \ [x .. xs] ->
                if pred(x) then [x] ++ xs.filter(pred)
                else xs.filter(pred)
                
        // Simple map function
        fn <T, U> (array: [T]) map(f: T -> U): [U] = match array
            | [] -> []
            \ [x .. xs] -> [f(x)] ++ xs.map(f)
            
        // Data type for our test
        data User(name: String, age: I64)
        
        fn main(): {String: I64} =
            let
                // Create some users
                users = [
                    User("Alice", 25),
                    User("Bob", 17),
                    User("Charlie", 30),
                    User("Diana", 15)
                ],
                
                // Filter for adults
                adults = users.filter((u: User) -> u#age >= 18),
                
                // Create name -> age map
                name_age_pairs = adults.map((u: User) -> (u#name, u#age)),
                
                // Convert to map
                age_map = name_age_pairs.to_map()
            in
                age_map
        "#;

        let result = run_type_inference(source, "exact_source_test.opt");
        assert!(
            result.is_ok(),
            "Type inference with exact source code failed: {:?}",
            result
        );
    }
}
