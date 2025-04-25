use super::registry::{Constraint, Type, TypeRegistry};
use crate::dsl::{
    analyzer::{
        errors::AnalyzerErrorKind,
        hir::{Identifier, TypedSpan},
        type_checks::registry::TypeKind,
    },
    utils::span::Span,
};
use std::mem;

/// Result type for constraint checking that tracks type changes even on error
type ConstraintResult = Result<bool, (Box<AnalyzerErrorKind>, bool)>;

impl TypeRegistry {
    /// Resolves all collected constraints and fills in the concrete types.
    ///
    /// This method iterates through all constraints, checking subtype relationships
    /// and refining unknown types until either all constraints are satisfied or
    /// a constraint cannot be satisfied and no more progress can be made.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if all constraints are successfully resolved
    /// * `Err(error)` containing the last encountered type error when no further progress could be made
    pub fn resolve(&mut self) -> Result<(), Box<AnalyzerErrorKind>> {
        let mut any_changed = true;
        let mut last_error = None;

        while any_changed {
            any_changed = false;
            last_error = None;

            // Temporarily take ownership of constraints to avoid borrow checker issues.
            let constraints = mem::take(&mut self.constraints);

            for constraint in &constraints {
                match self.check_constraint(constraint) {
                    Ok(changed) => {
                        any_changed |= changed;
                    }
                    Err((err, changed)) => {
                        // Store the error but continue processing other constraints.
                        any_changed |= changed;
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

    /// Checks if a single constraint is satisfied.
    ///
    /// # Returns
    ///
    /// * `Ok(bool)` - The constraint is satisfied, with a boolean indicating if any types were changed.
    /// * `Err((Box<AnalyzerErrorKind>, bool))` - The constraint failed, with the error and a boolean
    ///   indicating if any types were changed during the check.
    fn check_constraint(&mut self, constraint: &Constraint) -> ConstraintResult {
        match constraint {
            Constraint::Subtype { child, parent } => self.check_subtype_constraint(child, parent),

            Constraint::Call { inner, args, outer } => {
                self.check_call_constraint(inner, args, outer)
            }

            Constraint::FieldAccess {
                inner,
                field,
                outer,
            } => self.check_field_access_constraint(inner, field, outer),
        }
    }

    fn check_subtype_constraint(
        &mut self,
        child: &TypedSpan,
        parent_ty: &Type,
    ) -> ConstraintResult {
        let mut has_changed = false;
        let is_subtype = self.is_subtype_infer(&child.ty, parent_ty, &mut has_changed);

        if is_subtype {
            Ok(has_changed)
        } else {
            let err = AnalyzerErrorKind::new_invalid_subtype(
                &child.ty,
                parent_ty,
                &child.span,
                self.resolved_unknown.clone(),
            );
            Err((err, has_changed))
        }
    }

    fn check_call_constraint(
        &mut self,
        inner: &TypedSpan,
        args: &[TypedSpan],
        outer: &TypedSpan,
    ) -> ConstraintResult {
        let inner_resolved = self.resolve_type(&inner.ty);

        match &*inner_resolved.value {
            TypeKind::Nothing => Ok(false),

            TypeKind::Closure(param, ret) => {
                let (param_len, param_types) = match &**param {
                    TypeKind::Tuple(types) => (types.len(), types.to_vec()),
                    TypeKind::Unit => (0, vec![]),
                    _ => (1, vec![param.clone()]),
                };

                if param_len != args.len() {
                    return Err((
                        AnalyzerErrorKind::new_argument_number_mismatch(
                            &inner.span,
                            param_len,
                            args.len(),
                        ),
                        false,
                    ));
                }

                let param_result = args.iter().zip(param_types.iter()).try_fold(
                    false,
                    |acc_changes, (arg, param_type)| match self
                        .check_subtype_constraint(arg, param_type)
                    {
                        Ok(changed) => Ok(acc_changes | changed),
                        Err((err, changed)) => Err((err, acc_changes | changed)),
                    },
                );
                let ret_result = self.check_subtype_constraint(
                    &TypedSpan::new(ret.clone(), inner.span.clone()),
                    &outer.ty,
                );

                self.combine_results(param_result, ret_result)
            }

            TypeKind::Map(key_type, val_type) => {
                self.check_indexable(&inner.span, args, key_type, val_type, &outer.ty)
            }

            TypeKind::Array(elem_type) => {
                let index_type = TypeKind::I64.into();
                self.check_indexable(&inner.span, args, &index_type, elem_type, &outer.ty)
            }

            _ => Err((
                AnalyzerErrorKind::new_invalid_call_receiver(
                    &inner_resolved,
                    &inner.span,
                    self.resolved_unknown.clone(),
                ),
                false,
            )),
        }
    }

    fn check_indexable(
        &mut self,
        span: &Span,
        args: &[TypedSpan],
        key_type: &Type,
        elem_type: &Type,
        outer_ty: &Type,
    ) -> ConstraintResult {
        if args.len() != 1 {
            return Err((
                AnalyzerErrorKind::new_argument_number_mismatch(span, 1, args.len()),
                false,
            ));
        }

        let index_result = self.check_subtype_constraint(&args[0], key_type);
        let optional_elem_type = TypeKind::Optional(elem_type.clone()).into();
        let elem_result = self
            .check_subtype_constraint(&TypedSpan::new(optional_elem_type, span.clone()), outer_ty);

        self.combine_results(index_result, elem_result)
    }

    fn combine_results(
        &self,
        result1: ConstraintResult,
        result2: ConstraintResult,
    ) -> ConstraintResult {
        match (result1, result2) {
            (Ok(changed1), Ok(changed2)) => Ok(changed1 | changed2),
            (Ok(changed1), Err((err2, changed2))) => Err((err2, changed1 | changed2)),
            (Err((err1, changed1)), Ok(changed2)) => Err((err1, changed1 | changed2)),
            (Err((err1, changed1)), Err((_, changed2))) => Err((err1, changed1 | changed2)),
        }
    }

    fn check_field_access_constraint(
        &mut self,
        inner: &TypedSpan,
        field: &Identifier,
        outer: &Type,
    ) -> ConstraintResult {
        let inner_resolved = self.resolve_type(&inner.ty);

        match &*inner_resolved.value {
            TypeKind::Nothing => Ok(false),

            TypeKind::Adt(name) => {
                match self.get_product_field_type(name, field) {
                    Some(field_ty) => {
                        // Check that the field type is a subtype of the outer type.
                        self.check_subtype_constraint(
                            &TypedSpan::new(field_ty, inner.span.clone()),
                            outer,
                        )
                    }
                    None => Err((
                        AnalyzerErrorKind::new_invalid_field_access(
                            &inner_resolved,
                            &inner.span,
                            field,
                            self.resolved_unknown.clone(),
                        ),
                        false,
                    )),
                }
            }

            _ => Err((
                AnalyzerErrorKind::new_invalid_field_access(
                    &inner_resolved,
                    &inner.span,
                    field,
                    self.resolved_unknown.clone(),
                ),
                false,
            )),
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
                | Arithmetic = 
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
}
