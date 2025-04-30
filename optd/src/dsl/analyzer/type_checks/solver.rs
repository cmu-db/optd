use super::registry::{Constraint, Type, TypeRegistry};
use crate::dsl::{
    analyzer::{
        errors::AnalyzerErrorKind,
        hir::{Identifier, Pattern, PatternKind, TypedSpan},
        type_checks::registry::TypeKind,
    },
    utils::span::Span,
};
use std::mem;

impl TypeRegistry {
    /// Resolves all collected constraints and fills in the concrete types.
    ///
    /// This method iterates through all constraints, checking subtype relationships
    /// and refining unknown types until either all constraints are satisfied or
    /// a constraint cannot be satisfied and no more progress can be made.
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

    /// Checks if a single constraint is satisfied.
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
                // Initiatialize potential generics.
                let param = self.instantiate_type(param, constraint_id, true);
                let ret = self.instantiate_type(ret, constraint_id, true);

                let (param_len, param_types) = match &*param {
                    TypeKind::Tuple(types) => (types.len(), types.to_vec()),
                    TypeKind::Unit => (0, vec![]),
                    _ => (1, vec![param.clone()]),
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

                // Then check wildcard pattern against scrutinee type
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
    fn test_recursive_generic_datatypes() {
        let source = r#"
        // Generic tree data structure
        data Tree<T> = 
            | Node(value: T, left: Tree<T>, right: Tree<T>)
            \ Leaf(value: T)

        // Generic function to transform a tree
        fn <T, U> (tree: Tree<T>) map(transform: T -> U): Tree<U> = match tree
            | Leaf(value) -> Leaf(transform(value))
            \ Node(value, left, right) -> 
                Node(transform(value), left.map(transform), right.map(transform))

        // Generic function to fold a tree
        fn <T, U> (tree: Tree<T>) fold(f: (T, U, U) -> U, leaf_case: T -> U): U = match tree
            | Leaf(value) -> leaf_case(value)
            \ Node(value, left, right) -> 
                let 
                    left_result = left.fold(f, leaf_case),
                    right_result = right.fold(f, leaf_case)
                in
                    f(value, left_result, right_result)

        fn main(): I64 = 
            let 
                tree = Node(1, 
                            Node(2, Leaf(3), Leaf(4)), 
                            Leaf(5)),
                
                // Transform tree with map
                doubled_tree = tree.map(x: I64 -> x * 2),
                
                // Fold to get sum
                sum = doubled_tree.fold(
                    (value: I64, left: I64, right: I64) -> value + left + right,
                    value: I64 -> value
                )
            in
                sum  // Should return 30 (2 + 4 + 6 + 8 + 10)
        "#;

        let result = run_type_inference(source, "recursive_generic_datatypes.opt");
        assert!(
            result.is_ok(),
            "Recursive generic datatypes failed type inference: {:?}",
            result
        );
    }

    #[test]
    fn test_nested_generics_with_tuples() {
        let source = r#"
    // Generic Either type
    data Either<L, R> = 
        | Left(value: L)
        \ Right(value: R)

    // Generic Result type built on Either
    data Result<T, E> = Either<T, E>
    
    // Generic functions for Result handling
    fn <T, E, U> (result: Result<T, E>) map_ok(f: T -> U): Result<U, E> = match result
        | Left(value) -> Left(f(value))
        \ Right(err) -> Right(err)
    
    fn <T, E, F> (result: Result<T, E>) map_err(f: E -> F): Result<T, F> = match result
        | Left(value) -> Left(value)
        \ Right(err) -> Right(f(err))
    
    // Generic function to chain Results
    fn <T, E, U> (result: Result<T, E>) and_then(f: T -> Result<U, E>): Result<U, E> = match result
        | Left(value) -> f(value)
        \ Right(err) -> Right(err)
    
    // Test with tuples and nested generics
    fn compute(x: I64): Result<(I64, I64), String> =
        if x > 0 then
            Left((x, x * x))
        else
            Right("Value must be positive")
    
    fn main(): I64 = 
        let 
            result1 = compute(5),
            result2 = compute(-3),
            
            // Map a successful result
            mapped1 = result1.map_ok((tuple: (I64, I64)) -> tuple._0 + tuple._1),
            
            // Map an error result
            mapped2 = result2.map_err((err: String) -> err ++ "!")
        in
            match mapped1
                | Left(sum) -> sum
                \ Right(_) -> -1
    "#;

        let result = run_type_inference(source, "nested_generics_tuples.opt");
        assert!(
            result.is_ok(),
            "Nested generics with tuples failed type inference: {:?}",
            result
        );
    }

    #[test]
    fn test_higher_kinded_generics() {
        let source = r#"
    // Generic container interface
    data Option<T> = 
        | Some(value: T)
        \ None
    
    // Functions with higher-kinded type parameters
    fn <T, U> (opt: Option<T>) map_option(f: T -> U): Option<U> = match opt
        | Some(value) -> Some(f(value))
        \ None -> None
    
    fn <T, U> (arr: [T]) map_array(f: T -> U): [U] = match arr
        | [] -> []
        \ [x .. xs] -> [f(x)] ++ xs.map_array(f)
    
    // Generic lift function that works with both Options and Arrays
    fn <T, U, F> lift(container: F, mapper: T -> F): F = match container
        | option: Option<T> -> option.map_option(mapper)
        \ array: [T] -> array.map_array(mapper)
    
    fn main(): I64 = 
        let 
            opt1 = Some(10),
            opt2: Option<I64> = None,
            arr = [1, 2, 3],
            
            // Test lifting with different container types
            result1 = lift(opt1, x: I64 -> x * 2),
            result2 = lift(opt2, x: I64 -> x * 3),
            result3 = lift(arr, x: I64 -> x * 4)
        in
            match result1
                | Some(value) -> value
                \ None -> match result3
                    | [first .. _] -> first
                    \ [] -> -1
    "#;

        let result = run_type_inference(source, "higher_kinded_generics.opt");
        assert!(
            result.is_ok(),
            "Higher-kinded generics failed type inference: {:?}",
            result
        );
    }

    #[test]
    fn test_generic_recursive_functions() {
        let source = r#"
    // Generic functional data structures
    data List<T> = 
        | Cons(head: T, tail: List<T>)
        \ Nil
    
    // Generic recursive functions
    fn <T> (list: List<T>) length(): I64 = match list
        | Nil -> 0
        \ Cons(_, tail) -> 1 + tail.length()
    
    fn <T, U> (list: List<T>) map(f: T -> U): List<U> = match list
        | Nil -> Nil
        \ Cons(head, tail) -> Cons(f(head), tail.map(f))
    
    fn <T> (list: List<T>) reverse(): List<T> = 
        let 
            helper = (remaining: List<T>, acc: List<T>) -> match remaining
                | Nil -> acc
                \ Cons(head, tail) -> helper(tail, Cons(head, acc))
        in
            helper(list, Nil)
    
    fn <T> (list1: List<T>, list2: List<T>) append(): List<T> = match list1
        | Nil -> list2
        \ Cons(head, tail) -> Cons(head, tail.append(list2))
    
    fn <T> (list: List<T>) take(n: I64): List<T> = 
        if n <= 0 then
            Nil
        else match list
            | Nil -> Nil
            \ Cons(head, tail) -> Cons(head, tail.take(n - 1))
    
    fn main(): I64 = 
        let 
            list1 = Cons(1, Cons(2, Cons(3, Nil))),
            list2 = Cons(4, Cons(5, Nil)),
            
            // Test various operations
            mapped = list1.map(x: I64 -> x * x),
            reversed = mapped.reverse(),
            combined = list1.append(list2),
            first_three = combined.take(3)
        in
            first_three.length() + reversed.length()
    "#;

        let result = run_type_inference(source, "generic_recursive_functions.opt");
        assert!(
            result.is_ok(),
            "Generic recursive functions failed type inference: {:?}",
            result
        );
    }

    #[test]
    fn test_generic_mutual_recursion() {
        let source = r#"
    // Generic mutual recursive functions and datatypes
    data Tree<T> = 
        | Node(value: T, children: Forest<T>)
        \ Leaf(value: T)
    
    data Forest<T> = 
        | ConsF(first: Tree<T>, rest: Forest<T>)
        \ NilF
    
    // Generic mutually recursive functions
    fn <T, U> (tree: Tree<T>) map_tree(f: T -> U): Tree<U> = match tree
        | Leaf(value) -> Leaf(f(value))
        \ Node(value, children) -> Node(f(value), children.map_forest(f))
    
    fn <T, U> (forest: Forest<T>) map_forest(f: T -> U): Forest<U> = match forest
        | NilF -> NilF
        \ ConsF(first, rest) -> ConsF(first.map_tree(f), rest.map_forest(f))
    
    fn <T> (tree: Tree<T>) count_nodes(): I64 = match tree
        | Leaf(_) -> 1
        \ Node(_, children) -> 1 + children.count_forest_nodes()
    
    fn <T> (forest: Forest<T>) count_forest_nodes(): I64 = match forest
        | NilF -> 0
        \ ConsF(first, rest) -> first.count_nodes() + rest.count_forest_nodes()
    
    fn main(): I64 = 
        let 
            // Create a complex tree structure
            leaf1 = Leaf(1),
            leaf2 = Leaf(2),
            leaf3 = Leaf(3),
            forest1 = ConsF(leaf1, ConsF(leaf2, NilF)),
            forest2 = ConsF(leaf3, NilF),
            node1 = Node(4, forest1),
            node2 = Node(5, forest2),
            forest3 = ConsF(node1, ConsF(node2, NilF)),
            root = Node(6, forest3),
            
            // Test operations
            mapped_tree = root.map_tree(x: I64 -> x * 10),
            node_count = mapped_tree.count_nodes()
        in
            node_count
    "#;

        let result = run_type_inference(source, "generic_mutual_recursion.opt");
        assert!(
            result.is_ok(),
            "Generic mutual recursion failed type inference: {:?}",
            result
        );
    }

    #[test]
    fn test_generic_function_with_recursive_closures() {
        let source = r#"
    // Generic functions with recursive closures
    fn <T, U> memoize(f: T -> U): T -> U =
        let 
            cache = []  // Simplified cache
        in
            x: T -> {
                // In a real implementation, we would check the cache
                // and only compute if needed
                f(x)
            }
    
    fn <T> (array: [T]) quicksort(cmp: (T, T) -> I64): [T] =
        match array
            | [] -> []
            | [pivot .. rest] -> {
                let 
                    partition = (arr: [T], pivot: T, cmp: (T, T) -> I64) -> {
                        let 
                            less = arr.filter(x: T -> cmp(x, pivot) < 0),
                            greater = arr.filter(x: T -> cmp(x, pivot) >= 0)
                        in
                            (less, greater)
                    },
                    
                    (less, greater) = partition(rest, pivot, cmp),
                    sorted_less = less.quicksort(cmp),
                    sorted_greater = greater.quicksort(cmp)
                in
                    sorted_less ++ [pivot] ++ sorted_greater
            }
    
    fn <T> (array: [T]) filter(predicate: T -> Bool): [T] = match array
        | [] -> []
        | [x .. xs] -> 
            if predicate(x) then 
                [x] ++ xs.filter(predicate)
            else 
                xs.filter(predicate)
    
    fn main(): I64 = 
        let 
            numbers = [5, 3, 8, 1, 2, 9, 4, 7, 6],
            compare = (a: I64, b: I64) -> a - b,
            
            // Create a memoized factorial function
            factorial_impl = (n: I64, self: I64 -> I64) -> 
                if n <= 1 then 1 else n * self(n - 1),
                
            factorial_fix = (f: (I64, I64 -> I64) -> I64) -> {
                let 
                    g = (n: I64) -> f(n, g)
                in
                    g
            },
            
            factorial = factorial_fix(factorial_impl),
            memoized_factorial = memoize(factorial),
            
            // Sort numbers and map with factorial
            sorted = numbers.quicksort(compare),
            filtered = sorted.filter(x: I64 -> x % 2 == 0),
            result = memoized_factorial(filtered.length())
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
    fn test_generic_adt_inheritance() {
        let source = r#"
    // Generic ADTs with inheritance
    data Container<T> = 
        | MutableContainer = 
            | Array(items: [T])
            \ Queue(items: [T])
        \ ImmutableContainer = 
            | List(items: [T])
            \ Set(items: [T])
    
    // Generic higher-order functions for containers
    fn <T, U> (container: Container<T>) transform(f: T -> U): Container<U> = match container
        | Array(items) -> Array(items.map(f))
        | Queue(items) -> Queue(items.map(f))
        | List(items) -> List(items.map(f))
        \ Set(items) -> Set(items.map(f))
    
    fn <T, U> (items: [T]) map(f: T -> U): [U] = match items
        | [] -> []
        \ [x .. xs] -> [f(x)] ++ xs.map(f)
    
    fn <T> (container: Container<T>) size(): I64 = match container
        | Array(items) -> items.length()
        | Queue(items) -> items.length()
        | List(items) -> items.length()
        \ Set(items) -> items.length()
    
    fn <T> (items: [T]) length(): I64 = match items
        | [] -> 0
        \ [_ .. xs] -> 1 + xs.length()
    
    fn main(): I64 = 
        let 
            numbers = [1, 2, 3, 4, 5],
            array_container = Array(numbers),
            list_container = List(numbers),
            
            // Transform containers
            doubled_array = array_container.transform(x: I64 -> x * 2),
            squared_list = list_container.transform(x: I64 -> x * x),
            
            // Get sizes
            array_size = doubled_array.size(),
            list_size = squared_list.size()
        in
            array_size + list_size
    "#;

        let result = run_type_inference(source, "generic_adt_inheritance.opt");
        assert!(
            result.is_ok(),
            "Generic ADT inheritance failed type inference: {:?}",
            result
        );
    }

    #[test]
    fn test_instantiating_same_generic_differently() {
        let source = r#"
    // Generic identity function
    fn <T> id(x: T): T = x
    
    // Generic pair type
    data Pair<A, B> = Tuple(first: A, second: B)
    
    // Function combining different instantiations of the same generic
    fn example(): (I64, String, Bool) = 
        let 
            // Same generic function with different type instantiations
            id_int = id(42),
            id_str = id("hello"),
            id_bool = id(true),
            
            // Same generic type with different type instantiations
            pair1 = Tuple(id_int, id_str),
            pair2 = Tuple(id_str, id_bool),
            
            // Mixing instantiations
            nested_pair = Tuple(pair1, pair2)
        in
            (nested_pair.first.first, nested_pair.second.first, nested_pair.second.second)
    
    fn main(): I64 = 
        let 
            (num, str, bool) = example()
        in
            if bool then num else 0
    "#;

        let result = run_type_inference(source, "different_instantiations.opt");
        assert!(
            result.is_ok(),
            "Instantiating same generic differently failed type inference: {:?}",
            result
        );
    }

    #[test]
    fn test_generic_type_constraints() {
        // This should fail since we don't properly support type constraints/bounds yet
        let source = r#"
    // Attempting to use type constraints (which might not be supported)
    
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
}
