use super::expr::convert_expr;
use super::types::{convert_type, create_function_type};
use crate::analyzer::hir::{Annotation, FunKind, Identifier, UdfKind};
use crate::analyzer::semantic_check::error::SemanticErrorKind;
use crate::analyzer::{
    context::Context,
    hir::{CoreData, HIR, TypedSpan, Value},
    types::{Type, TypeRegistry},
};
use crate::parser::ast::{Function, Item, Module};
use crate::utils::span::Spanned;
use FunKind::*;
use UdfKind::*;
use std::collections::{HashMap, HashSet};

/// Converts an AST to a High-level Intermediate Representation (HIR).
#[derive(Debug, Clone, Default)]
pub struct ASTConverter {
    /// Registry of types for type checking and subtyping.
    pub(super) type_registry: TypeRegistry,
    /// Context for variable bindings.
    pub(super) context: Context<TypedSpan>,
    /// Annotations for HIR expressions.
    pub(super) annotations: HashMap<Identifier, Vec<Annotation>>,
}

impl ASTConverter {
    /// Converts an AST module to a partially typed and spanned HIR.
    ///
    /// This method processes all items in the AST module and transforms them into the HIR representation.
    /// For functions, it handles receivers, parameters, function bodies, and return types.
    ///
    /// # Arguments
    ///
    /// * `module` - The AST module to convert
    ///
    /// # Returns
    ///
    /// The HIR representation and TypeRegistry, or a SemanticErrorKind if conversion fails.
    pub fn convert(
        mut self,
        module: &Module,
    ) -> Result<(HIR<TypedSpan>, TypeRegistry), SemanticErrorKind> {
        // Process all module items sequentially.
        for item in &module.items {
            match item {
                Item::Adt(spanned_adt) => {
                    self.type_registry.register_adt(&spanned_adt.value)?;
                }
                Item::Function(spanned_fn) => {
                    self.process_function(spanned_fn)?;
                }
            }
        }

        // Push the context scope of the module, as we have processed all functionss.
        self.context.push_scope();

        Ok((
            HIR {
                context: self.context,
                annotations: self.annotations,
            },
            self.type_registry,
        ))
    }

    /// Processes a function AST node and adds it to the context.
    ///
    /// Handles function parameters, return type, and body conversion.
    pub(super) fn process_function(
        &mut self,
        spanned_fn: &Spanned<Function>,
    ) -> Result<(), SemanticErrorKind> {
        let func = &spanned_fn.value;
        let name = &*func.name.value;
        let fn_span = func.name.span.clone();

        // Reject functions without parameters.
        if func.receiver.is_none() && func.params.is_none() {
            return Err(SemanticErrorKind::new_incomplete_function(
                name.clone(),
                fn_span,
            ));
        }

        // Register the function in the context, while checking for duplicates.
        let generics = func
            .type_params
            .iter()
            .map(|param| (*param.value).clone())
            .collect();

        let params = Self::get_parameters(func, &generics);
        let return_type = convert_type(&func.return_type.value, &generics);
        let fn_type = create_function_type(&params, &return_type);

        match &func.body {
            Some(body_expr) => {
                // Process function with body.
                let body_hir = convert_expr(body_expr, &generics)?;
                let param_names = params.iter().map(|(name, _)| name.clone()).collect();

                let fn_value = Value::new_with(
                    CoreData::Function(Closure(param_names, body_hir.into())),
                    fn_type,
                    fn_span,
                );

                self.context.try_bind(name.clone(), fn_value)?;
            }
            None => {
                // Process external function (UDF).
                let fn_value = Value::new_with(
                    CoreData::Function(Udf(Unlinked(name.clone()))),
                    fn_type,
                    fn_span,
                );

                self.context.try_bind(name.clone(), fn_value)?;
            }
        };

        // Register function annotations if present.
        if !func.annotations.is_empty() {
            let annotations = func
                .annotations
                .iter()
                .map(|annotation| (*annotation.value).clone())
                .collect();

            self.annotations.insert(name.clone(), annotations);
        }

        Ok(())
    }

    /// Extracts parameter information from a function AST.
    ///
    /// Collects parameter names and types from both the receiver (if present)
    /// and the parameter list.
    fn get_parameters(func: &Function, generics: &HashSet<Identifier>) -> Vec<(Identifier, Type)> {
        // Start with receiver if it exists.
        let mut param_fields = match &func.receiver {
            Some(receiver) => {
                vec![(
                    (*receiver.name).clone(),
                    convert_type(&receiver.ty.value, generics),
                )]
            }
            None => vec![],
        };

        // Add regular parameters if they exist.
        if let Some(params) = &func.params {
            let regular_params = params
                .iter()
                .map(|field| {
                    (
                        (*field.name).clone(),
                        convert_type(&field.ty.value, generics),
                    )
                })
                .collect::<Vec<_>>();
            param_fields.extend(regular_params);
        }

        param_fields
    }
}

#[cfg(test)]
mod converter_tests {
    use super::*;
    use crate::analyzer::hir::{CoreData, FunKind};
    use crate::analyzer::types::Type;
    use crate::parser::ast::{self, Function, Item, Module};
    use crate::utils::span::{Span, Spanned};
    use std::collections::HashSet;

    // Helper functions to create test items
    fn create_test_span() -> Span {
        Span::new("test".to_string(), 0..1)
    }

    fn spanned<T>(value: T) -> Spanned<T> {
        Spanned::new(value, create_test_span())
    }

    fn create_simple_function(name: &str, has_body: bool) -> Spanned<Function> {
        // Create a simple function with Int64 parameter and return type
        let field = spanned(ast::Field {
            name: spanned(String::from("param")),
            ty: spanned(ast::Type::Int64),
        });

        let body = if has_body {
            Some(spanned(ast::Expr::Literal(ast::Literal::Int64(42))))
        } else {
            None
        };

        spanned(Function {
            name: spanned(String::from(name)),
            type_params: Vec::new(),
            receiver: None,
            params: Some(vec![field]),
            return_type: spanned(ast::Type::Int64),
            body,
            annotations: Vec::new(),
        })
    }

    fn create_annotated_function(name: &str, annotations: Vec<&str>) -> Spanned<Function> {
        // Create a function with annotations
        let func = create_simple_function(name, true);
        let mut func_val = (*func.value).clone();

        func_val.annotations = annotations
            .into_iter()
            .map(|a| spanned(String::from(a)))
            .collect();

        spanned(func_val)
    }

    fn create_function_without_params(name: &str) -> Spanned<Function> {
        // Create a function without parameters (should cause an error)
        spanned(Function {
            name: spanned(String::from(name)),
            type_params: Vec::new(),
            receiver: None,
            params: None,
            return_type: spanned(ast::Type::Int64),
            body: Some(spanned(ast::Expr::Literal(ast::Literal::Int64(42)))),
            annotations: Vec::new(),
        })
    }

    fn create_method_with_receiver(name: &str) -> Spanned<Function> {
        // Create a method with a receiver parameter
        let receiver = spanned(ast::Field {
            name: spanned(String::from("self")),
            ty: spanned(ast::Type::Identifier(String::from("MyType"))),
        });

        spanned(Function {
            name: spanned(String::from(name)),
            type_params: Vec::new(),
            receiver: Some(receiver),
            params: None,
            return_type: spanned(ast::Type::Int64),
            body: Some(spanned(ast::Expr::Literal(ast::Literal::Int64(42)))),
            annotations: Vec::new(),
        })
    }

    fn create_module_with_functions(functions: Vec<Spanned<Function>>) -> Module {
        let items = functions.into_iter().map(Item::Function).collect();

        Module { items }
    }

    #[test]
    fn test_convert_simple_module() {
        // Create a simple module with one function
        let func = create_simple_function("test_function", true);
        let module = create_module_with_functions(vec![func]);

        // Create and run the converter
        let converter = ASTConverter::default();
        let result = converter.convert(&module);

        // Verify result is Ok and contains the expected function
        assert!(result.is_ok());
        let (hir, _) = result.unwrap();

        // Check that function is in the context
        assert!(hir.context.lookup("test_function").is_some());
    }

    #[test]
    fn test_convert_function_with_annotations() {
        // Create a function with annotations
        let func = create_annotated_function("annotated_function", vec!["test", "important"]);
        let module = create_module_with_functions(vec![func]);

        // Create and run the converter
        let converter = ASTConverter::default();
        let result = converter.convert(&module);

        // Verify result is Ok and contains the expected annotations
        assert!(result.is_ok());
        let (hir, _) = result.unwrap();

        // Check that annotations are stored
        let annotations = hir.annotations.get("annotated_function");
        assert!(annotations.is_some());
        let annotations = annotations.unwrap();
        assert_eq!(annotations.len(), 2);
        assert!(annotations.contains(&String::from("test")));
        assert!(annotations.contains(&String::from("important")));
    }

    #[test]
    fn test_reject_function_without_parameters() {
        // Create a function without parameters (should be rejected)
        let func = create_function_without_params("invalid_function");
        let module = create_module_with_functions(vec![func]);

        // Create and run the converter
        let converter = ASTConverter::default();
        let result = converter.convert(&module);

        // Verify result is an Error
        assert!(result.is_err());
        match result {
            Err(err) => {
                // Check that it's the expected error type (incomplete function)
                match err {
                    SemanticErrorKind::IncompleteFunction { .. } => (),
                    _ => panic!("Expected IncompleteFunction error, got: {:?}", err),
                }
            }
            _ => panic!("Expected error"),
        }
    }

    #[test]
    fn test_process_method_with_receiver() {
        // Create a method with a receiver
        let method = create_method_with_receiver("test_method");
        let module = create_module_with_functions(vec![method]);

        // Create and run the converter
        let converter = ASTConverter::default();
        let result = converter.convert(&module);

        // Verify result is Ok and contains the expected method
        assert!(result.is_ok());
        let (hir, _) = result.unwrap();

        // Check that method is in the context
        let func_val = hir.context.lookup("test_method");
        assert!(func_val.is_some());

        // Verify the first parameter is the receiver
        if let CoreData::Function(FunKind::Closure(params, _)) = &func_val.unwrap().data {
            assert_eq!(params[0], "self");
        } else {
            panic!("Expected function with receiver");
        }
    }

    #[test]
    fn test_process_external_function() {
        // Create an external function (no body)
        let ext_func = create_simple_function("external_function", false);
        let module = create_module_with_functions(vec![ext_func]);

        // Create and run the converter
        let converter = ASTConverter::default();
        let result = converter.convert(&module);

        // Verify result is Ok and contains the expected function
        assert!(result.is_ok());
        let (hir, _registry) = result.unwrap();

        // Check that function is in the context
        let func_val = hir.context.lookup("external_function");
        assert!(func_val.is_some());

        // Verify it's an unlinked UDF
        if let CoreData::Function(FunKind::Udf(UdfKind::Unlinked(name))) = &func_val.unwrap().data {
            assert_eq!(name, "external_function");
        } else {
            panic!("Expected unlinked UDF");
        }
    }

    #[test]
    fn test_get_parameters() {
        // Test with just regular parameters
        let func_with_params = create_simple_function("func_params", true);
        let params = ASTConverter::get_parameters(&func_with_params.value, &HashSet::new());
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].0, "param");
        assert_eq!(params[0].1, Type::Int64);

        // Test with receiver and no regular parameters
        let func_with_receiver = create_method_with_receiver("func_receiver");
        let params = ASTConverter::get_parameters(&func_with_receiver.value, &HashSet::new());
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].0, "self");

        // Create a function with both receiver and regular parameters
        let mut func = (*func_with_receiver.value).clone();
        let field = spanned(ast::Field {
            name: spanned(String::from("extra_param")),
            ty: spanned(ast::Type::Bool),
        });
        func.params = Some(vec![field]);
        let func = spanned(func);

        let params = ASTConverter::get_parameters(&func.value, &HashSet::new());
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].0, "self");
        assert_eq!(params[1].0, "extra_param");
        assert_eq!(params[1].1, Type::Bool);
    }

    #[test]
    fn test_duplicate_function_name() {
        // Create two functions with the same name
        let func1 = create_simple_function("duplicate", true);
        let func2 = create_simple_function("duplicate", true);
        let module = create_module_with_functions(vec![func1, func2]);

        // Create and run the converter
        let converter = ASTConverter::default();
        let result = converter.convert(&module);

        // Verify result is an Error (duplicate name)
        assert!(result.is_err());
    }

    #[test]
    fn test_function_with_generics() {
        // Create a function with generic type parameters
        let func = create_simple_function("generic_function", true);
        let mut func_val = (*func.value).clone();

        // Add type parameters
        func_val.type_params = vec![spanned(String::from("T")), spanned(String::from("U"))];

        // Modify the return type to use a generic
        func_val.return_type = spanned(ast::Type::Identifier(String::from("T")));

        let func = spanned(func_val);
        let module = create_module_with_functions(vec![func]);

        // Create and run the converter
        let converter = ASTConverter::default();
        let result = converter.convert(&module);

        // Verify result is Ok
        assert!(result.is_ok());
        let (hir, _registry) = result.unwrap();

        // Check that function is in the context
        let func_val = hir.context.lookup("generic_function");
        assert!(func_val.is_some());

        // Verify the return type is a generic
        match &func_val.unwrap().metadata.ty {
            Type::Closure(_, ret_type) => {
                assert_eq!(**ret_type, Type::Generic(String::from("T")));
            }
            _ => panic!("Expected closure type"),
        }
    }
}
