use std::collections::HashSet;

use crate::irs::hir::{Expr, File, Function, Operator, OperatorKind, Pattern, Properties, Type};

/// SemanticAnalyzer performs static analysis on the DSL code to ensure semantic correctness.
/// It validates properties, operators, functions, and expressions while maintaining scope information.
///
/// # Fields
/// * `logical_properties` - Set of valid logical property names defined in the current context
/// * `operators` - Set of operator names to prevent duplicates
/// * `identifiers` - Stack of identifier sets representing different scopes
#[derive(Debug)]
pub struct SemanticAnalyzer {
    logical_properties: HashSet<String>,
    operators: HashSet<String>,
    identifiers: Vec<HashSet<String>>,
}

impl Default for SemanticAnalyzer {
    /// Default implementation for SemanticAnalyzer.
    fn default() -> Self {
        Self::new()
    }
}

impl SemanticAnalyzer {
    /// Creates a new SemanticAnalyzer instance with empty sets and a single global scope.
    pub fn new() -> Self {
        SemanticAnalyzer {
            logical_properties: HashSet::new(),
            operators: HashSet::new(),
            identifiers: vec![HashSet::new()], // Initialize with global scope
        }
    }

    /// Creates a new scope for local variables.
    /// Used when entering functions, closures, or blocks.
    fn enter_scope(&mut self) {
        self.identifiers.push(HashSet::new());
    }

    /// Removes the current scope when exiting a block.
    /// Any variables defined in this scope become inaccessible.
    fn exit_scope(&mut self) {
        self.identifiers.pop();
    }

    /// Adds a new identifier to the current scope.
    /// Returns an error if the identifier is already defined in the current scope.
    ///
    /// # Arguments
    /// * `name` - The identifier name to add
    ///
    /// # Returns
    /// * `Ok(())` if the identifier was added successfully
    /// * `Err(String)` if the identifier already exists in the current scope
    fn add_identifier(&mut self, name: String) -> Result<(), String> {
        if let Some(scope) = self.identifiers.last_mut() {
            if scope.contains(&name) {
                return Err(format!("Duplicate identifier name: {}", name));
            }
            scope.insert(name);
        }
        Ok(())
    }

    /// Checks if an identifier is defined in any accessible scope.
    /// Searches from innermost to outermost scope.
    ///
    /// # Arguments
    /// * `name` - The identifier name to look up
    ///
    /// # Returns
    /// * `true` if the identifier is found in any accessible scope
    /// * `false` otherwise
    fn lookup_identifier(&self, name: &str) -> bool {
        self.identifiers
            .iter()
            .rev()
            .any(|scope| scope.contains(name))
    }

    /// Validates that a type is valid for scalar operators.
    /// Scalar types include basic types and arrays/tuples of scalar types.
    ///
    /// # Arguments
    /// * `ty` - The type to validate
    ///
    /// # Returns
    /// * `true` if the type is valid for scalar operators
    /// * `false` otherwise
    fn is_valid_scalar_type(ty: &Type) -> bool {
        match ty {
            Type::Array(inner) => Self::is_valid_scalar_type(inner),
            Type::Tuple(fields) => fields.iter().all(Self::is_valid_scalar_type),
            Type::Int64 | Type::String | Type::Bool | Type::Float64 => true,
            Type::Operator(OperatorKind::Scalar) => true,
            _ => false,
        }
    }

    /// Validates that a type is valid for logical operators.
    /// Logical types include basic types, operators, and arrays/tuples of logical types.
    ///
    /// # Arguments
    /// * `ty` - The type to validate
    ///
    /// # Returns
    /// * `true` if the type is valid for logical operators
    /// * `false` otherwise
    fn is_valid_logical_type(ty: &Type) -> bool {
        match ty {
            Type::Array(inner) => Self::is_valid_logical_type(inner),
            Type::Tuple(fields) => fields.iter().all(Self::is_valid_logical_type),
            Type::Int64 | Type::String | Type::Bool | Type::Float64 => true,
            Type::Operator(_) => true,
            _ => false,
        }
    }

    /// Validates that a type is valid for properties.
    /// Property types include basic types, maps, and arrays/tuples of property types.
    ///
    /// # Arguments
    /// * `ty` - The type to validate
    ///
    /// # Returns
    /// * `true` if the type is valid for properties
    /// * `false` otherwise
    fn is_valid_property_type(ty: &Type) -> bool {
        match ty {
            Type::Array(inner) => Self::is_valid_property_type(inner),
            Type::Tuple(fields) => fields.iter().all(Self::is_valid_property_type),
            Type::Map(a, b) => Self::is_valid_property_type(a) && Self::is_valid_property_type(b),
            Type::Int64 | Type::String | Type::Bool | Type::Float64 => true,
            Type::Function(_, _) | Type::Operator(_) => false,
        }
    }

    /// Validates property definitions and updates the logical_properties set.
    ///
    /// # Arguments
    /// * `properties` - The properties to validate
    ///
    /// # Returns
    /// * `Ok(())` if all properties are valid
    /// * `Err(String)` if any property has an invalid type
    fn validate_properties(&mut self, properties: &Properties) -> Result<(), String> {
        // Validate all property types
        for field in &properties.fields {
            if !Self::is_valid_property_type(&field.ty) {
                return Err(format!("Invalid type in properties: {:?}", field.ty));
            }
        }

        // Update logical properties set
        self.logical_properties = properties
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect();

        Ok(())
    }

    /// Validates operator definitions, including field types and derived properties.
    ///
    /// # Arguments
    /// * `operator` - The operator to validate
    ///
    /// # Returns
    /// * `Ok(())` if the operator is valid
    /// * `Err(String)` containing the validation error
    fn validate_operator(&mut self, operator: &Operator) -> Result<(), String> {
        let (name, fields, is_logical) = match operator {
            Operator::Scalar(op) => (&op.name, &op.fields, false),
            Operator::Logical(op) => (&op.name, &op.fields, true),
        };

        // Check for duplicate operator names
        if self.operators.contains(name) {
            return Err(format!("Duplicate operator name: {}", name));
        }
        self.operators.insert(name.clone());

        // Validate field types based on operator kind
        if let Some(field) = fields.iter().find(|f| {
            if is_logical {
                !Self::is_valid_logical_type(&f.ty)
            } else {
                !Self::is_valid_scalar_type(&f.ty)
            }
        }) {
            return Err(format!(
                "Invalid type in {} operator: {:?}",
                if is_logical { "logical" } else { "scalar" },
                field.ty
            ));
        }

        // Additional validation for logical operators
        if let Operator::Logical(op) = operator {
            // Validate derived properties exist in logical properties
            if let Some(prop) = op
                .derived_props
                .keys()
                .find(|&p| !self.logical_properties.contains(p))
            {
                return Err(format!(
                    "Derived property not found in logical properties: {}",
                    prop
                ));
            }

            // Ensure all logical properties have derived implementations
            if let Some(field) = self
                .logical_properties
                .iter()
                .find(|&f| !op.derived_props.contains_key(f))
            {
                return Err(format!(
                    "Logical property field '{}' is missing a derived property",
                    field
                ));
            }
        }

        Ok(())
    }

    /// Validates function definitions, including parameters and body.
    ///
    /// # Arguments
    /// * `function` - The function to validate
    ///
    /// # Returns
    /// * `Ok(())` if the function is valid
    /// * `Err(String)` containing the validation error
    fn validate_function(&mut self, function: &Function) -> Result<(), String> {
        // Add function name to current scope
        self.add_identifier(function.name.clone())?;

        // Create new scope for function parameters and body
        self.enter_scope();

        // Add parameters to function scope
        for (param_name, _) in &function.params {
            self.add_identifier(param_name.clone())?;
        }

        // Validate function body
        self.validate_expr(&function.body)?;

        // Exit function scope
        self.exit_scope();

        Ok(())
    }

    /// Validates expressions recursively, ensuring all variables are defined
    /// and sub-expressions are valid.
    ///
    /// # Arguments
    /// * `expr` - The expression to validate
    ///
    /// # Returns
    /// * `Ok(())` if the expression is valid
    /// * `Err(String)` containing the validation error
    fn validate_expr(&mut self, expr: &Expr) -> Result<(), String> {
        match expr {
            Expr::Var(name) => {
                if !self.lookup_identifier(name) {
                    return Err(format!("Undefined identifier: {}", name));
                }
            }
            Expr::Val(name, expr1, expr2) => {
                self.validate_expr(expr1)?;
                self.add_identifier(name.clone())?;
                self.validate_expr(expr2)?;
            }
            Expr::Match(expr, arms) => {
                self.validate_expr(expr)?;
                for arm in arms {
                    self.validate_pattern(&arm.pattern)?;
                    self.validate_expr(&arm.expr)?;
                }
            }
            Expr::If(cond, then_expr, else_expr) => {
                self.validate_expr(cond)?;
                self.validate_expr(then_expr)?;
                self.validate_expr(else_expr)?;
            }
            Expr::Binary(left, _, right) => {
                self.validate_expr(left)?;
                self.validate_expr(right)?;
            }
            Expr::Unary(_, expr) => {
                self.validate_expr(expr)?;
            }
            Expr::Call(func, args) => {
                self.validate_expr(func)?;
                for arg in args {
                    self.validate_expr(arg)?;
                }
            }
            Expr::Member(expr, _) => {
                self.validate_expr(expr)?;
            }
            Expr::MemberCall(expr, _, args) => {
                self.validate_expr(expr)?;
                for arg in args {
                    self.validate_expr(arg)?;
                }
            }
            Expr::ArrayIndex(array, index) => {
                self.validate_expr(array)?;
                self.validate_expr(index)?;
            }
            Expr::Literal(_) => {}
            Expr::Fail(_) => {}
            Expr::Closure(params, body) => {
                self.enter_scope();
                for param in params {
                    self.add_identifier(param.clone())?;
                }
                self.validate_expr(body)?;
                self.exit_scope();
            }
            _ => {}
        }
        Ok(())
    }

    /// Validates pattern matching constructs, ensuring all bindings are valid.
    ///
    /// # Arguments
    /// * `pattern` - The pattern to validate
    ///
    /// # Returns
    /// * `Ok(())` if the pattern is valid
    /// * `Err(String)` containing the validation error
    fn validate_pattern(&mut self, pattern: &Pattern) -> Result<(), String> {
        match pattern {
            Pattern::Bind(name, pat) => {
                self.add_identifier(name.clone())?;
                self.validate_pattern(pat)?;
            }
            Pattern::Constructor(_, pats) => {
                for pat in pats {
                    self.validate_pattern(pat)?;
                }
            }
            Pattern::Literal(_) | Pattern::Wildcard => {}
            Pattern::Var(name) => {
                self.add_identifier(name.clone())?;
            }
        }
        Ok(())
    }

    /// Main entry point for validating a complete DSL file.
    /// Validates properties, operators, and functions in order.
    ///
    /// # Arguments
    /// * `file` - The File AST node to validate
    ///
    /// # Returns
    /// * `Ok(())` if the file is semantically valid
    /// * `Err(String)` containing the first validation error encountered
    pub fn validate_file(&mut self, file: &File) -> Result<(), String> {
        self.validate_properties(&file.properties)?;

        for operator in &file.operators {
            self.validate_operator(operator)?;
        }

        for function in &file.functions {
            self.validate_function(function)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        irs::hir::{
            Expr, Field, File, Function, Literal, LogicalOp, Operator, Properties, ScalarOp, Type,
        },
        parser::parse_file,
    };
    use std::collections::HashMap;

    fn create_test_file() -> File {
        File {
            properties: Properties {
                fields: vec![
                    Field {
                        name: "prop1".to_string(),
                        ty: Type::Int64,
                    },
                    Field {
                        name: "prop2".to_string(),
                        ty: Type::String,
                    },
                ],
            },
            operators: vec![
                Operator::Scalar(ScalarOp {
                    name: "op1".to_string(),
                    fields: vec![Field {
                        name: "field1".to_string(),
                        ty: Type::Int64,
                    }],
                }),
                Operator::Logical(LogicalOp {
                    name: "op2".to_string(),
                    fields: vec![Field {
                        name: "field2".to_string(),
                        ty: Type::String,
                    }],
                    derived_props: HashMap::from([
                        ("prop1".to_string(), Expr::Literal(Literal::Int64(42))),
                        (
                            "prop2".to_string(),
                            Expr::Literal(Literal::String("test".to_string())),
                        ),
                    ]),
                }),
            ],
            functions: vec![Function {
                name: "func1".to_string(),
                params: vec![("x".to_string(), Type::Int64)],
                return_type: Type::Int64,
                body: Expr::Literal(Literal::Int64(42)),
                rule_type: None,
            }],
        }
    }

    #[test]
    fn test_working_file() {
        let input = include_str!("../programs/working.optd");
        let out = parse_file(input).unwrap();
        let mut analyzer = SemanticAnalyzer::new();
        analyzer.validate_file(&out).unwrap();
    }

    #[test]
    fn test_valid_file() {
        let file = create_test_file();
        let mut analyzer = SemanticAnalyzer::new();
        assert!(analyzer.validate_file(&file).is_ok());
    }

    #[test]
    fn test_duplicate_operator_name() {
        let mut file = create_test_file();
        file.operators.push(Operator::Scalar(ScalarOp {
            name: "op1".to_string(), // Duplicate name
            fields: vec![Field {
                name: "field3".to_string(),
                ty: Type::Int64,
            }],
        }));

        let mut analyzer = SemanticAnalyzer::new();
        let result = analyzer.validate_file(&file);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Duplicate operator name: op1");
    }

    #[test]
    fn test_duplicate_function_name() {
        let mut file = create_test_file();
        file.functions.push(Function {
            name: "func1".to_string(), // Duplicate name
            params: vec![("y".to_string(), Type::Int64)],
            return_type: Type::Int64,
            body: Expr::Literal(Literal::Int64(42)),
            rule_type: None,
        });

        let mut analyzer = SemanticAnalyzer::new();
        let result = analyzer.validate_file(&file);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Duplicate identifier name: func1");
    }

    #[test]
    fn test_undefined_variable() {
        let mut file = create_test_file();
        file.functions[0].body = Expr::Var("undefined_var".to_string()); // Undefined variable

        let mut analyzer = SemanticAnalyzer::new();
        let result = analyzer.validate_file(&file);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Undefined identifier: undefined_var");
    }

    #[test]
    fn test_invalid_scalar_operator_type() {
        let mut file = create_test_file();
        file.operators[0] = Operator::Scalar(ScalarOp {
            name: "op1".to_string(),
            fields: vec![Field {
                name: "field1".to_string(),
                ty: Type::Function(Box::new(Type::Int64), Box::new(Type::Int64)),
            }],
        });

        let mut analyzer = SemanticAnalyzer::new();
        let result = analyzer.validate_file(&file);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Invalid type in scalar operator: Function(Int64, Int64)"
        );
    }

    #[test]
    fn test_invalid_logical_operator_type() {
        let mut file = create_test_file();
        file.operators[1] = Operator::Logical(LogicalOp {
            name: "op2".to_string(),
            fields: vec![Field {
                name: "field2".to_string(),
                ty: Type::Function(Box::new(Type::Int64), Box::new(Type::Int64)),
            }],
            derived_props: HashMap::new(),
        });

        let mut analyzer = SemanticAnalyzer::new();
        let result = analyzer.validate_file(&file);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Invalid type in logical operator: Function(Int64, Int64)"
        );
    }

    #[test]
    fn test_missing_derived_property() {
        let mut file = create_test_file();
        if let Operator::Logical(op) = &mut file.operators[1] {
            op.derived_props.remove("prop2"); // Missing derived property
        }

        let mut analyzer = SemanticAnalyzer::new();
        let result = analyzer.validate_file(&file);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Logical property field 'prop2' is missing a derived property"
        );
    }

    #[test]
    fn test_invalid_property_type() {
        let mut file = create_test_file();
        file.properties.fields[0].ty = Type::Function(Box::new(Type::Int64), Box::new(Type::Int64));

        let mut analyzer = SemanticAnalyzer::new();
        let result = analyzer.validate_file(&file);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Invalid type in properties: Function(Int64, Int64)"
        );
    }
}
