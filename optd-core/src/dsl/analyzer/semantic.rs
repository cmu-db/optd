use std::collections::HashSet;

use crate::dsl::ast::upper_layer::{Expr, File, Function, Operator, Pattern, Properties, Type};

#[derive(Debug)]
pub struct SemanticAnalyzer {
    logical_properties: HashSet<String>,
    operators: HashSet<String>,
    identifiers: Vec<HashSet<String>>,
}

impl SemanticAnalyzer {
    pub fn new() -> Self {
        SemanticAnalyzer {
            logical_properties: HashSet::new(),
            operators: HashSet::new(),
            identifiers: Vec::new(),
        }
    }

    fn enter_scope(&mut self) {
        self.identifiers.push(HashSet::new());
    }

    fn exit_scope(&mut self) {
        self.identifiers.pop();
    }

    fn add_identifier(&mut self, name: String) -> Result<(), String> {
        if let Some(scope) = self.identifiers.last_mut() {
            if scope.contains(&name) {
                return Err(format!("Duplicate identifier name: {}", name));
            }
            scope.insert(name);
        }
        Ok(())
    }

    fn lookup_identifier(&self, name: &str) -> bool {
        self.identifiers
            .iter()
            .rev()
            .any(|scope| scope.contains(name))
    }

    fn is_valid_scalar_type(&self, ty: &Type) -> bool {
        match ty {
            Type::Array(inner) => self.is_valid_scalar_type(inner),
            Type::Tuple(fields) => fields.iter().all(|f| self.is_valid_scalar_type(f)),
            Type::Int64 | Type::String | Type::Bool | Type::Float64 => true,
            _ => false,
        }
    }

    fn is_valid_logical_type(&self, ty: &Type) -> bool {
        match ty {
            Type::Array(inner) => self.is_valid_logical_type(inner),
            Type::Tuple(fields) => fields.iter().all(|f| self.is_valid_logical_type(f)),
            Type::Int64 | Type::String | Type::Bool | Type::Float64 => true,
            _ => false,
        }
    }

    fn is_valid_property_type(&self, ty: &Type) -> bool {
        match ty {
            Type::Array(inner) => self.is_valid_property_type(inner),
            Type::Tuple(fields) => fields.iter().all(|f| self.is_valid_property_type(f)),
            Type::Map(a, b) => self.is_valid_property_type(a) && self.is_valid_property_type(b),
            Type::Int64 | Type::String | Type::Bool | Type::Float64 => true,
            Type::Function(_, _) => false,
            Type::Operator(_) => false,
        }
    }

    fn validate_properties(&mut self, properties: &Properties) -> Result<(), String> {
        for field in &properties.fields {
            if !self.is_valid_property_type(&field.ty) {
                return Err(format!("Invalid type in properties: {:?}", field.ty));
            }
        }

        self.logical_properties = properties
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect();

        Ok(())
    }

    fn validate_operator(&self, operator: &Operator) -> Result<(), String> {
        let (name, fields, is_logical) = match operator {
            Operator::Scalar(op) => (&op.name, &op.fields, false),
            Operator::Logical(op) => (&op.name, &op.fields, true),
        };

        if self.operators.contains(name) {
            return Err(format!("Duplicate operator name: {}", name));
        }

        if let Some(field) = fields.iter().find(|f| {
            if is_logical {
                !self.is_valid_logical_type(&f.ty)
            } else {
                !self.is_valid_scalar_type(&f.ty)
            }
        }) {
            return Err(format!(
                "Invalid type in {} operator: {:?}",
                if is_logical { "logical" } else { "scalar" },
                field.ty
            ));
        }

        if let Operator::Logical(op) = operator {
            if let Some(prop) = op
                .derived_props
                .keys()
                .find(|&p| !self.operators.contains(p))
            {
                return Err(format!(
                    "Derived property not found in logical properties: {}",
                    prop
                ));
            }

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

    fn validate_function(&mut self, function: &Function) -> Result<(), String> {
        self.add_identifier(function.name.clone())?;

        self.enter_scope();
        for (param_name, _) in &function.params {
            self.add_identifier(param_name.clone())?;
        }
        self.validate_expr(&function.body)?;
        self.exit_scope();

        Ok(())
    }

    fn validate_expr(&mut self, expr: &Expr) -> Result<(), String> {
        match expr {
            Expr::Var(name) => {
                if self.lookup_identifier(name) {
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
            Pattern::Literal(_) => {}
            Pattern::Wildcard => {}
            Pattern::Var(name) => {
                self.add_identifier(name.clone())?;
            }
        }
        Ok(())
    }

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
    use crate::dsl::ast::upper_layer::{
        Expr, Field, File, Function, Literal, LogicalOp, Operator, Properties, ScalarOp, Type,
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
    fn test_valid_file() {
        let file = create_test_file();
        let mut analyzer = SemanticAnalyzer::new();
        analyzer.validate_file(&file).unwrap();
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
            "Invalid type in scalar operator: Function(Int64)"
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
            "Invalid type in logical operator: Function(Int64)"
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
            "Invalid type in properties: Function(Int64)"
        );
    }
}
