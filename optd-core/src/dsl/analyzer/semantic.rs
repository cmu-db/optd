/*use std::collections::HashSet;

use crate::dsl::parser::ast::{Expr, File, Function, Operator, Pattern, Properties, Type};

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
            Type::Int64 | Type::String | Type::Bool | Type::Float64 => true,
            _ => false,
        }
    }

    fn is_valid_logical_type(&self, ty: &Type) -> bool {
        match ty {
            Type::Array(inner) => self.is_valid_logical_type(inner),
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

    fn validate_operator(&mut self, operator: &Operator) -> Result<(), String> {
        match operator {
            Operator::Scalar(scalar_op) => {
                if self.operators.contains(&scalar_op.name) {
                    return Err(format!("Duplicate operator name: {}", scalar_op.name));
                }
                self.operators.insert(scalar_op.name.clone());

                for field in &scalar_op.fields {
                    if !self.is_valid_scalar_type(&field.ty) {
                        return Err(format!("Invalid type in scalar operator: {:?}", field.ty));
                    }
                }
            }
            Operator::Logical(logical_op) => {
                if self.operators.contains(&logical_op.name) {
                    return Err(format!("Duplicate operator name: {}", logical_op.name));
                }
                self.operators.insert(logical_op.name.clone());

                for field in &logical_op.fields {
                    if !self.is_valid_logical_type(&field.ty) {
                        return Err(format!("Invalid type in logical operator: {:?}", field.ty));
                    }
                }

                // Check that derived properties match the logical properties fields
                for (prop_name, _) in &logical_op.derived_props {
                    if !self.operators.iter().any(|f| f == prop_name) {
                        return Err(format!(
                            "Derived property not found in logical properties: {}",
                            prop_name
                        ));
                    }
                }

                // Check that all logical properties fields have corresponding derived properties
                for field in &self.logical_properties {
                    if !logical_op.derived_props.contains_key(field) {
                        return Err(format!(
                            "Logical property field '{}' is missing a derived property",
                            field
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    // Validate a function definition
    fn validate_function(&mut self, function: &Function) -> Result<(), String> {
        if self.function_names.contains(&function.name) {
            return Err(format!("Duplicate function name: {}", function.name));
        }
        self.function_names.insert(function.name.clone());

        self.enter_scope();
        for (param_name, _) in &function.params {
            self.add_identifier(param_name.clone())?;
        }
        self.validate_expr(&function.body)?;
        self.exit_scope();

        Ok(())
    }

    // Validate an expression
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

    // Validate a pattern
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

    // Validate a complete file
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
*/
