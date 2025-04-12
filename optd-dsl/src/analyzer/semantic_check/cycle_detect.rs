use crate::{
    analyzer::{error::AnalyzerErrorKind, hir::Identifier, types::TypeRegistry},
    parser::ast::Type as AstType,
    utils::span::Spanned,
};
use std::collections::HashMap;

/// Exploration status for ADT cycle detection.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ExplorationStatus {
    /// Currently exploring this node.
    Exploring,
    /// Finished exploring, this type terminates.
    Terminated,
}

/// Contains the state needed for ADT cycle detection.
pub(super) struct CycleDetector<'a> {
    registry: &'a TypeRegistry,
    explore_status: HashMap<Identifier, ExplorationStatus>,
    dependencies: HashMap<Identifier, Vec<Identifier>>,
    pub(super) path: Vec<Spanned<Identifier>>,
}

impl<'a> CycleDetector<'a> {
    pub(super) fn new(registry: &'a TypeRegistry) -> Self {
        Self {
            registry,
            explore_status: HashMap::new(),
            dependencies: HashMap::default(),
            path: Vec::new(),
        }
    }

    /// Checks if a type can terminate (has no infinite recursion).
    ///
    /// Returns:
    /// - Ok(true) if the type terminates.
    /// - Ok(false) if the type has a cycle.
    /// - Err(AnalyzerErrorKind) for other errors like undefined types.
    pub(super) fn can_terminate(
        &mut self,
        adt: &Identifier,
    ) -> Result<bool, Box<AnalyzerErrorKind>> {
        use ExplorationStatus::*;

        if let Some(status) = self.explore_status.get(adt).cloned() {
            match status {
                Terminated => return Ok(true),
                Exploring => return Ok(false),
            }
        }

        self.explore_status.insert(adt.clone(), Exploring);

        // Start exploration in the path.
        self.path.push(Spanned::new(
            adt.clone(),
            self.registry.spans.get(adt).cloned().unwrap(),
        ));

        let terminates = if self.registry.product_fields.contains_key(adt) {
            // Product type: all fields must terminate.
            self.check_product_type_terminates(adt)?
        } else {
            // Sum type: one variant must terminate.
            self.check_sum_type_terminates(adt)?
        };

        if terminates {
            self.explore_status.insert(adt.to_string(), Terminated);
            // Restart the exploration of the previously blocked ADTs, populate
            // the exploration status map.
            if let Some(deps) = self.dependencies.remove(adt) {
                for dep in deps {
                    self.explore_status.remove(&dep);
                    let _ = self.can_terminate(&dep);
                }
            }
        }

        Ok(terminates)
    }

    /// Checks if a product type terminates - all fields must terminate.
    fn check_product_type_terminates(
        &mut self,
        product_type: &Identifier,
    ) -> Result<bool, Box<AnalyzerErrorKind>> {
        let fields = self.registry.product_fields.get(product_type).unwrap();

        for field in fields {
            if !self.check_field_type_terminates(&field.ty, product_type)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Checks if a sum type terminates - at least one variant must terminate.
    fn check_sum_type_terminates(
        &mut self,
        sum_type: &Identifier,
    ) -> Result<bool, Box<AnalyzerErrorKind>> {
        let variants = self.registry.subtypes.get(sum_type).unwrap();

        for variant in variants {
            if self.can_terminate(variant)? {
                return Ok(true);
            }

            self.dependencies
                .entry(variant.clone())
                .or_default()
                .push(sum_type.clone());
        }

        Ok(false)
    }

    /// Checks if a type (including complex types like Array, Map, etc.) terminates.
    fn check_field_type_terminates(
        &mut self,
        ty: &Spanned<AstType>,
        product_type: &Identifier,
    ) -> Result<bool, Box<AnalyzerErrorKind>> {
        use AstType::*;

        match &*ty.value {
            Identifier(name) => {
                // Check if the type exists.
                if !self.registry.subtypes.contains_key(name)
                    && !self.registry.product_fields.contains_key(name)
                {
                    return Err(AnalyzerErrorKind::new_undefined_type(name, &ty.span));
                }

                // Access field in path.
                self.path.push(Spanned::new(name.clone(), ty.span.clone()));

                let can_terminate = self.can_terminate(name)?;
                if !can_terminate {
                    self.dependencies
                        .entry(name.clone())
                        .or_default()
                        .push(product_type.clone());
                }

                Ok(can_terminate)
            }

            Array(elem_type) => self.check_field_type_terminates(elem_type, product_type),

            Tuple(types) => {
                for t in types {
                    if !self.check_field_type_terminates(t, product_type)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }

            Map(key_type, val_type) => {
                if !self.check_field_type_terminates(key_type, product_type)? {
                    return Ok(false);
                }
                self.check_field_type_terminates(val_type, product_type)
            }

            Closure(param_type, return_type) => {
                if !self.check_field_type_terminates(param_type, product_type)? {
                    return Ok(false);
                }
                self.check_field_type_terminates(return_type, product_type)
            }

            Starred(inner_type) | Dollared(inner_type) => {
                self.check_field_type_terminates(inner_type, product_type)
            }

            _ => Ok(true),
        }
    }

    /// Reset state for checking the next type.
    pub(super) fn reset(&mut self) {
        self.path.clear();
        self.explore_status
            .retain(|_, status| *status == ExplorationStatus::Terminated);
        self.dependencies.clear();
    }

    /// Find the cycle start index in the path.
    pub(super) fn find_cycle_start_index(&self) -> Option<usize> {
        if self.path.is_empty() {
            return None;
        }

        let last_element = self.path.last().unwrap();

        (0..self.path.len() - 1)
            .rev()
            .find(|&i| **self.path[i] == **last_element)
    }
}
