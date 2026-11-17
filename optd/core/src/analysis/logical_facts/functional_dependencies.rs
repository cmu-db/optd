//! Logical value dependencies, separate from estimated NDV and row selectivity.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Column, Scan};

use super::{AnalysisContext, LogicalFacts, ValueId};

/// A semantic functional dependency between query-local value identities.
///
/// Equal determinant tuples imply equal dependent tuples, including SQL grouping's
/// treatment of NULL. Catalog providers must satisfy that contract when asserting
/// a unique key. Scan-instance value identities prevent self-join aliases from
/// borrowing one another's dependencies.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ValueFunctionalDependency {
    /// Values that jointly determine the dependent values; an empty set is constant.
    pub determinants: BTreeSet<ValueId>,
    /// Values determined by the determinant tuple.
    pub dependents: BTreeSet<ValueId>,
}

impl LogicalFacts {
    /// Returns values implied by `values`, using equality and functional dependencies.
    /// Known literal equalities are included as constant values even without determinants.
    ///
    /// The result contains canonical value identities. This is a logical closure:
    /// neither point estimates nor observed uniqueness create dependencies.
    pub fn functional_dependency_closure(&self, values: &BTreeSet<ValueId>) -> BTreeSet<ValueId> {
        let canonicalize = |values: &BTreeSet<ValueId>| {
            values
                .iter()
                .map(|value| self.constraints.canonical(value))
                .collect::<BTreeSet<_>>()
        };
        let dependencies = self
            .functional_dependencies
            .iter()
            .map(|dependency| {
                (
                    canonicalize(&dependency.determinants),
                    canonicalize(&dependency.dependents),
                )
            })
            .collect::<Vec<_>>();
        let mut closure = canonicalize(values);
        // A literal equality determines its value even when no input key is known.
        closure.extend(
            self.constraints
                .literal_equalities
                .keys()
                .map(|value| self.constraints.canonical(value)),
        );
        loop {
            let old_len = closure.len();
            for (determinants, dependents) in &dependencies {
                if determinants.is_subset(&closure) {
                    closure.extend(dependents.iter().cloned());
                }
            }
            if closure.len() == old_len {
                return closure;
            }
        }
    }
}

pub(super) fn scan_dependencies(
    scan: &Scan,
    lineage: &BTreeMap<Column, ValueId>,
    analyses: &AnalysisContext,
) -> Vec<ValueFunctionalDependency> {
    // Constraints are optional metadata. Missing catalog entries retain the previous
    // lineage-only behavior rather than making logical analysis require statistics.
    let Ok(metadata) = analyses.catalog().table_by_ref(&scan.table) else {
        return Vec::new();
    };
    let Some(statistics) = metadata.statistics else {
        return Vec::new();
    };
    let values = lineage
        .values()
        .filter_map(|value| match value {
            ValueId::Base(base) => Some((base.name.as_str(), value)),
            ValueId::Derived(_) => None,
        })
        .collect::<BTreeMap<_, _>>();
    statistics
        .constraints
        .unique_keys
        .iter()
        .filter_map(|key| {
            let determinants = key
                .columns()
                .iter()
                .map(|name| values.get(name.as_str()).map(|value| (*value).clone()))
                .collect::<Option<BTreeSet<_>>>()?;
            Some(ValueFunctionalDependency {
                determinants,
                dependents: lineage.values().cloned().collect(),
            })
        })
        .collect()
}

pub(super) fn aggregation_dependencies(
    input: &LogicalFacts,
    grouping_values: &BTreeSet<ValueId>,
    output_values: &BTreeMap<Column, ValueId>,
) -> Vec<ValueFunctionalDependency> {
    // Input dependencies remain valid over grouping values, but aggregate results
    // must not inherit dependencies on arbitrary values removed by grouping.
    let mut dependencies = input
        .functional_dependencies
        .iter()
        .filter_map(|dependency| {
            if !dependency.determinants.is_subset(grouping_values) {
                return None;
            }
            let dependents = dependency
                .dependents
                .intersection(grouping_values)
                .cloned()
                .collect();
            Some(ValueFunctionalDependency {
                determinants: dependency.determinants.clone(),
                dependents,
            })
        })
        .collect::<Vec<_>>();
    // One aggregate output tuple per grouping tuple, including scalar aggregation.
    dependencies.push(ValueFunctionalDependency {
        determinants: grouping_values.clone(),
        dependents: output_values.values().cloned().collect(),
    });
    dependencies
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnData, QueryContext};
    use arrow_schema::DataType;

    #[test]
    fn closure_requires_composite_determinants_and_follows_transitive_dependencies() {
        let mut ctx = QueryContext::new();
        let values = ["a", "b", "c", "d"]
            .map(|name| ValueId::Derived(ColumnData::new(name, DataType::Int64).add(&mut ctx)));
        let [a, b, c, d] = values;
        let facts = LogicalFacts {
            functional_dependencies: vec![
                ValueFunctionalDependency {
                    determinants: [c.clone()].into_iter().collect(),
                    dependents: [d.clone()].into_iter().collect(),
                },
                ValueFunctionalDependency {
                    determinants: [a.clone(), b.clone()].into_iter().collect(),
                    dependents: [c.clone()].into_iter().collect(),
                },
            ],
            ..LogicalFacts::default()
        };
        assert!(
            !facts
                .functional_dependency_closure(&[a.clone()].into_iter().collect())
                .contains(&c)
        );
        let closure = facts.functional_dependency_closure(&[a, b].into_iter().collect());
        assert!(closure.contains(&c));
        assert!(closure.contains(&d));
    }

    #[test]
    fn cyclic_dependencies_retain_a_grouping_representative() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let av = ValueId::Derived(a);
        let bv = ValueId::Derived(b);
        let facts = LogicalFacts {
            lineage: [(a, av.clone()), (b, bv.clone())].into_iter().collect(),
            functional_dependencies: vec![
                ValueFunctionalDependency {
                    determinants: [av.clone()].into_iter().collect(),
                    dependents: [bv.clone()].into_iter().collect(),
                },
                ValueFunctionalDependency {
                    determinants: [bv].into_iter().collect(),
                    dependents: [av].into_iter().collect(),
                },
            ],
            ..LogicalFacts::default()
        };
        let keys = [a, b].map(|col| crate::ExprData::ColumnRef(col).add(&mut ctx));
        let reduced =
            crate::analysis::grouping_dependencies::independent_grouping_keys(&keys, &facts, &ctx);
        assert_eq!(reduced.len(), 1);
        let reversed = crate::analysis::grouping_dependencies::independent_grouping_keys(
            &[keys[1], keys[0]],
            &facts,
            &ctx,
        );
        assert_eq!(reduced, reversed);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn old_logical_facts_default_to_no_dependencies() {
        let mut encoded = serde_json::to_value(LogicalFacts::default()).unwrap();
        encoded
            .as_object_mut()
            .unwrap()
            .remove("functional_dependencies");
        let decoded: LogicalFacts = serde_json::from_value(encoded).unwrap();
        assert!(decoded.functional_dependencies.is_empty());
    }
}
