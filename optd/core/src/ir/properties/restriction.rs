use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use crate::ir::{
    ColumnMap, OperatorKind,
    operator::LogicalSelect,
    properties::{Derive, GetProperty},
    scalar::{BinaryOp, ColumnRef, Literal, NaryOp},
};

pub struct ColumnRestrictions;
impl super::PropertyMarker for ColumnRestrictions {
    type Output = Arc<ColumnMap<Restrictions<crate::ir::ScalarValue>>>;
}

impl Derive<ColumnRestrictions> for crate::ir::Operator {
    fn derive_by_compute(
        &self,
        ctx: &crate::ir::context::IRContext,
    ) -> <ColumnRestrictions as super::PropertyMarker>::Output {
        match &self.kind {
            OperatorKind::LogicalSelect(meta) => {
                let filter = LogicalSelect::borrow_raw_parts(meta, &self.common);
                let mut restrictions = filter
                    .input()
                    .get_property::<ColumnRestrictions>(ctx)
                    .as_ref()
                    .clone();
                let predicate = filter.predicate();
                add_restrictions_from_predicate(&mut restrictions, predicate);
                Arc::new(restrictions)
            }
            _ => self
                .input_operators()
                .iter()
                .fold(Arc::new(ColumnMap::new()), |acc, op| {
                    let op_restrictions = op.get_property::<ColumnRestrictions>(ctx);
                    let mut merged = (*acc).clone();
                    for (col, res) in op_restrictions.iter() {
                        merged
                            .entry(*col)
                            .and_modify(|existing| {
                                let combined = Restrictions::from_vec(
                                    [existing.ranges.clone(), res.ranges.clone()].concat(),
                                )
                                .simplify();
                                *existing = combined;
                            })
                            .or_insert_with(|| res.clone());
                    }
                    Arc::new(merged)
                }),
        }
    }
}

fn add_restrictions_from_predicate(
    restrictions: &mut ColumnMap<Restrictions<crate::ir::ScalarValue>>,
    predicate: &crate::ir::Scalar,
) {
    match &predicate.kind {
        crate::ir::ScalarKind::BinaryOp(meta) => {
            let binary_op = BinaryOp::borrow_raw_parts(&meta, &predicate.common);
            match (
                binary_op.lhs().try_borrow::<ColumnRef>(),
                binary_op.rhs().try_borrow::<Literal>(),
            ) {
                (Ok(column_ref), Ok(literal)) => {
                    let column = *column_ref.column();
                    let value = literal.value();
                    let restriction = match binary_op.op_kind() {
                        crate::ir::scalar::BinaryOpKind::Eq => Restriction::new(
                            Bound::Included(value.clone()),
                            Bound::Included(value.clone()),
                        ),
                        crate::ir::scalar::BinaryOpKind::Lt => {
                            Restriction::new(Bound::Unbounded, Bound::Excluded(value.clone()))
                        }
                        crate::ir::scalar::BinaryOpKind::Le => {
                            Restriction::new(Bound::Unbounded, Bound::Included(value.clone()))
                        }

                        crate::ir::scalar::BinaryOpKind::Gt => {
                            Restriction::new(Bound::Excluded(value.clone()), Bound::Unbounded)
                        }
                        crate::ir::scalar::BinaryOpKind::Ge => {
                            Restriction::new(Bound::Included(value.clone()), Bound::Unbounded)
                        }
                        _ => return,
                    };
                    let col_restrictions =
                        restrictions.entry(column).or_insert_with(Restrictions::new);

                    col_restrictions.ranges.push(restriction);
                    *col_restrictions = col_restrictions.simplify();
                }
                _ => return,
            }
        }
        crate::ir::ScalarKind::NaryOp(meta) => {
            let nary_op = NaryOp::borrow_raw_parts(&meta, &predicate.common);
            for term in nary_op.terms() {
                add_restrictions_from_predicate(restrictions, term);
            }
        }
        _ => return,
    }
}

/// A collection of disjoint restrictions representing the union (OR) of multiple ranges.
/// Invariant: ranges should be disjoint and sorted for efficient operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Restrictions<T> {
    ranges: Vec<Restriction<T>>,
}

impl<T> Restrictions<T> {
    /// Creates a new empty Restrictions collection
    pub fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    /// Creates a Restrictions collection from a vector of restrictions
    pub fn from_vec(ranges: Vec<Restriction<T>>) -> Self {
        Self { ranges }
    }

    /// Returns true if there are no restrictions
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Returns the number of restrictions
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Simplifies the restrictions by merging overlapping ranges or intersecting
    /// ranges that cover different parts of the space (like one providing lower bound,
    /// another providing upper bound).
    /// Returns a new Restrictions with sorted, merged ranges.
    pub fn simplify(&self) -> Self
    where
        T: PartialOrd + Clone,
    {
        if self.ranges.is_empty() {
            return Self::new();
        }

        // Filter out empty restrictions
        let non_empty: Vec<_> = self
            .ranges
            .iter()
            .filter(|r| !r.is_empty())
            .cloned()
            .collect();

        if non_empty.is_empty() {
            return Self::new();
        }

        // If all ranges overlap (no disjoint ranges), intersect them all
        // This handles cases like "x >= 1 AND x < 10" where both ranges overlap
        let all_overlap = non_empty.iter().all(|r1| {
            non_empty
                .iter()
                .all(|r2| r1.overlaps(r2) || std::ptr::eq(r1, r2))
        });

        if all_overlap {
            // Intersect all restrictions (AND logic for overlapping constraints)
            let mut result = non_empty[0].clone();
            for next in non_empty.iter().skip(1) {
                result = result.intersection(next);
                if result.is_empty() {
                    return Self::new();
                }
            }
            return Self {
                ranges: vec![result],
            };
        }

        // Otherwise, merge overlapping and adjacent restrictions (OR logic for disjoint ranges)
        let mut sorted = non_empty;
        sorted.sort_by(|a, b| compare_lower_bounds(&a.lower, &b.lower).unwrap());

        let mut result = Vec::new();
        let mut current = sorted[0].clone();

        for next in sorted.iter().skip(1) {
            if current.can_merge(next) {
                current = current.merge(next);
            } else {
                result.push(current);
                current = next.clone();
            }
        }
        result.push(current);

        Self { ranges: result }
    }

    /// Returns an iterator over the restrictions
    pub fn iter(&self) -> std::slice::Iter<'_, Restriction<T>> {
        self.ranges.iter()
    }
}

impl<T> Default for Restrictions<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Restriction<T> {
    lower: Bound<T>,
    upper: Bound<T>,
}

impl<T> RangeBounds<T> for Restriction<T> {
    fn start_bound(&self) -> Bound<&T> {
        self.lower.as_ref()
    }

    fn end_bound(&self) -> Bound<&T> {
        self.upper.as_ref()
    }
}

impl<T> Restriction<T> {
    /// Creates an unbounded restriction (all values)
    pub fn unbounded() -> Self {
        Self {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }
    }

    /// Creates a restriction with specified lower and upper bounds
    pub fn new(lower: Bound<T>, upper: Bound<T>) -> Self {
        Self { lower, upper }
    }

    /// Creates a restriction from any type implementing RangeBounds
    pub fn from_range(range: impl RangeBounds<T>) -> Self
    where
        T: Clone,
    {
        let lower = range.start_bound().cloned();
        let upper = range.end_bound().cloned();
        Self { lower, upper }
    }

    /// Checks if this restriction is empty (no values can satisfy it)
    pub fn is_empty(&self) -> bool
    where
        T: PartialOrd,
    {
        match (&self.lower, &self.upper) {
            (Bound::Included(l), Bound::Included(u)) => l > u,
            (Bound::Included(l), Bound::Excluded(u)) => l >= u,
            (Bound::Excluded(l), Bound::Included(u)) => l >= u,
            (Bound::Excluded(l), Bound::Excluded(u)) => l >= u,
            _ => false,
        }
    }

    /// Checks if this restriction is unbounded (all values)
    pub fn is_unbounded(&self) -> bool {
        matches!(
            (&self.lower, &self.upper),
            (Bound::Unbounded, Bound::Unbounded)
        )
    }

    /// Checks if a value is contained within this restriction
    pub fn contains(&self, value: &T) -> bool
    where
        T: Ord,
    {
        let lower_ok = match &self.lower {
            Bound::Included(l) => value >= l,
            Bound::Excluded(l) => value > l,
            Bound::Unbounded => true,
        };
        let upper_ok = match &self.upper {
            Bound::Included(u) => value <= u,
            Bound::Excluded(u) => value < u,
            Bound::Unbounded => true,
        };
        lower_ok && upper_ok
    }

    /// Computes the intersection of two restrictions
    pub fn intersection(&self, other: &Self) -> Self
    where
        T: PartialOrd + Clone,
    {
        let lower = max_bound(&self.lower, &other.lower);
        let upper = min_bound(&self.upper, &other.upper);
        Self { lower, upper }
    }

    /// Checks if two restrictions overlap
    pub fn overlaps(&self, other: &Self) -> bool
    where
        T: PartialOrd + Clone,
    {
        !self.intersection(other).is_empty()
    }

    /// Checks if this restriction is adjacent to or can be merged with another
    /// Two restrictions can be merged if they overlap or are adjacent
    pub fn can_merge(&self, other: &Self) -> bool
    where
        T: PartialOrd + Clone,
    {
        if self.overlaps(other) {
            return true;
        }

        // Check if they are adjacent (e.g., [1,5) and [5,10])
        let adjacent_right = match (&self.upper, &other.lower) {
            (Bound::Excluded(u), Bound::Included(l)) | (Bound::Included(u), Bound::Excluded(l)) => {
                u == l
            }
            _ => false,
        };

        let adjacent_left = match (&other.upper, &self.lower) {
            (Bound::Excluded(u), Bound::Included(l)) | (Bound::Included(u), Bound::Excluded(l)) => {
                u == l
            }
            _ => false,
        };

        adjacent_right || adjacent_left
    }

    /// Merges two restrictions into one
    /// Assumes the restrictions overlap or are adjacent
    pub fn merge(&self, other: &Self) -> Self
    where
        T: PartialOrd + Clone,
    {
        let lower = min_lower_bound(&self.lower, &other.lower);
        let upper = max_upper_bound(&self.upper, &other.upper);
        Self { lower, upper }
    }
}

/// Helper function to find the maximum of two lower bounds
fn max_bound<T: PartialOrd + Clone>(a: &Bound<T>, b: &Bound<T>) -> Bound<T> {
    match (a, b) {
        (Bound::Unbounded, other) | (other, Bound::Unbounded) => other.clone(),
        (Bound::Included(x), Bound::Included(y)) => {
            Bound::Included(if x >= y { x.clone() } else { y.clone() })
        }
        (Bound::Included(x), Bound::Excluded(y)) | (Bound::Excluded(y), Bound::Included(x)) => {
            if x > y {
                Bound::Included(x.clone())
            } else if x == y {
                Bound::Excluded(y.clone())
            } else {
                Bound::Excluded(y.clone())
            }
        }
        (Bound::Excluded(x), Bound::Excluded(y)) => {
            Bound::Excluded(if x >= y { x.clone() } else { y.clone() })
        }
    }
}

/// Helper function to find the minimum of two upper bounds
fn min_bound<T: PartialOrd + Clone>(a: &Bound<T>, b: &Bound<T>) -> Bound<T> {
    match (a, b) {
        (Bound::Unbounded, other) | (other, Bound::Unbounded) => other.clone(),
        (Bound::Included(x), Bound::Included(y)) => {
            Bound::Included(if x <= y { x.clone() } else { y.clone() })
        }
        (Bound::Included(x), Bound::Excluded(y)) | (Bound::Excluded(y), Bound::Included(x)) => {
            if x < y {
                Bound::Included(x.clone())
            } else if x == y {
                Bound::Excluded(y.clone())
            } else {
                Bound::Excluded(y.clone())
            }
        }
        (Bound::Excluded(x), Bound::Excluded(y)) => {
            Bound::Excluded(if x <= y { x.clone() } else { y.clone() })
        }
    }
}

/// Helper function to find the minimum of two lower bounds (for merging)
fn min_lower_bound<T: PartialOrd + Clone>(a: &Bound<T>, b: &Bound<T>) -> Bound<T> {
    match (a, b) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => Bound::Unbounded,
        (Bound::Included(x), Bound::Included(y)) => {
            Bound::Included(if x <= y { x.clone() } else { y.clone() })
        }
        (Bound::Included(x), Bound::Excluded(y)) | (Bound::Excluded(y), Bound::Included(x)) => {
            if x < y {
                Bound::Included(x.clone())
            } else if x == y {
                Bound::Included(x.clone())
            } else {
                Bound::Excluded(y.clone())
            }
        }
        (Bound::Excluded(x), Bound::Excluded(y)) => {
            Bound::Excluded(if x <= y { x.clone() } else { y.clone() })
        }
    }
}

/// Helper function to find the maximum of two upper bounds (for merging)
fn max_upper_bound<T: PartialOrd + Clone>(a: &Bound<T>, b: &Bound<T>) -> Bound<T> {
    match (a, b) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => Bound::Unbounded,
        (Bound::Included(x), Bound::Included(y)) => {
            Bound::Included(if x >= y { x.clone() } else { y.clone() })
        }
        (Bound::Included(x), Bound::Excluded(y)) | (Bound::Excluded(y), Bound::Included(x)) => {
            if x > y {
                Bound::Included(x.clone())
            } else if x == y {
                Bound::Included(x.clone())
            } else {
                Bound::Excluded(y.clone())
            }
        }
        (Bound::Excluded(x), Bound::Excluded(y)) => {
            Bound::Excluded(if x >= y { x.clone() } else { y.clone() })
        }
    }
}

/// Helper function to compare lower bounds for sorting
fn compare_lower_bounds<T: PartialOrd>(a: &Bound<T>, b: &Bound<T>) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;

    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Some(Ordering::Equal),
        (Bound::Unbounded, _) => Some(Ordering::Less),
        (_, Bound::Unbounded) => Some(Ordering::Greater),
        (Bound::Included(x), Bound::Included(y)) | (Bound::Excluded(x), Bound::Excluded(y)) => {
            x.partial_cmp(y)
        }
        (Bound::Included(x), Bound::Excluded(y)) => {
            match x.partial_cmp(y) {
                Some(Ordering::Equal) => Some(Ordering::Less), // Included comes before Excluded at same value
                other => other,
            }
        }
        (Bound::Excluded(x), Bound::Included(y)) => {
            match x.partial_cmp(y) {
                Some(Ordering::Equal) => Some(Ordering::Greater), // Excluded comes after Included at same value
                other => other,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::{
        Column, IRContext,
        builder::{column_ref, integer},
        catalog::DataSourceId,
        statistics::{Bucket, Histogram, ValueHistogram},
    };

    use super::*;

    #[test]
    fn test_derive_restrictions() {
        let ctx = IRContext::with_empty_magic();

        // SELECT * FROM table WHERE col1 > 5 AND col1 <= 10
        let plan = ctx.mock_scan(1, vec![1, 2, 3], 150.).logical_select(
            column_ref(Column(1))
                .gt(integer(5))
                .and(column_ref(Column(1)).le(integer(10))),
        );

        let hist_col1 = Histogram::new(vec![
            Bucket::new(5, 50., 5.),
            Bucket::new(10, 50., 3.),
            Bucket::new(15, 50., 2.),
        ]);
        ctx.set_histogram(&Column(1), ValueHistogram::Int32(hist_col1));

        let restrictions = plan.get_property::<ColumnRestrictions>(&ctx);
        let col_rest = restrictions.get(&Column(1)).unwrap();
        assert_eq!(col_rest.ranges.len(), 1);
        assert_eq!(
            col_rest.ranges[0],
            Restriction::new(
                Bound::Excluded(crate::ir::ScalarValue::Int32(Some(5))),
                Bound::Included(crate::ir::ScalarValue::Int32(Some(10))),
            )
        );
        let card = plan.get_property::<crate::ir::properties::Cardinality>(&ctx);
        println!("card: {:?}", card);
    }

    #[test]
    fn test_restriction_unbounded() {
        let r = Restriction::<i32>::unbounded();
        assert!(r.is_unbounded());
        assert!(!r.is_empty());
        assert!(r.contains(&0));
        assert!(r.contains(&-1000));
        assert!(r.contains(&1000));
    }

    #[test]
    fn test_restriction_from_range() {
        let r = Restriction::from_range(1..5);
        assert_eq!(r.lower, Bound::Included(1));
        assert_eq!(r.upper, Bound::Excluded(5));
        assert!(r.contains(&1));
        assert!(r.contains(&4));
        assert!(!r.contains(&5));
        assert!(!r.contains(&0));
    }

    #[test]
    fn test_restriction_is_empty() {
        let empty = Restriction::new(Bound::Included(5), Bound::Excluded(5));
        assert!(empty.is_empty());

        let also_empty = Restriction::new(Bound::Included(5), Bound::Included(4));
        assert!(also_empty.is_empty());

        let not_empty = Restriction::new(Bound::Included(5), Bound::Included(5));
        assert!(!not_empty.is_empty());
    }

    #[test]
    fn test_restriction_contains() {
        let r = Restriction::new(Bound::Included(1), Bound::Excluded(10));

        assert!(r.contains(&1));
        assert!(r.contains(&5));
        assert!(r.contains(&9));
        assert!(!r.contains(&0));
        assert!(!r.contains(&10));
        assert!(!r.contains(&11));
    }

    #[test]
    fn test_restriction_intersection() {
        let r1 = Restriction::new(Bound::Included(1), Bound::Excluded(10));
        let r2 = Restriction::new(Bound::Included(5), Bound::Included(15));

        let intersection = r1.intersection(&r2);
        assert_eq!(intersection.lower, Bound::Included(5));
        assert_eq!(intersection.upper, Bound::Excluded(10));
    }

    #[test]
    fn test_restriction_overlaps() {
        let r1 = Restriction::new(Bound::Included(1), Bound::Excluded(10));
        let r2 = Restriction::new(Bound::Included(5), Bound::Included(15));
        let r3 = Restriction::new(Bound::Included(10), Bound::Included(20));

        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));
        assert!(!r1.overlaps(&r3));
    }

    #[test]
    fn test_restriction_can_merge_overlapping() {
        let r1 = Restriction::new(Bound::Included(1), Bound::Excluded(10));
        let r2 = Restriction::new(Bound::Included(5), Bound::Included(15));

        assert!(r1.can_merge(&r2));
        assert!(r2.can_merge(&r1));
    }

    #[test]
    fn test_restriction_can_merge_adjacent() {
        // [1, 5) and [5, 10] are adjacent
        let r1 = Restriction::new(Bound::Included(1), Bound::Excluded(5));
        let r2 = Restriction::new(Bound::Included(5), Bound::Included(10));

        assert!(r1.can_merge(&r2));
        assert!(r2.can_merge(&r1));

        // [1, 5] and (5, 10] are adjacent
        let r3 = Restriction::new(Bound::Included(1), Bound::Included(5));
        let r4 = Restriction::new(Bound::Excluded(5), Bound::Included(10));

        assert!(r3.can_merge(&r4));
        assert!(r4.can_merge(&r3));
    }

    #[test]
    fn test_restriction_merge() {
        let r1 = Restriction::new(Bound::Included(1), Bound::Excluded(10));
        let r2 = Restriction::new(Bound::Included(5), Bound::Included(15));

        let merged = r1.merge(&r2);
        assert_eq!(merged.lower, Bound::Included(1));
        assert_eq!(merged.upper, Bound::Included(15));
    }

    #[test]
    fn test_restrictions_empty() {
        let restrictions = Restrictions::<i32>::new();
        assert!(restrictions.is_empty());
        assert_eq!(restrictions.len(), 0);
    }

    #[test]
    fn test_restrictions_simplify_empty() {
        let restrictions = Restrictions::<i32>::new();
        let simplified = restrictions.simplify();
        assert!(simplified.is_empty());
    }

    #[test]
    fn test_restrictions_simplify_single() {
        let r1 = Restriction::new(Bound::Included(1), Bound::Excluded(10));
        let restrictions = Restrictions::from_vec(vec![r1.clone()]);

        let simplified = restrictions.simplify();
        assert_eq!(simplified.len(), 1);
        assert_eq!(simplified.iter().next().unwrap(), &r1);
    }

    #[test]
    fn test_restrictions_simplify_overlapping() {
        let r1 = Restriction::new(Bound::Included(1), Bound::Excluded(10));
        let r2 = Restriction::new(Bound::Included(5), Bound::Included(15));
        let r3 = Restriction::new(Bound::Included(12), Bound::Included(20));

        let restrictions = Restrictions::from_vec(vec![r1, r2, r3]);
        let simplified = restrictions.simplify();

        assert_eq!(simplified.len(), 1);
        let merged = simplified.iter().next().unwrap();
        assert_eq!(merged.lower, Bound::Included(1));
        assert_eq!(merged.upper, Bound::Included(20));
    }

    #[test]
    fn test_restrictions_simplify_between() {
        //  x >= 1,
        let r1 = Restriction::new(Bound::Included(1), Bound::Unbounded);
        // x < 10,
        let r2 = Restriction::new(Bound::Unbounded, Bound::Excluded(10));

        let restrictions = Restrictions::from_vec(vec![r1, r2]);
        let simplified = restrictions.simplify();

        assert_eq!(simplified.len(), 1);
        let merged = simplified.iter().next().unwrap();
        assert_eq!(merged.lower, Bound::Included(1));
        assert_eq!(merged.upper, Bound::Excluded(10));
    }

    #[test]
    fn test_restrictions_simplify_adjacent() {
        // [1, 5), [5, 10), [10, 15] should merge into [1, 15]
        let r1 = Restriction::new(Bound::Included(1), Bound::Excluded(5));
        let r2 = Restriction::new(Bound::Included(5), Bound::Excluded(10));
        let r3 = Restriction::new(Bound::Included(10), Bound::Included(15));

        let restrictions = Restrictions::from_vec(vec![r1, r2, r3]);
        let simplified = restrictions.simplify();

        assert_eq!(simplified.len(), 1);
        let merged = simplified.iter().next().unwrap();
        assert_eq!(merged.lower, Bound::Included(1));
        assert_eq!(merged.upper, Bound::Included(15));
    }

    #[test]
    fn test_restrictions_simplify_disjoint() {
        let r1 = Restriction::new(Bound::Included(1), Bound::Excluded(5));
        let r2 = Restriction::new(Bound::Included(10), Bound::Included(15));
        let r3 = Restriction::new(Bound::Included(20), Bound::Included(25));

        let restrictions = Restrictions::from_vec(vec![r1.clone(), r2.clone(), r3.clone()]);
        let simplified = restrictions.simplify();

        assert_eq!(simplified.len(), 3);
        // Should be sorted
        let ranges: Vec<_> = simplified.iter().collect();
        assert_eq!(ranges[0], &r1);
        assert_eq!(ranges[1], &r2);
        assert_eq!(ranges[2], &r3);
    }

    #[test]
    fn test_restrictions_simplify_unsorted() {
        // Provide restrictions in unsorted order
        let r1 = Restriction::new(Bound::Included(10), Bound::Included(15));
        let r2 = Restriction::new(Bound::Included(1), Bound::Excluded(5));
        let r3 = Restriction::new(Bound::Included(20), Bound::Included(25));

        let restrictions = Restrictions::from_vec(vec![r1, r2.clone(), r3.clone()]);
        let simplified = restrictions.simplify();

        assert_eq!(simplified.len(), 3);
        // Should be sorted after simplification
        let ranges: Vec<_> = simplified.iter().collect();
        assert_eq!(ranges[0], &r2);
    }

    #[test]
    fn test_restrictions_simplify_with_empty() {
        let r1 = Restriction::new(Bound::Included(1), Bound::Excluded(5));
        let empty = Restriction::new(Bound::Included(10), Bound::Excluded(10));
        let r2 = Restriction::new(Bound::Included(15), Bound::Included(20));

        let restrictions = Restrictions::from_vec(vec![r1.clone(), empty, r2.clone()]);
        let simplified = restrictions.simplify();

        // Empty restriction should be filtered out
        assert_eq!(simplified.len(), 2);
        let ranges: Vec<_> = simplified.iter().collect();
        assert_eq!(ranges[0], &r1);
        assert_eq!(ranges[1], &r2);
    }

    #[test]
    fn test_restrictions_simplify_complex() {
        // Mix of overlapping, adjacent, and disjoint ranges
        let r1 = Restriction::new(Bound::Included(1), Bound::Excluded(5));
        let r2 = Restriction::new(Bound::Included(3), Bound::Included(7));
        let r3 = Restriction::new(Bound::Excluded(7), Bound::Included(10));
        let r4 = Restriction::new(Bound::Included(15), Bound::Included(20));
        let r5 = Restriction::new(Bound::Included(18), Bound::Included(25));

        let restrictions = Restrictions::from_vec(vec![r1, r2, r3, r4, r5]);
        let simplified = restrictions.simplify();

        // Should result in [1, 10] and [15, 25]
        assert_eq!(simplified.len(), 2);
        let ranges: Vec<_> = simplified.iter().collect();
        assert_eq!(ranges[0].lower, Bound::Included(1));
        assert_eq!(ranges[0].upper, Bound::Included(10));
        assert_eq!(ranges[1].lower, Bound::Included(15));
        assert_eq!(ranges[1].upper, Bound::Included(25));
    }
}
