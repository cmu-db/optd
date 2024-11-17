use optd_core::nodes::VariantTag;
use serde::{Deserialize, Serialize};

use super::{
    BinOpType, ConstantType, DfNodeType, DfPredType, JoinType, LogOpType, SortOrderType, UnOpType,
};

/// ## Serialization
/// A variant tag for [`DfPredType`] has two parts. An `u8` discriminant
/// followed an `u8` extra field (could be [`ConstantType`], [`UnOpType`], etc.),
/// listed in big-endian order.
///
/// ```
/// ----------------------------------------
/// | discriminant (u8) | extra field (u8) |
/// ----------------------------------------
/// ```
/// See https://doc.rust-lang.org/std/mem/fn.discriminant.html.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::FromRepr,
    strum::EnumIter,
    strum::EnumCount,
)]
#[repr(u8)]
enum DfPredTypeFlat {
    List,
    Constant,
    ColumnRef,
    ExternColumnRef,
    UnOp,
    BinOp,
    LogOp,
    Func,
    SortOrder,
    Between,
    Cast,
    Like,
    DataType,
    InList,
}

/// ## Serialization
/// A variant tag for [`DfNodeType`] has two parts. An `u8` discriminant
/// followed an `u8` extra field ([`JoinType`] for now), listed in big-endian order.
///
/// ```
/// ----------------------------------------
/// | discriminant (u8) | extra field (u8) |
/// ----------------------------------------
/// ```
/// See https://doc.rust-lang.org/std/mem/fn.discriminant.html.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::FromRepr,
    strum::EnumIter,
    strum::EnumCount,
)]
#[repr(u8)]
pub enum DfNodeTypeFlat {
    // Logical plan nodes
    Projection,
    Filter,
    Scan,
    Join,
    RawDepJoin,
    DepJoin,
    Sort,
    Agg,
    EmptyRelation,
    Limit,
    // Physical plan nodes
    PhysicalProjection,
    PhysicalFilter,
    PhysicalScan,
    PhysicalSort,
    PhysicalAgg,
    PhysicalHashJoin,
    PhysicalNestedLoopJoin,
    PhysicalEmptyRelation,
    PhysicalLimit,
}

impl TryFrom<VariantTag> for DfPredType {
    type Error = u16;

    fn try_from(value: VariantTag) -> Result<Self, Self::Error> {
        let VariantTag(v) = value;
        let [discriminant, rest] = v.to_be_bytes();
        let typ = {
            let flat = DfPredTypeFlat::from_repr(discriminant).ok_or_else(|| v)?;
            match flat {
                DfPredTypeFlat::Constant => {
                    DfPredType::Constant(ConstantType::from_repr(rest).ok_or_else(|| v)?)
                }

                DfPredTypeFlat::UnOp => {
                    DfPredType::UnOp(UnOpType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfPredTypeFlat::BinOp => {
                    DfPredType::BinOp(BinOpType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfPredTypeFlat::LogOp => {
                    DfPredType::LogOp(LogOpType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfPredTypeFlat::SortOrder => {
                    DfPredType::SortOrder(SortOrderType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfPredTypeFlat::ColumnRef => DfPredType::ColumnRef,
                DfPredTypeFlat::ExternColumnRef => DfPredType::ExternColumnRef,
                DfPredTypeFlat::List => DfPredType::List,
                DfPredTypeFlat::Func => DfPredType::Func,
                DfPredTypeFlat::Between => DfPredType::Between,
                DfPredTypeFlat::Cast => DfPredType::Cast,
                DfPredTypeFlat::Like => DfPredType::Like,
                DfPredTypeFlat::DataType => DfPredType::DataType,
                DfPredTypeFlat::InList => DfPredType::InList,
            }
        };

        Ok(typ)
    }
}

impl From<DfPredType> for VariantTag {
    fn from(value: DfPredType) -> Self {
        let (discriminant, rest) = {
            match value {
                DfPredType::Constant(constant_type) => {
                    (DfPredTypeFlat::Constant as u8, constant_type as u8)
                }
                DfPredType::UnOp(un_op_type) => (DfPredTypeFlat::UnOp as u8, un_op_type as u8),
                DfPredType::BinOp(bin_op_type) => (DfPredTypeFlat::BinOp as u8, bin_op_type as u8),
                DfPredType::LogOp(log_op_type) => (DfPredTypeFlat::LogOp as u8, log_op_type as u8),
                DfPredType::SortOrder(sort_order_type) => {
                    (DfPredTypeFlat::SortOrder as u8, sort_order_type as u8)
                }
                DfPredType::List => (DfPredTypeFlat::List as u8, 0),
                DfPredType::ColumnRef => (DfPredTypeFlat::ColumnRef as u8, 0),
                DfPredType::ExternColumnRef => (DfPredTypeFlat::ExternColumnRef as u8, 0),
                DfPredType::Func => (DfPredTypeFlat::Func as u8, 0),
                DfPredType::Between => (DfPredTypeFlat::Between as u8, 0),
                DfPredType::Cast => (DfPredTypeFlat::Cast as u8, 0),
                DfPredType::Like => (DfPredTypeFlat::Like as u8, 0),
                DfPredType::DataType => (DfPredTypeFlat::DataType as u8, 0),
                DfPredType::InList => (DfPredTypeFlat::InList as u8, 0),
            }
        };
        VariantTag(u16::from_be_bytes([discriminant, rest]))
    }
}

impl TryFrom<VariantTag> for DfNodeType {
    type Error = u16;

    fn try_from(value: VariantTag) -> Result<Self, Self::Error> {
        let VariantTag(v) = value;
        let [discriminant, rest] = v.to_be_bytes();
        let typ = {
            let flat = DfNodeTypeFlat::from_repr(discriminant).ok_or_else(|| v)?;
            match flat {
                DfNodeTypeFlat::Join => {
                    DfNodeType::Join(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeTypeFlat::RawDepJoin => {
                    DfNodeType::RawDepJoin(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeTypeFlat::DepJoin => {
                    DfNodeType::DepJoin(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeTypeFlat::PhysicalHashJoin => {
                    DfNodeType::PhysicalHashJoin(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeTypeFlat::PhysicalNestedLoopJoin => {
                    DfNodeType::PhysicalNestedLoopJoin(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeTypeFlat::Projection => DfNodeType::Projection,
                DfNodeTypeFlat::Filter => DfNodeType::Filter,
                DfNodeTypeFlat::Scan => DfNodeType::Scan,
                DfNodeTypeFlat::Sort => DfNodeType::Sort,
                DfNodeTypeFlat::Agg => DfNodeType::Agg,
                DfNodeTypeFlat::EmptyRelation => DfNodeType::EmptyRelation,
                DfNodeTypeFlat::Limit => DfNodeType::Limit,
                DfNodeTypeFlat::PhysicalProjection => DfNodeType::PhysicalProjection,
                DfNodeTypeFlat::PhysicalFilter => DfNodeType::PhysicalFilter,
                DfNodeTypeFlat::PhysicalScan => DfNodeType::PhysicalScan,
                DfNodeTypeFlat::PhysicalSort => DfNodeType::PhysicalSort,
                DfNodeTypeFlat::PhysicalAgg => DfNodeType::PhysicalAgg,
                DfNodeTypeFlat::PhysicalEmptyRelation => DfNodeType::PhysicalEmptyRelation,
                DfNodeTypeFlat::PhysicalLimit => DfNodeType::PhysicalLimit,
            }
        };
        Ok(typ)
    }
}

impl From<DfNodeType> for VariantTag {
    fn from(value: DfNodeType) -> Self {
        let (discriminant, rest) = match value {
            DfNodeType::Join(join_type) => (DfNodeTypeFlat::Join as u8, join_type as u8),
            DfNodeType::RawDepJoin(join_type) => {
                (DfNodeTypeFlat::RawDepJoin as u8, join_type as u8)
            }
            DfNodeType::DepJoin(join_type) => (DfNodeTypeFlat::DepJoin as u8, join_type as u8),
            DfNodeType::PhysicalHashJoin(join_type) => {
                (DfNodeTypeFlat::PhysicalHashJoin as u8, join_type as u8)
            }
            DfNodeType::PhysicalNestedLoopJoin(join_type) => (
                DfNodeTypeFlat::PhysicalNestedLoopJoin as u8,
                join_type as u8,
            ),
            DfNodeType::Projection => (DfNodeTypeFlat::Projection as u8, 0),
            DfNodeType::Filter => (DfNodeTypeFlat::Filter as u8, 0),
            DfNodeType::Scan => (DfNodeTypeFlat::Scan as u8, 0),
            DfNodeType::Sort => (DfNodeTypeFlat::Sort as u8, 0),
            DfNodeType::Agg => (DfNodeTypeFlat::Agg as u8, 0),
            DfNodeType::EmptyRelation => (DfNodeTypeFlat::EmptyRelation as u8, 0),
            DfNodeType::Limit => (DfNodeTypeFlat::Limit as u8, 0),
            DfNodeType::PhysicalProjection => (DfNodeTypeFlat::PhysicalProjection as u8, 0),
            DfNodeType::PhysicalFilter => (DfNodeTypeFlat::PhysicalFilter as u8, 0),
            DfNodeType::PhysicalScan => (DfNodeTypeFlat::PhysicalScan as u8, 0),
            DfNodeType::PhysicalSort => (DfNodeTypeFlat::PhysicalSort as u8, 0),
            DfNodeType::PhysicalAgg => (DfNodeTypeFlat::PhysicalAgg as u8, 0),
            DfNodeType::PhysicalEmptyRelation => (DfNodeTypeFlat::PhysicalEmptyRelation as u8, 0),
            DfNodeType::PhysicalLimit => (DfNodeTypeFlat::PhysicalLimit as u8, 0),
        };
        VariantTag(u16::from_be_bytes([discriminant, rest]))
    }
}

#[cfg(test)]
impl DfNodeTypeFlat {
    fn is_join(&self) -> bool {
        match self {
            DfNodeTypeFlat::Join
            | DfNodeTypeFlat::RawDepJoin
            | DfNodeTypeFlat::DepJoin
            | DfNodeTypeFlat::PhysicalHashJoin
            | DfNodeTypeFlat::PhysicalNestedLoopJoin => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use strum::{EnumCount, IntoEnumIterator};

    use super::*;

    #[test]
    fn test_df_node_type_to_tag_e2e() {
        let mut valid_tags = Vec::new();
        let mut invalid_tags = Vec::new();
        for flattened in DfNodeTypeFlat::iter() {
            let discriminant = flattened as u8;
            if flattened.is_join() {
                for join_type in JoinType::iter() {
                    valid_tags.push(VariantTag(u16::from_be_bytes([
                        discriminant,
                        join_type as u8,
                    ])));
                }
                invalid_tags.push(VariantTag(u16::from_be_bytes([discriminant, u8::MAX])));
            } else {
                valid_tags.push(VariantTag(u16::from_be_bytes([discriminant, 0])));
            }
        }
        invalid_tags.push(VariantTag(u16::from_be_bytes([
            DfNodeTypeFlat::COUNT as u8,
            0,
        ])));

        invalid_tags.into_iter().for_each(|tag| {
            DfNodeType::try_from(tag).unwrap_err();
        });

        valid_tags.iter().for_each(|&tag| {
            let new_tag = VariantTag::from(DfNodeType::try_from(tag).unwrap());
            assert_eq!(tag, new_tag);
        });

        valid_tags.into_iter().combinations(2).for_each(|x| {
            let a = x[0];
            let b = x[1];
            assert_ne!(a, b);
            let a = DfNodeType::try_from(a).unwrap();
            let b = DfNodeType::try_from(b).unwrap();
            assert_ne!(a, b);
        });
    }

    #[test]
    fn test_df_pred_type_to_tag_e2e() {
        let mut valid_tags = Vec::new();
        let mut invalid_tags = Vec::new();
        for flattened in DfPredTypeFlat::iter() {
            let discriminant = flattened as u8;
            match flattened {
                DfPredTypeFlat::Constant => {
                    for constant_type in ConstantType::iter() {
                        valid_tags.push(VariantTag(u16::from_be_bytes([
                            discriminant,
                            constant_type as u8,
                        ])));
                    }
                    invalid_tags.push(VariantTag(u16::from_be_bytes([discriminant, u8::MAX])));
                }
                DfPredTypeFlat::UnOp => {
                    for un_op_type in UnOpType::iter() {
                        valid_tags.push(VariantTag(u16::from_be_bytes([
                            discriminant,
                            un_op_type as u8,
                        ])));
                    }
                    invalid_tags.push(VariantTag(u16::from_be_bytes([discriminant, u8::MAX])));
                }
                DfPredTypeFlat::BinOp => {
                    for bin_op_type in BinOpType::iter() {
                        valid_tags.push(VariantTag(u16::from_be_bytes([
                            discriminant,
                            bin_op_type as u8,
                        ])));
                    }
                    invalid_tags.push(VariantTag(u16::from_be_bytes([discriminant, u8::MAX])));
                }
                DfPredTypeFlat::LogOp => {
                    for log_op_type in LogOpType::iter() {
                        valid_tags.push(VariantTag(u16::from_be_bytes([
                            discriminant,
                            log_op_type as u8,
                        ])));
                    }
                    invalid_tags.push(VariantTag(u16::from_be_bytes([discriminant, u8::MAX])));
                }
                DfPredTypeFlat::SortOrder => {
                    for sort_order_type in SortOrderType::iter() {
                        valid_tags.push(VariantTag(u16::from_be_bytes([
                            discriminant,
                            sort_order_type as u8,
                        ])));
                    }
                    invalid_tags.push(VariantTag(u16::from_be_bytes([discriminant, u8::MAX])));
                }
                _ => valid_tags.push(VariantTag(u16::from_be_bytes([discriminant, 0]))),
            }
        }
        invalid_tags.push(VariantTag(u16::from_be_bytes([
            DfPredTypeFlat::COUNT as u8,
            0,
        ])));

        invalid_tags.into_iter().for_each(|tag| {
            DfPredType::try_from(tag).unwrap_err();
        });

        valid_tags.iter().for_each(|&tag| {
            let new_tag = VariantTag::from(DfPredType::try_from(tag).unwrap());
            assert_eq!(tag, new_tag);
        });

        valid_tags.into_iter().combinations(2).for_each(|x| {
            let a = x[0];
            let b = x[1];
            assert_ne!(a, b);
            let a = DfPredType::try_from(a).unwrap();
            let b = DfPredType::try_from(b).unwrap();
            assert_ne!(a, b);
        });
    }
}
