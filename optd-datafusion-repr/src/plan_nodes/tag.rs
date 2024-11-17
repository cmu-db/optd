use optd_core::nodes::{SerializedNodeTag, SerializedPredTag};
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
enum DfPredTypeFlattened {
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
pub enum DfNodeTypeFlattened {
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

impl TryFrom<SerializedPredTag> for DfPredType {
    type Error = u16;

    fn try_from(value: SerializedPredTag) -> Result<Self, Self::Error> {
        let SerializedPredTag(v) = value;
        let [discriminant, rest] = v.to_be_bytes();
        let typ = {
            let flattened = DfPredTypeFlattened::from_repr(discriminant).ok_or_else(|| v)?;
            match flattened {
                DfPredTypeFlattened::Constant => {
                    DfPredType::Constant(ConstantType::from_repr(rest).ok_or_else(|| v)?)
                }

                DfPredTypeFlattened::UnOp => {
                    DfPredType::UnOp(UnOpType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfPredTypeFlattened::BinOp => {
                    DfPredType::BinOp(BinOpType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfPredTypeFlattened::LogOp => {
                    DfPredType::LogOp(LogOpType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfPredTypeFlattened::SortOrder => {
                    DfPredType::SortOrder(SortOrderType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfPredTypeFlattened::ColumnRef => DfPredType::ColumnRef,
                DfPredTypeFlattened::ExternColumnRef => DfPredType::ExternColumnRef,
                DfPredTypeFlattened::List => DfPredType::List,
                DfPredTypeFlattened::Func => DfPredType::Func,
                DfPredTypeFlattened::Between => DfPredType::Between,
                DfPredTypeFlattened::Cast => DfPredType::Cast,
                DfPredTypeFlattened::Like => DfPredType::Like,
                DfPredTypeFlattened::DataType => DfPredType::DataType,
                DfPredTypeFlattened::InList => DfPredType::InList,
            }
        };

        Ok(typ)
    }
}

impl From<DfPredType> for SerializedPredTag {
    fn from(value: DfPredType) -> Self {
        let (discriminant, rest) = {
            match value {
                DfPredType::Constant(constant_type) => {
                    (DfPredTypeFlattened::Constant as u8, constant_type as u8)
                }
                DfPredType::UnOp(un_op_type) => (DfPredTypeFlattened::UnOp as u8, un_op_type as u8),
                DfPredType::BinOp(bin_op_type) => {
                    (DfPredTypeFlattened::BinOp as u8, bin_op_type as u8)
                }
                DfPredType::LogOp(log_op_type) => {
                    (DfPredTypeFlattened::LogOp as u8, log_op_type as u8)
                }
                DfPredType::SortOrder(sort_order_type) => {
                    (DfPredTypeFlattened::SortOrder as u8, sort_order_type as u8)
                }
                DfPredType::List => (DfPredTypeFlattened::List as u8, 0),
                DfPredType::ColumnRef => (DfPredTypeFlattened::ColumnRef as u8, 0),
                DfPredType::ExternColumnRef => (DfPredTypeFlattened::ExternColumnRef as u8, 0),
                DfPredType::Func => (DfPredTypeFlattened::Func as u8, 0),
                DfPredType::Between => (DfPredTypeFlattened::Between as u8, 0),
                DfPredType::Cast => (DfPredTypeFlattened::Cast as u8, 0),
                DfPredType::Like => (DfPredTypeFlattened::Like as u8, 0),
                DfPredType::DataType => (DfPredTypeFlattened::DataType as u8, 0),
                DfPredType::InList => (DfPredTypeFlattened::InList as u8, 0),
            }
        };
        SerializedPredTag(u16::from_be_bytes([discriminant, rest]))
    }
}

impl TryFrom<SerializedNodeTag> for DfNodeType {
    type Error = u16;

    fn try_from(value: SerializedNodeTag) -> Result<Self, Self::Error> {
        let SerializedNodeTag(v) = value;
        let [discriminant, rest] = v.to_be_bytes();
        let typ = {
            let flattened = DfNodeTypeFlattened::from_repr(discriminant).ok_or_else(|| v)?;
            match flattened {
                DfNodeTypeFlattened::Join => {
                    DfNodeType::Join(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeTypeFlattened::RawDepJoin => {
                    DfNodeType::RawDepJoin(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeTypeFlattened::DepJoin => {
                    DfNodeType::DepJoin(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeTypeFlattened::PhysicalHashJoin => {
                    DfNodeType::PhysicalHashJoin(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeTypeFlattened::PhysicalNestedLoopJoin => {
                    DfNodeType::PhysicalNestedLoopJoin(JoinType::from_repr(rest).ok_or_else(|| v)?)
                }
                DfNodeTypeFlattened::Projection => DfNodeType::Projection,
                DfNodeTypeFlattened::Filter => DfNodeType::Filter,
                DfNodeTypeFlattened::Scan => DfNodeType::Scan,
                DfNodeTypeFlattened::Sort => DfNodeType::Sort,
                DfNodeTypeFlattened::Agg => DfNodeType::Agg,
                DfNodeTypeFlattened::EmptyRelation => DfNodeType::EmptyRelation,
                DfNodeTypeFlattened::Limit => DfNodeType::Limit,
                DfNodeTypeFlattened::PhysicalProjection => DfNodeType::PhysicalProjection,
                DfNodeTypeFlattened::PhysicalFilter => DfNodeType::PhysicalFilter,
                DfNodeTypeFlattened::PhysicalScan => DfNodeType::PhysicalScan,
                DfNodeTypeFlattened::PhysicalSort => DfNodeType::PhysicalSort,
                DfNodeTypeFlattened::PhysicalAgg => DfNodeType::PhysicalAgg,
                DfNodeTypeFlattened::PhysicalEmptyRelation => DfNodeType::PhysicalEmptyRelation,
                DfNodeTypeFlattened::PhysicalLimit => DfNodeType::PhysicalLimit,
            }
        };
        Ok(typ)
    }
}

impl From<DfNodeType> for SerializedNodeTag {
    fn from(value: DfNodeType) -> Self {
        let (discriminant, rest) = match value {
            DfNodeType::Join(join_type) => (DfNodeTypeFlattened::Join as u8, join_type as u8),
            DfNodeType::RawDepJoin(join_type) => {
                (DfNodeTypeFlattened::RawDepJoin as u8, join_type as u8)
            }
            DfNodeType::DepJoin(join_type) => (DfNodeTypeFlattened::DepJoin as u8, join_type as u8),
            DfNodeType::PhysicalHashJoin(join_type) => {
                (DfNodeTypeFlattened::PhysicalHashJoin as u8, join_type as u8)
            }
            DfNodeType::PhysicalNestedLoopJoin(join_type) => (
                DfNodeTypeFlattened::PhysicalNestedLoopJoin as u8,
                join_type as u8,
            ),
            DfNodeType::Projection => (DfNodeTypeFlattened::Projection as u8, 0),
            DfNodeType::Filter => (DfNodeTypeFlattened::Filter as u8, 0),
            DfNodeType::Scan => (DfNodeTypeFlattened::Scan as u8, 0),
            DfNodeType::Sort => (DfNodeTypeFlattened::Sort as u8, 0),
            DfNodeType::Agg => (DfNodeTypeFlattened::Agg as u8, 0),
            DfNodeType::EmptyRelation => (DfNodeTypeFlattened::EmptyRelation as u8, 0),
            DfNodeType::Limit => (DfNodeTypeFlattened::Limit as u8, 0),
            DfNodeType::PhysicalProjection => (DfNodeTypeFlattened::PhysicalProjection as u8, 0),
            DfNodeType::PhysicalFilter => (DfNodeTypeFlattened::PhysicalFilter as u8, 0),
            DfNodeType::PhysicalScan => (DfNodeTypeFlattened::PhysicalScan as u8, 0),
            DfNodeType::PhysicalSort => (DfNodeTypeFlattened::PhysicalSort as u8, 0),
            DfNodeType::PhysicalAgg => (DfNodeTypeFlattened::PhysicalAgg as u8, 0),
            DfNodeType::PhysicalEmptyRelation => {
                (DfNodeTypeFlattened::PhysicalEmptyRelation as u8, 0)
            }
            DfNodeType::PhysicalLimit => (DfNodeTypeFlattened::PhysicalLimit as u8, 0),
        };
        SerializedNodeTag(u16::from_be_bytes([discriminant, rest]))
    }
}

#[cfg(test)]
impl DfNodeTypeFlattened {
    fn is_join(&self) -> bool {
        match self {
            DfNodeTypeFlattened::Join
            | DfNodeTypeFlattened::RawDepJoin
            | DfNodeTypeFlattened::DepJoin
            | DfNodeTypeFlattened::PhysicalHashJoin
            | DfNodeTypeFlattened::PhysicalNestedLoopJoin => true,
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
        for flattened in DfNodeTypeFlattened::iter() {
            let discriminant = flattened as u8;
            if flattened.is_join() {
                for join_type in JoinType::iter() {
                    valid_tags.push(SerializedNodeTag(u16::from_be_bytes([
                        discriminant,
                        join_type as u8,
                    ])));
                }
                invalid_tags.push(SerializedNodeTag(u16::from_be_bytes([
                    discriminant,
                    u8::MAX,
                ])));
            } else {
                valid_tags.push(SerializedNodeTag(u16::from_be_bytes([discriminant, 0])));
            }
        }
        invalid_tags.push(SerializedNodeTag(u16::from_be_bytes([
            DfNodeTypeFlattened::COUNT as u8,
            0,
        ])));

        invalid_tags.into_iter().for_each(|tag| {
            DfNodeType::try_from(tag).unwrap_err();
        });

        valid_tags.iter().for_each(|&tag| {
            let new_tag = SerializedNodeTag::from(DfNodeType::try_from(tag).unwrap());
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
        for flattened in DfPredTypeFlattened::iter() {
            let discriminant = flattened as u8;
            match flattened {
                DfPredTypeFlattened::Constant => {
                    for constant_type in ConstantType::iter() {
                        valid_tags.push(SerializedPredTag(u16::from_be_bytes([
                            discriminant,
                            constant_type as u8,
                        ])));
                    }
                    invalid_tags.push(SerializedPredTag(u16::from_be_bytes([
                        discriminant,
                        u8::MAX,
                    ])));
                }
                DfPredTypeFlattened::UnOp => {
                    for un_op_type in UnOpType::iter() {
                        valid_tags.push(SerializedPredTag(u16::from_be_bytes([
                            discriminant,
                            un_op_type as u8,
                        ])));
                    }
                    invalid_tags.push(SerializedPredTag(u16::from_be_bytes([
                        discriminant,
                        u8::MAX,
                    ])));
                }
                DfPredTypeFlattened::BinOp => {
                    for bin_op_type in BinOpType::iter() {
                        valid_tags.push(SerializedPredTag(u16::from_be_bytes([
                            discriminant,
                            bin_op_type as u8,
                        ])));
                    }
                    invalid_tags.push(SerializedPredTag(u16::from_be_bytes([
                        discriminant,
                        u8::MAX,
                    ])));
                }
                DfPredTypeFlattened::LogOp => {
                    for log_op_type in LogOpType::iter() {
                        valid_tags.push(SerializedPredTag(u16::from_be_bytes([
                            discriminant,
                            log_op_type as u8,
                        ])));
                    }
                    invalid_tags.push(SerializedPredTag(u16::from_be_bytes([
                        discriminant,
                        u8::MAX,
                    ])));
                }
                DfPredTypeFlattened::SortOrder => {
                    for sort_order_type in SortOrderType::iter() {
                        valid_tags.push(SerializedPredTag(u16::from_be_bytes([
                            discriminant,
                            sort_order_type as u8,
                        ])));
                    }
                    invalid_tags.push(SerializedPredTag(u16::from_be_bytes([
                        discriminant,
                        u8::MAX,
                    ])));
                }
                _ => valid_tags.push(SerializedPredTag(u16::from_be_bytes([discriminant, 0]))),
            }
        }
        invalid_tags.push(SerializedPredTag(u16::from_be_bytes([
            DfPredTypeFlattened::COUNT as u8,
            0,
        ])));

        invalid_tags.into_iter().for_each(|tag| {
            DfPredType::try_from(tag).unwrap_err();
        });

        valid_tags.iter().for_each(|&tag| {
            let new_tag = SerializedPredTag::from(DfPredType::try_from(tag).unwrap());
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
