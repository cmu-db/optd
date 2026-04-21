use std::sync::Arc;

use crate::{
    error::Result,
    ir::{
        ColumnSet, IRContext, Operator, OperatorKind, Scalar,
        operator::{Join, JoinType},
    },
};

#[derive(Debug, Clone)]
pub(crate) struct JoinIsland {
    root: JoinIslandNode,
    leaf_count: usize,
    join_count: usize,
}

impl JoinIsland {
    pub(crate) fn extract(root: Arc<Operator>, ctx: &IRContext) -> Result<Option<Self>> {
        if !is_logical_join(root.as_ref()) {
            return Ok(None);
        }

        let mut leaf_count = 0;
        let mut join_count = 0;
        let root = extract_node(root, ctx, &mut leaf_count, &mut join_count)?;
        Ok(Some(Self {
            root,
            leaf_count,
            join_count,
        }))
    }

    pub(crate) fn root(&self) -> &JoinIslandNode {
        &self.root
    }

    pub(crate) fn leaf_count(&self) -> usize {
        self.leaf_count
    }

    pub(crate) fn join_count(&self) -> usize {
        self.join_count
    }
}

#[derive(Debug, Clone)]
pub(crate) enum JoinIslandNode {
    Leaf(JoinIslandLeaf),
    Join {
        atom: JoinAtom,
        outer: Box<JoinIslandNode>,
        inner: Box<JoinIslandNode>,
    },
}

impl JoinIslandNode {
    pub(crate) fn leaf_count(&self) -> usize {
        match self {
            Self::Leaf(_) => 1,
            Self::Join { outer, inner, .. } => outer.leaf_count() + inner.leaf_count(),
        }
    }

    pub(crate) fn join_count(&self) -> usize {
        match self {
            Self::Leaf(_) => 0,
            Self::Join { outer, inner, .. } => 1 + outer.join_count() + inner.join_count(),
        }
    }

    pub(crate) fn atom(&self) -> Option<&JoinAtom> {
        match self {
            Self::Leaf(_) => None,
            Self::Join { atom, .. } => Some(atom),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct JoinIslandLeaf {
    pub(crate) op: Arc<Operator>,
    pub(crate) output_columns: Arc<ColumnSet>,
}

#[derive(Debug, Clone)]
pub(crate) struct JoinAtom {
    pub(crate) join_type: JoinType,
    pub(crate) join_cond: Arc<Scalar>,
    pub(crate) semantics: JoinSemantics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JoinSemantics {
    pub(crate) commutative: bool,
    pub(crate) associative: bool,
    pub(crate) preserves_left_rows: bool,
    pub(crate) preserves_right_rows: bool,
    pub(crate) outputs_left_only: bool,
    pub(crate) outputs_right_only: bool,
    pub(crate) introduces_mark_column: bool,
}

impl JoinSemantics {
    pub(crate) fn from_join_type(join_type: JoinType) -> Self {
        match join_type {
            JoinType::Inner => Self {
                commutative: true,
                associative: true,
                preserves_left_rows: false,
                preserves_right_rows: false,
                outputs_left_only: false,
                outputs_right_only: false,
                introduces_mark_column: false,
            },
            JoinType::LeftOuter | JoinType::Single => Self {
                commutative: false,
                associative: false,
                preserves_left_rows: true,
                preserves_right_rows: false,
                outputs_left_only: false,
                outputs_right_only: false,
                introduces_mark_column: false,
            },
            JoinType::LeftSemi | JoinType::LeftAnti => Self {
                commutative: false,
                associative: false,
                preserves_left_rows: true,
                preserves_right_rows: false,
                outputs_left_only: true,
                outputs_right_only: false,
                introduces_mark_column: false,
            },
            JoinType::Mark(_) => Self {
                commutative: false,
                associative: false,
                preserves_left_rows: true,
                preserves_right_rows: false,
                outputs_left_only: false,
                outputs_right_only: false,
                introduces_mark_column: true,
            },
        }
    }
}

pub(crate) fn is_logical_join(op: &Operator) -> bool {
    matches!(&op.kind, OperatorKind::Join(meta) if meta.implementation.is_none())
}

pub(crate) fn is_inner_logical_join(op: &Operator) -> bool {
    matches!(&op.kind, OperatorKind::Join(meta) if meta.implementation.is_none() && meta.join_type == JoinType::Inner)
}

fn extract_node(
    op: Arc<Operator>,
    ctx: &IRContext,
    leaf_count: &mut usize,
    join_count: &mut usize,
) -> Result<JoinIslandNode> {
    if let OperatorKind::Join(meta) = &op.kind
        && meta.implementation.is_none()
    {
        *join_count += 1;
        let join = Join::borrow_raw_parts(meta, &op.common);
        return Ok(JoinIslandNode::Join {
            atom: JoinAtom {
                join_type: *join.join_type(),
                join_cond: join.join_cond().clone(),
                semantics: JoinSemantics::from_join_type(*join.join_type()),
            },
            outer: Box::new(extract_node(join.outer().clone(), ctx, leaf_count, join_count)?),
            inner: Box::new(extract_node(join.inner().clone(), ctx, leaf_count, join_count)?),
        });
    }

    *leaf_count += 1;
    Ok(JoinIslandNode::Leaf(JoinIslandLeaf {
        output_columns: op.output_columns(ctx)?,
        op,
    }))
}
