//! Mergeable sketches used by optimizer statistics.
//!
//! Sketches operate on canonical byte encodings rather than planner values. This keeps the
//! implementation independent of any query IR while making compatibility explicit at merge and
//! comparison boundaries.

mod ertl_joint;
mod hyperloglog;
mod space_saving;

pub use ertl_joint::{
    ERTL_MIN_JACCARD, JointCardinalityEstimate, JointEstimateQuality, estimate_joint,
};
pub use hyperloglog::{HyperLogLog, HyperLogLogError};
pub use space_saving::{FrequentItem, SpaceSaving, SpaceSavingError};
