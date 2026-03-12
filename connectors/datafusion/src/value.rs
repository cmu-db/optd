//! Converts between DataFusion ScalarValue and optd ScalarValue.

use crate::OptdResult;
use datafusion::scalar::ScalarValue as DFScalarValue;
use optd_core::{connector_err, ir::ScalarValue as OptdScalarValue};
