use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;

use crate::sql::sql_provider_datafusion::{
  get_stream, to_execution_error, Result as SqlResult
}