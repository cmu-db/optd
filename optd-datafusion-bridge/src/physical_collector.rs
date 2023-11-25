use std::{
    sync::Arc,
    task::{Context, Poll},
};

use arrow_schema::SchemaRef;
use datafusion::{
    arrow::record_batch::RecordBatch,
    error::{DataFusionError, Result},
    execution::TaskContext,
    physical_plan::{
        internal_err, DisplayAs, DisplayFormatType, ExecutionPlan, RecordBatchStream,
        SendableRecordBatchStream,
    },
};
use futures_lite::Stream;
use futures_util::stream::StreamExt;
use optd_core::cascades::GroupId;
use optd_datafusion_repr::cost::RuntimeAdaptionStorage;

pub struct CollectorExec {
    group_id: GroupId,
    input: Arc<dyn ExecutionPlan>,
    collect_into: RuntimeAdaptionStorage,
}

impl std::fmt::Debug for CollectorExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CollectorExec")
    }
}

impl DisplayAs for CollectorExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CollectorExec group_id={}", self.group_id)
    }
}

impl CollectorExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        group_id: GroupId,
        collect_into: RuntimeAdaptionStorage,
    ) -> Self {
        Self {
            group_id,
            input,
            collect_into,
        }
    }
}

impl ExecutionPlan for CollectorExec {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        self.input.output_partitioning()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.group_id,
            self.collect_into.clone(),
        )))
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        self.input.statistics()
    }

    /// Execute one partition and return an iterator over RecordBatch
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return internal_err!("CollectorExec invalid partition {partition}");
        }

        Ok(Box::pin(CollectorReader {
            input: self.input.execute(partition, context)?,
            group_id: self.group_id,
            collect_into: self.collect_into.clone(),
            row_cnt: 0,
            done: false,
        }))
    }
}

struct CollectorReader {
    input: SendableRecordBatchStream,
    group_id: GroupId,
    done: bool,
    row_cnt: usize,
    collect_into: RuntimeAdaptionStorage,
}

impl Stream for CollectorReader {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        let poll = self.input.poll_next_unpin(cx);

        match poll {
            Poll::Ready(Some(Ok(batch))) => {
                self.row_cnt += batch.num_rows();
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(None) => {
                self.done = true;
                {
                    let mut guard = self.collect_into.lock().unwrap();
                    let iter_cnt = guard.iter_cnt;
                    guard
                        .history
                        .insert(self.group_id, (self.row_cnt, iter_cnt));
                }
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

impl RecordBatchStream for CollectorReader {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}
