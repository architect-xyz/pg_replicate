use std::collections::HashSet;

use futures::StreamExt;
use tokio::pin;
use tokio_postgres::types::PgLsn;

use crate::{
    conversions::cdc_event::CdcEvent,
    pipeline::{
        batching::stream::BatchTimeoutStream,
        sinks::BatchSink,
        sources::{Source, SourceError},
        PipelineAction, PipelineError,
    },
    table::TableId,
};

use super::BatchConfig;

pub struct BatchDataPipeline<Src: Source, Snk: BatchSink> {
    source: Src,
    sink: Snk,
    action: PipelineAction,
    batch_config: BatchConfig,
}

impl<Src: Source, Snk: BatchSink> BatchDataPipeline<Src, Snk> {
    pub fn new(source: Src, sink: Snk, action: PipelineAction, batch_config: BatchConfig) -> Self {
        BatchDataPipeline {
            source,
            sink,
            action,
            batch_config,
        }
    }

    async fn copy_table_schemas(&mut self) -> Result<(), PipelineError> {
        let table_schemas = self.source.get_table_schemas();
        let table_schemas = table_schemas.clone();

        if !table_schemas.is_empty() {
            self.sink.write_table_schemas(table_schemas).await?;
        }

        Ok(())
    }

    async fn copy_tables(&mut self, copied_tables: &HashSet<TableId>) -> Result<(), PipelineError> {
        let table_schemas = self.source.get_table_schemas();

        for table_schema in table_schemas.values() {
            if copied_tables.contains(&table_schema.table_id) {
                continue;
            }

            self.sink.truncate_table(table_schema.table_id).await?;

            let table_rows = self
                .source
                .get_table_copy_stream(&table_schema.table_name, &table_schema.column_schemas)
                .await?;

            let batch_timeout_stream =
                BatchTimeoutStream::new(table_rows, self.batch_config.clone());

            pin!(batch_timeout_stream);

            while let Some(batch) = batch_timeout_stream.next().await {
                //TODO: Avoid a vec copy
                let mut rows = Vec::with_capacity(batch.len());
                for row in batch {
                    rows.push(row.map_err(SourceError::TableCopyStream)?);
                }
                self.sink
                    .write_table_rows(rows, table_schema.table_id)
                    .await?;
            }

            self.sink.table_copied(table_schema.table_id).await?;
        }
        self.source.commit_transaction().await?;

        Ok(())
    }

    async fn copy_cdc_events(&mut self, last_lsn: PgLsn) -> Result<(), PipelineError> {
        let mut last_lsn: u64 = last_lsn.into();
        last_lsn += 1;
        let cdc_events = self.source.get_cdc_stream(last_lsn.into()).await?;

        pin!(cdc_events);

        let batch_timeout_stream = BatchTimeoutStream::new(cdc_events, self.batch_config.clone());

        pin!(batch_timeout_stream);

        while let Some(batch) = batch_timeout_stream.next().await {
            // let cdc_event = cdc_event.map_err(SourceError::CdcStream)?;
            let mut _send_status_update = false;
            let mut events = Vec::with_capacity(batch.len());
            for event in batch {
                let event = event.map_err(SourceError::CdcStream)?;
                if let CdcEvent::KeepAliveRequested { reply } = event {
                    _send_status_update = reply;
                };
                events.push(event);
            }
            // let send_status_update = if let CdcEvent::KeepAliveRequested { reply } = cdc_event {
            //     reply
            // } else {
            //     false
            // };
            let _last_lsn = self.sink.write_cdc_events(events).await?;
            //TODO: send status update
            // if send_status_update {
            //     cdc_events
            //         .as_mut()
            //         .send_status_update_impl(last_lsn)
            //         .await
            //         .map_err(|e| PipelineError::SourceError(SourceError::StatusUpdate(e)))?;
            // }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), PipelineError> {
        let resumption_state = self.sink.get_resumption_state().await?;
        match self.action {
            PipelineAction::TableCopiesOnly => {
                self.copy_table_schemas().await?;
                self.copy_tables(&resumption_state.copied_tables).await?;
            }
            PipelineAction::CdcOnly => {
                self.copy_table_schemas().await?;
                self.copy_cdc_events(resumption_state.last_lsn).await?;
            }
            PipelineAction::Both => {
                self.copy_table_schemas().await?;
                self.copy_tables(&resumption_state.copied_tables).await?;
                self.copy_cdc_events(resumption_state.last_lsn).await?;
            }
        }

        Ok(())
    }
}