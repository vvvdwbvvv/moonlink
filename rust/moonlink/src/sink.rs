use crate::table_handler::{TableEvent, TableEventSender, TableHandler};
use crate::util::table_schema_to_arrow_schema;
use async_trait::async_trait;
use pg_replicate::conversions::Cell;
use pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::{
        sinks::{BatchSink, InfallibleSinkError},
        PipelineResumptionState,
    },
    table::{TableId, TableSchema},
};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::types::PgLsn;

pub struct Sink {
    table_handlers: Arc<Mutex<HashMap<TableId, TableHandler>>>,
    event_senders: HashMap<TableId, TableEventSender>,
}

impl Sink {
    pub fn new() -> Self {
        let table_handlers: Arc<Mutex<HashMap<u32, TableHandler>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let event_senders: HashMap<TableId, TableEventSender> = HashMap::new();
        Self {
            table_handlers,
            event_senders,
        }
    }
}

#[async_trait]
impl BatchSink for Sink {
    type Error = InfallibleSinkError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        Ok(PipelineResumptionState {
            copied_tables: HashSet::new(),
            last_lsn: PgLsn::from(0),
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        let mut table_handlers = self.table_handlers.lock().await;
        for (table_id, table_schema) in table_schemas {
            let table_handler = TableHandler::new(
                table_schema_to_arrow_schema(&table_schema),
                table_schema.table_name.to_string(),
                table_id as u64,
                PathBuf::from("mooncake_test/"),
            );
            self.event_senders
                .insert(table_id, table_handler.get_event_sender());
            table_handlers.insert(table_id, table_handler);
        }

        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        let event_sender = self.event_senders.get_mut(&table_id).unwrap();
        for row in rows {
            if let Cell::I64(id) = row.values[0] {
                event_sender
                    .send(TableEvent::Append {
                        primary_key: id,
                        row,
                    })
                    .await
                    .unwrap();
            } else {
                eprintln!("Invalid primary key type: {:?}", row.values[0]);
            }
        }
        event_sender
            .send(TableEvent::Commit { lsn: 0 })
            .await
            .unwrap();
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        // TODO: support multiple tables in a transaction
        //
        let mut table_id_in_transaction: Option<u32> = None;
        let mut lsn_in_transaction: Option<u64> = None;

        for event in events {
            println!("Received CDC event: {:?}", event);
            match event {
                CdcEvent::Begin(begin_body) => {
                    lsn_in_transaction = Some(begin_body.final_lsn());
                }
                CdcEvent::Commit(commit_body) => {
                    if let Some(table_id) = table_id_in_transaction {
                        let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                        event_sender
                            .send(TableEvent::Commit {
                                lsn: commit_body.commit_lsn(),
                            })
                            .await
                            .unwrap();
                        table_id_in_transaction = None;
                    }
                }
                CdcEvent::Insert((table_id, table_row)) => {
                    if let Some(prev_id) = table_id_in_transaction {
                        assert!(
                            prev_id == table_id,
                            "Multiple tables in a transaction are not supported"
                        );
                    }
                    table_id_in_transaction = Some(table_id);
                    if let Cell::I64(id) = table_row.values[0] {
                        let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                        event_sender
                            .send(TableEvent::Append {
                                primary_key: (id),
                                row: (table_row),
                            })
                            .await
                            .unwrap();
                    } else {
                        println!("Invalid primary key type: {:?}", table_row.values[0]);
                    }
                }
                CdcEvent::Update((table_id, old_table_row, new_table_row)) => {
                    if let Some(prev_id) = table_id_in_transaction {
                        assert!(
                            prev_id == table_id,
                            "Multiple tables in a transaction are not supported"
                        );
                    }
                    table_id_in_transaction = Some(table_id);
                    if let Cell::I64(id) = old_table_row.as_ref().unwrap().values[0] {
                        let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                        event_sender
                            .send(TableEvent::Delete {
                                primary_key: (id),
                                lsn: (lsn_in_transaction.unwrap()),
                            })
                            .await
                            .unwrap();
                        event_sender
                            .send(TableEvent::Append {
                                primary_key: (id),
                                row: (new_table_row),
                            })
                            .await
                            .unwrap();
                    } else {
                        println!(
                            "Invalid primary key type: {:?}",
                            old_table_row.as_ref().unwrap().values[0]
                        );
                    }
                }
                CdcEvent::Delete((table_id, table_row)) => {
                    if let Some(prev_id) = table_id_in_transaction {
                        assert!(
                            prev_id == table_id,
                            "Multiple tables in a transaction are not supported"
                        );
                    }
                    table_id_in_transaction = Some(table_id);
                    if let Cell::I64(id) = table_row.values[0] {
                        let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                        event_sender
                            .send(TableEvent::Delete {
                                primary_key: (id),
                                lsn: (lsn_in_transaction.unwrap()),
                            })
                            .await
                            .unwrap();
                    } else {
                        println!("Invalid primary key type: {:?}", table_row.values[0]);
                    }
                }
                CdcEvent::Relation(relation_body) => println!("Relation {relation_body:?}"),
                CdcEvent::Type(type_body) => println!("Type {type_body:?}"),
                CdcEvent::KeepAliveRequested { .. } => {}
            }
        }
        Ok(PgLsn::from(0))
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        println!("table {table_id} copied");
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        println!("table {table_id} truncated");
        Ok(())
    }
}
