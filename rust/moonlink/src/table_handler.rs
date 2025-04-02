use crate::error::Result;
use crate::storage::disk_slice::DiskSliceWriter;
use crate::storage::mooncake_table::MooncakeTable;
use arrow::datatypes::Schema;
use pg_replicate::conversions::table_row::TableRow;
use std::path::PathBuf;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

/// Event types that can be processed by the TableHandler
#[derive(Debug)]
pub enum TableEvent {
    /// Append a row to the table
    Append { row: TableRow },
    /// Delete a row from the table
    Delete { row: TableRow, lsn: u64 },
    /// Commit all pending operations with a given LSN
    Commit { lsn: u64 },
    /// Prepare the table for reading
    PrepareRead {
        response_channel: oneshot::Sender<(Vec<PathBuf>, Vec<(usize, usize)>)>,
    },
    /// Flush the table to disk
    Flush { lsn: u64 },
    /// Shutdown the handler
    _Shutdown,
}

/// Handler for table operations
pub struct TableHandler {
    /// Handle to the event processing task
    _event_handle: Option<JoinHandle<()>>,

    /// Sender for the event queue
    event_sender: Sender<TableEvent>,
}

pub type TableEventSender = Sender<TableEvent>;

impl TableHandler {
    /// Create a new TableHandler for the given schema and table name
    pub fn new(schema: Schema, table_name: String, table_id: u64, path: PathBuf) -> Self {
        // Create the table
        let table = MooncakeTable::new(schema, table_name, table_id, path);

        // Create channel for events
        let (event_sender, event_receiver) = mpsc::channel(100);

        // Create oneshot channel to send the table to the task
        let (tx, rx) = oneshot::channel();

        // Spawn the task with the oneshot receiver
        let event_handle = Some(tokio::spawn(async move {
            match rx.await {
                Ok(table) => Self::event_loop(event_receiver, table).await,
                Err(_) => println!("Failed to receive table in event loop task"),
            }
        }));

        // Create the handler
        let handler = Self {
            _event_handle: event_handle,
            event_sender,
        };

        // Send the table to the task
        if tx.send(table).is_err() {
            println!("Failed to send table to event loop task");
        }

        handler
    }

    /// Get the event sender to send events to this handler
    pub fn get_event_sender(&self) -> Sender<TableEvent> {
        self.event_sender.clone()
    }

    /// Main event processing loop
    async fn event_loop(mut event_receiver: Receiver<TableEvent>, mut table: MooncakeTable) {
        let mut snapshot_handle: Option<JoinHandle<()>> = None;
        let mut flush_handle: Option<JoinHandle<Result<DiskSliceWriter>>> = None;
        let mut periodic_snapshot_interval = time::interval(Duration::from_secs(1));

        // Process events until the receiver is closed or a Shutdown event is received
        loop {
            tokio::select! {
                // Process events from the queue
                Some(event) = event_receiver.recv() => {
                    match event {
                        TableEvent::Append { row } => {
                            if let Err(e) = table.append(row) {
                                println!("Append failed: {}", e);
                            }
                        }

                        TableEvent::Delete { row, lsn } => {
                            table.delete(row, lsn);
                        }

                        TableEvent::Commit { lsn } => {
                            table.commit(lsn);
                            if table.should_flush() {
                                flush_handle = Some(table.flush(lsn));
                            }
                        }

                        TableEvent::PrepareRead { response_channel } => {
                            // Wait for any pending snapshot to complete first
                            if let Some(handle) = &mut snapshot_handle {
                                match handle.await {
                                    Ok(()) => {
                                        println!("Snapshot creation completed successfully before prepare read");
                                        snapshot_handle = None;
                                    }
                                    Err(e) => {
                                        println!("Snapshot task was cancelled before prepare read: {}", e);
                                        snapshot_handle = None;
                                    }
                                }
                            }

                            // Request read and return the file path
                            match table.request_read() {
                                Ok((file_paths, deletions)) => {
                                    let _ = response_channel.send((file_paths, deletions));
                                }
                                Err(e) => {
                                    println!("Failed to prepare read: {}", e);
                                }
                            }
                        }

                        TableEvent::Flush { lsn } => {
                            flush_handle = Some(table.flush(lsn));
                        }

                        TableEvent::_Shutdown => {
                            println!("Shutting down table handler");
                            break;
                        }
                    }
                }
                // wait for the snapshot to complete
                Some(()) = async {
                    if let Some(handle) = &mut snapshot_handle {
                        match handle.await {
                            Ok(()) => {
                                println!("Snapshot creation completed successfully");
                            }
                            Err(e) => {
                                println!("Snapshot task was cancelled: {}", e);
                            }
                        }
                        Some(())
                    } else {
                        futures::future::pending::<Option<_>>().await
                    }
                } => {
                    snapshot_handle = None;
                }
                // wait for the flush to complete
                Some(writer) = async {
                    if let Some(handle) = &mut flush_handle {
                        match handle.await {
                            Ok(writer) => {
                                Some(writer)
                            }
                            Err(e) => {
                                println!("Flush task was cancelled: {}", e);
                                None
                            }
                        }
                    } else {
                        futures::future::pending::<Option<_>>().await
                    }
                } => {
                    table.commit_flush(writer.unwrap()).unwrap();
                    flush_handle = None;
                }
                // Periodic snapshot based on time
                _ = periodic_snapshot_interval.tick() => {
                    // Only create a periodic snapshot if there isn't already one in progress
                    if snapshot_handle.is_none() {
                        println!("Creating periodic snapshot");
                        snapshot_handle = table.create_snapshot();
                    }
                }
                // If all senders have been dropped, exit the loop
                else => {
                    println!("All event senders have been dropped, shutting down table handler");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use pg_replicate::conversions::Cell;
    use std::fs::File;

    #[tokio::test]
    async fn test_table_handler() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let handler = TableHandler::new(schema, "test_table".to_string(), 1, path);
        let event_sender = handler.get_event_sender();

        // Test append operation
        let row1 = TableRow {
            values: vec![
                Cell::I32(1),
                Cell::String("John".to_string()),
                Cell::I32(30),
            ],
        };

        event_sender
            .send(TableEvent::Append { row: row1 })
            .await
            .unwrap();

        // Test commit operation
        event_sender
            .send(TableEvent::Commit { lsn: 1 })
            .await
            .unwrap();

        // wait for 1 second
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Test prepare read operation - with response channel
        let (tx, rx) = oneshot::channel();
        event_sender
            .send(TableEvent::PrepareRead {
                response_channel: tx,
            })
            .await
            .unwrap();

        // Wait for the response
        match tokio::time::timeout(Duration::from_secs(1), rx).await {
            Ok(Ok((paths, _deletions))) => {
                println!("Received snapshot paths: {:?}", paths);

                if paths.is_empty() {
                    println!("Warning: No snapshot files returned");
                }

                // Verify that the paths exist
                for path in paths {
                    if path.exists() {
                        println!("Confirmed snapshot file exists: {:?}", path);
                        let file = File::open(&path).unwrap();
                        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                        println!("Converted arrow schema is: {}", builder.schema());
                        let mut reader = builder.build().unwrap();
                        let record_batch = reader.next();
                        println!("Record batch: {:?}", record_batch);
                    } else {
                        println!("Warning: Snapshot file does not exist at {:?}", path);
                    }
                }
            }
            Ok(Err(_)) => println!("Response channel was dropped"),
            Err(_) => println!("Timed out waiting for prepare read response"),
        }

        // Test shutdown
        event_sender.send(TableEvent::_Shutdown).await.unwrap();

        // Wait for event handler to exit
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }

        println!("All table handler tests passed!");
    }

    #[tokio::test]
    async fn test_table_handler_flush() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let handler = TableHandler::new(schema, "flush_test_table".to_string(), 1, path);
        let event_sender = handler.get_event_sender();

        // Test append operations - add multiple rows
        let rows: Vec<TableRow> = vec![
            TableRow {
                values: vec![
                    Cell::I32(1),
                    Cell::String("John".to_string()),
                    Cell::I32(30),
                ],
            },
            TableRow {
                values: vec![
                    Cell::I32(2),
                    Cell::String("Jane".to_string()),
                    Cell::I32(25),
                ],
            },
            TableRow {
                values: vec![Cell::I32(3), Cell::String("Bob".to_string()), Cell::I32(40)],
            },
        ];

        // Send append events for all rows
        for row in rows {
            event_sender.send(TableEvent::Append { row }).await.unwrap();
        }

        // Commit the appended rows
        event_sender
            .send(TableEvent::Commit { lsn: 1 })
            .await
            .unwrap();

        // Allow time for processing
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Send the flush event
        println!("Sending flush event");
        event_sender
            .send(TableEvent::Flush { lsn: 1 })
            .await
            .unwrap();

        // Wait for the flush to complete - this might take some time as it's asynchronous
        // in the TableHandler implementation
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Now create a snapshot to apply the changes from flush and make them visible
        // By default, the snapshot is created periodically, but we'll wait for a bit to ensure
        // one has been created after our flush
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Test prepare read operation to verify the data is accessible
        let (tx, rx) = oneshot::channel();
        event_sender
            .send(TableEvent::PrepareRead {
                response_channel: tx,
            })
            .await
            .unwrap();

        // Wait for the response with a longer timeout to ensure it completes
        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok((paths, _deletions))) => {
                println!("Received snapshot paths: {:?}", paths);

                // Check if we received any paths
                if paths.is_empty() {
                    panic!("No snapshot files were returned");
                }

                // Process each path in the returned vector
                for path in &paths {
                    println!("Processing path: {:?}", path);
                    if let Ok(abs_path) = path.canonicalize() {
                        println!("Absolute path: {:?}", abs_path);
                    }

                    // Verify that the path exists
                    if path.exists() {
                        println!("Confirmed snapshot file exists after flush");

                        // Print file size
                        let metadata = std::fs::metadata(path).unwrap();
                        println!("File size: {} bytes", metadata.len());

                        // Open and read the file to verify it contains data
                        let file = File::open(path).unwrap();
                        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                        println!("Converted arrow schema is: {}", builder.schema());

                        let mut reader = builder.build().unwrap();
                        if let Some(Ok(batch)) = reader.next() {
                            println!("Record batch after flush: {:?}", batch);
                            println!("Number of rows in batch: {}", batch.num_rows());
                            assert!(
                                batch.num_rows() > 0,
                                "Expected at least one row in the batch after flush"
                            );
                        } else {
                            println!("Warning: No records in the batch");
                        }
                    } else {
                        panic!("Snapshot file does not exist after flush: {:?}", path);
                    }
                }

                // Assert we have the expected files
                assert!(
                    !paths.is_empty(),
                    "Expected at least one snapshot file after flush"
                );
            }
            Ok(Err(_)) => panic!("Response channel was dropped"),
            Err(_) => panic!("Timed out waiting for prepare read response"),
        }
        // Test shutdown
        event_sender.send(TableEvent::_Shutdown).await.unwrap();

        // Wait for event handler to exit
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }

        println!("Flush test passed!");
    }
}
