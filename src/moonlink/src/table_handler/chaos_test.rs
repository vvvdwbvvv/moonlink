/// This file implements chaos test for table handler.
///
/// System invariants:
/// - Begin events happen only after end events
/// - End events happen only after begin events
/// - Rows to delete comes from committed appended ones
/// - LSN always increases
use crate::event_sync::create_table_event_syncer;
use crate::row::{MoonlinkRow, RowValue};
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::gcs_test_utils::*;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::test_guard::TestGuard as GcsTestGuard;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::s3_test_utils::*;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::test_guard::TestGuard as S3TestGuard;
use crate::storage::mooncake_table::replay::replay_events::MooncakeTableEvent;
use crate::storage::mooncake_table::table_event_manager::TableEventManager;
use crate::storage::mooncake_table::{table_creation_test_utils::*, TableMetadata};
use crate::table_handler::chaos_replay::replay;
use crate::table_handler::chaos_table_metadata::ReplayTableMetadata;
use crate::table_handler::test_utils::*;
use crate::table_handler::{TableEvent, TableHandler};
use crate::table_handler_timer::create_table_handler_timers;
use crate::union_read::ReadStateManager;
use crate::VisibilityLsn;
use crate::{IcebergTableConfig, ObjectStorageCache, ObjectStorageCacheConfig, StorageConfig};

use function_name::named;
use more_asserts as ma;
use pico_args::Arguments;
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::{tempdir, TempDir};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::sync::watch;

/// To avoid excessive and continuous table maintenance operations, set an interval between each invocation for each non table update operation.
const NON_UPDATE_COMMAND_INTERVAL_LSN: u64 = 5;

/// Parsed chaos test arguments for convenience
#[derive(Clone)]
struct ChaosTestArgs {
    seed: u64,
    print_events_on_success: bool,
}

/// Combine argument parsing logic into one function using pico-args.
/// Since cargo test filters unknown flags, we use a different approach:
/// - Try to parse from command line args first (for direct binary execution)
/// - Fall back to timestamp-based seed
/// - For print events, we'll use a simple flag that might work
fn parse_chaos_test_args() -> ChaosTestArgs {
    let mut pargs = Arguments::from_env();

    // Try to parse command line arguments (works when running binary directly)
    let seed: Option<u64> = pargs.opt_value_from_str("--seed").unwrap_or(None);
    let print_events_on_success = pargs.contains("--print-events-on-success");

    // Default seed if not provided
    let seed = seed.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    });

    ChaosTestArgs {
        seed,
        print_events_on_success,
    }
}

/// Create a test moonlink row.
fn create_row(id: i32, name: &str, age: i32) -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(id),
        RowValue::ByteArray(name.as_bytes().to_vec()),
        RowValue::Int32(age),
    ])
}

/// Events randomly selected for chaos test.
#[derive(Debug)]
struct ChaosEvent {
    table_events: Vec<TableEvent>,
    table_maintenance_event: Option<TableEvent>,
    snapshot_read_lsn: Option<u64>,
    force_snapshot_lsn: Option<u64>,
}

impl ChaosEvent {
    fn create_table_events(table_events: Vec<TableEvent>) -> Self {
        Self {
            table_events,
            table_maintenance_event: None,
            snapshot_read_lsn: None,
            force_snapshot_lsn: None,
        }
    }
    fn create_table_maintenance_event(table_event: TableEvent) -> Self {
        Self {
            table_events: vec![],
            table_maintenance_event: Some(table_event),
            snapshot_read_lsn: None,
            force_snapshot_lsn: None,
        }
    }
    fn create_snapshot_read(lsn: u64) -> Self {
        Self {
            table_events: vec![],
            table_maintenance_event: None,
            snapshot_read_lsn: Some(lsn),
            force_snapshot_lsn: None,
        }
    }
    fn create_force_snapshot(lsn: u64) -> Self {
        Self {
            table_events: vec![],
            table_maintenance_event: None,
            snapshot_read_lsn: None,
            force_snapshot_lsn: Some(lsn),
        }
    }
}

#[derive(Default)]
struct NonTableUpdateCmdCall {
    /// LSN (value of [`cur_lsn`]) for the last read snapshot invocation.
    read_snapshot_lsn: u64,
    /// LSN (value of [`cur_lsn`]) for the last force snapshot invocation.
    force_snapshot_lsn: u64,
    /// LSN (value of [`cur_lsn`]) for the last force index merge invocation.
    force_index_merge_lsn: u64,
    /// LSN (value of [`cur_lsn`]) for the last force data compaction invocation.
    force_data_compaction_lsn: u64,
}

#[derive(Debug, Clone)]
enum EventKind {
    BeginStreamingTxn,
    BeginNonStreamingTxn,
    Append,
    Delete,
    Update,
    StreamAbort,
    StreamFlush,
    EndWithFlush,
    EndNoFlush,
    ReadSnapshot,
    /// Foreground force snapshot only happens after commit operation, otherwise it gets blocked.
    ForegroundForceSnapshot,
    /// Foreground force table maintenance only happens after commit operation, otherwise it gets blocked.
    ForegroundForceIndexMerge,
    ForegroundForceDataCompaction,
}

#[derive(Clone, Debug, PartialEq)]
enum TxnState {
    /// No active transaction ongoing.
    Empty,
    /// Within a non-streaming transaction.
    InNonStreaming,
    /// Within a streaming transaction.
    InStreaming,
}

struct ChaosState {
    /// Random seed used to generate random events.
    random_seed: u64,
    /// Used to generate random events, with current timestamp as random seed.
    rng: StdRng,
    /// Whether to enable delete operations.
    append_only: bool,
    /// Whether to test upsert/ delete if exists.
    is_upsert_table: bool,
    /// Non table update operation invocation status.
    non_table_update_cmd_call: NonTableUpdateCmdCall,
    /// Used to generate rows to insert.
    next_id: i32,
    /// Inserted rows in committed transactions.
    committed_inserted_rows: VecDeque<(i32 /*id*/, MoonlinkRow)>,
    /// Inserted rows in the current uncommitted transaction.
    uncommitted_inserted_rows: VecDeque<(i32 /*id*/, MoonlinkRow)>,
    /// Updated rows in the current uncommitted transaction.
    /// The row being deleted and appended is of the same content.
    uncommitted_updated_rows: HashMap<i32 /*id*/, MoonlinkRow>,
    /// Deleted committed row ids in the current uncommitted transaction.
    deleted_committed_row_ids: HashSet<i32 /*id*/>,
    /// Deleted uncommitted row ids in the current uncommitted transaction.
    /// Notice: only stream transactions are able to delete uncommitted rows.
    deleted_uncommitted_row_ids: HashSet<i32 /*id*/>,
    /// Used to indicate whether there's an ongoing transaction.
    txn_state: TxnState,
    /// LSN to use for the next operation, including update operations and commits.
    cur_lsn: u64,
    /// Txn id used for streaming transaction.
    cur_xact_id: u32,
    /// Used to read snapshot.
    read_state_manager: ReadStateManager,
    /// Last commit LSN.
    last_commit_lsn: Option<u64>,
    /// Whether the last finished transaction committed successfully, or not.
    last_txn_is_committed: bool,
}

impl ChaosState {
    fn new(
        read_state_manager: ReadStateManager,
        random_seed: u64,
        append_only: bool,
        upsert_delete_if_exists: bool,
    ) -> Self {
        let rng = StdRng::seed_from_u64(random_seed);
        Self {
            random_seed,
            rng,
            append_only,
            is_upsert_table: upsert_delete_if_exists,
            non_table_update_cmd_call: NonTableUpdateCmdCall::default(),
            txn_state: TxnState::Empty,
            next_id: 0,
            committed_inserted_rows: VecDeque::new(),
            uncommitted_inserted_rows: VecDeque::new(),
            deleted_committed_row_ids: HashSet::new(),
            deleted_uncommitted_row_ids: HashSet::new(),
            uncommitted_updated_rows: HashMap::new(),
            read_state_manager,
            cur_lsn: 0,
            cur_xact_id: 0,
            last_commit_lsn: None,
            last_txn_is_committed: false,
        }
    }

    /// Get the current LSN to use for the current operation, and increment.
    fn get_and_update_cur_lsn(&mut self) -> u64 {
        let cur_lsn = self.cur_lsn;
        self.cur_lsn += 1;
        cur_lsn
    }

    /// Clear all buffered rows for the current transaction.
    fn clear_cur_transaction_buffered_rows(&mut self) {
        self.uncommitted_inserted_rows.clear();
        self.uncommitted_updated_rows.clear();
        self.deleted_committed_row_ids.clear();
        self.deleted_uncommitted_row_ids.clear();
    }

    /// Assert on preconditions to start a new transaction, whether it's streaming one or non-streaming one.
    fn assert_txn_begin_precondition(&self) {
        assert_eq!(self.txn_state, TxnState::Empty);
        assert!(self.uncommitted_inserted_rows.is_empty());
        assert!(self.uncommitted_updated_rows.is_empty());
        assert!(self.deleted_committed_row_ids.is_empty());
        assert!(self.deleted_uncommitted_row_ids.is_empty());
    }

    /// Start a streaming transaction, and return xact id to use for current transaction.
    fn begin_streaming_txn(&mut self) {
        self.assert_txn_begin_precondition();
        self.txn_state = TxnState::InStreaming;
    }

    fn begin_non_streaming_txn(&mut self) {
        self.assert_txn_begin_precondition();
        self.txn_state = TxnState::InNonStreaming;
    }

    /// Abort the current stream transaction.
    fn stream_abort_transaction(&mut self) {
        assert_eq!(self.txn_state, TxnState::InStreaming);
        self.txn_state = TxnState::Empty;
        self.cur_xact_id += 1;
        self.last_txn_is_committed = false;
        self.clear_cur_transaction_buffered_rows();
    }

    fn commit_transaction(&mut self, lsn: u64) {
        // Update transaction id if streaming one.
        if self.txn_state == TxnState::InStreaming {
            self.cur_xact_id += 1;
        }

        // Set chaos test states.
        assert_ne!(self.txn_state, TxnState::Empty);
        self.txn_state = TxnState::Empty;
        self.last_commit_lsn = Some(lsn);
        self.last_txn_is_committed = true;

        // Set table states.
        self.committed_inserted_rows
            .extend(self.uncommitted_inserted_rows.drain(..));
        self.committed_inserted_rows.retain(|(id, _)| {
            !self.deleted_committed_row_ids.contains(id)
                && !self.deleted_uncommitted_row_ids.contains(id)
        });

        self.clear_cur_transaction_buffered_rows();
    }

    /// Get transaction id to set for both streaming and non-streaming transactions.
    fn get_cur_xact_id(&self) -> Option<u32> {
        if self.txn_state == TxnState::InStreaming {
            Some(self.cur_xact_id)
        } else {
            None
        }
    }

    fn get_next_row_to_append(&mut self) -> MoonlinkRow {
        let row = create_row(self.next_id, /*name=*/ "user", self.next_id % 5);
        self.uncommitted_inserted_rows
            .push_back((self.next_id, row.clone()));
        self.next_id += 1;
        row
    }

    /// Return all [`id`] fields for the moonlink rows which haven't been deleted in the alphabetical order.
    fn get_valid_ids(&self) -> Vec<i32> {
        self.committed_inserted_rows
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>()
    }

    /// Util function to decide whether there's any updated rows, which are not deleted in the current transaction.
    fn has_updated_undeleted_row(&self) -> bool {
        for (id, _) in self.uncommitted_updated_rows.iter() {
            if !self.deleted_uncommitted_row_ids.contains(id)
                && !self.deleted_committed_row_ids.contains(id)
            {
                return true;
            }
        }
        false
    }

    /// Util function to decide whether the given id indicates a row that has been committed or not.
    fn is_committed_row(&self, id: i32) -> bool {
        // Uncommitted rows have much less records, so search on uncommitted records instead of committed ones.
        !self
            .uncommitted_inserted_rows
            .iter()
            .any(|(cur_id, _)| *cur_id == id)
    }

    fn can_append(&self) -> bool {
        if self.is_upsert_table {
            return false;
        }
        true
    }

    /// Return whether we could delete a row in the next event.
    ///
    /// The logic corresponds to [`get_random_row_to_delete`].
    fn can_delete(&self) -> bool {
        if self.append_only {
            return false;
        }

        let uncommitted_inserted_rows = self.uncommitted_inserted_rows.len();
        let committed_inserted_rows = self.committed_inserted_rows.len();
        let uncommitted_updated_rows = self.uncommitted_updated_rows.len();
        let deleted_committed_rows = self.deleted_committed_row_ids.len();
        let deleted_uncommitted_rows = self.deleted_uncommitted_row_ids.len();

        // There're undeleted committed records, which are not deleted or updated in the current transaction.
        if committed_inserted_rows > deleted_committed_rows + uncommitted_updated_rows {
            return true;
        }

        // Streaming transactions are allowed to update and deleted rows updated or appended in the current transaction.
        if self.txn_state == TxnState::InStreaming {
            // Streaming transactions are allowed to delete rows inserted in the current transaction.
            if uncommitted_inserted_rows != deleted_uncommitted_rows {
                ma::assert_lt!(
                    self.deleted_uncommitted_row_ids.len(),
                    self.uncommitted_inserted_rows.len()
                );
                return true;
            }

            // Streaming transactions are allowed to delete updated rows in the current transaction.
            if self.has_updated_undeleted_row() {
                return true;
            }
        }

        false
    }

    /// Return whether we could update a row in the next event.
    ///
    /// The logic corresponds to [`get_random_row_to_update`].
    fn can_update(&self) -> bool {
        if self.append_only {
            return false;
        }

        let committed_inserted_rows = self.committed_inserted_rows.len();
        let uncommitted_updated_rows = self.uncommitted_updated_rows.len();
        let deleted_committed_rows = self.deleted_committed_row_ids.len();
        let deleted_uncommitted_rows = self.deleted_uncommitted_row_ids.len();

        if committed_inserted_rows
            > (uncommitted_updated_rows + deleted_committed_rows + deleted_uncommitted_rows)
        {
            return true;
        }

        if self.txn_state == TxnState::InStreaming {
            // Streaming transaction allows to update an already updated row.
            if self.has_updated_undeleted_row() {
                return true;
            }

            // Streaming transaction allows to update an uncommitted row.
            if self.uncommitted_inserted_rows.len() > self.deleted_uncommitted_row_ids.len() {
                return true;
            }
        }

        false
    }

    /// Get a random row to delete.
    fn get_random_row_to_delete(&mut self) -> MoonlinkRow {
        // Delete if exists is only supported for non-streaming transaction.
        if self.is_upsert_table
            && self.get_cur_xact_id().is_none()
            && self.rng.random_range(0..100) < 50
        {
            // Delete a none existing row.
            let row = create_row(self.next_id, /*name=*/ "user", self.next_id % 5);
            self.next_id += 1;
            return row;
        }

        let mut candidates: Vec<(i32, MoonlinkRow, bool /*committed*/)> = self
            .committed_inserted_rows
            .iter()
            .filter(|(id, _)| {
                !self.deleted_committed_row_ids.contains(id)
                    && !self.uncommitted_updated_rows.contains_key(id)
            })
            .map(|(id, row)| (*id, row.clone(), /*committed=*/ true))
            .collect();

        // If within a streaming transaction, could also delete from uncommitted inserted rows.
        if self.txn_state == TxnState::InStreaming {
            // Allow to delete new rows appended in the current txn.
            candidates.extend(
                self.uncommitted_inserted_rows
                    .iter()
                    .filter(|(id, _)| !self.deleted_uncommitted_row_ids.contains(id))
                    .map(|(id, row)| (*id, row.clone(), /*committed=*/ false)),
            );
            // Allow to delete rows updated in the current txn.
            candidates.extend(
                self.uncommitted_updated_rows
                    .iter()
                    .filter(|(id, _)| {
                        !self.deleted_uncommitted_row_ids.contains(id)
                            && !self.deleted_committed_row_ids.contains(id)
                    })
                    .map(|(id, row)| {
                        (
                            *id,
                            row.clone(),
                            /*committed=*/ self.is_committed_row(*id),
                        )
                    }),
            );
        }
        assert!(!candidates.is_empty());

        // Randomly pick one row from the candidates.
        let random_idx = self.rng.random_range(0..candidates.len());
        let (id, row, is_committed) = candidates[random_idx].clone();

        // Update deleted rows set.
        if is_committed {
            assert!(self.deleted_committed_row_ids.insert(id));
        } else {
            assert!(self.deleted_uncommitted_row_ids.insert(id));
        }

        row
    }

    /// Get a random row to update.
    fn get_random_row_to_update(&mut self) -> MoonlinkRow {
        if self.is_upsert_table && self.rng.random_range(0..100) < 50 {
            // upsert a new row.
            let row = create_row(self.next_id, /*name=*/ "user", self.next_id % 5);
            self.uncommitted_inserted_rows
                .push_back((self.next_id, row.clone()));
            self.next_id += 1;
            return row;
        }

        let mut candidates: Vec<(i32, MoonlinkRow)> = self
            .committed_inserted_rows
            .iter()
            .filter(|(id, _)| {
                !self.uncommitted_updated_rows.contains_key(id)
                    && !self.deleted_committed_row_ids.contains(id)
                    && !self.deleted_uncommitted_row_ids.contains(id)
            })
            .map(|(id, row)| (*id, row.clone()))
            .collect();

        if self.txn_state == TxnState::InStreaming {
            // Streaming transaction could update from uncommitted updated rows, as long as it's not deleted in the current transaction.
            candidates.extend(
                self.uncommitted_updated_rows
                    .iter()
                    .filter(|(id, _)| {
                        !self.deleted_uncommitted_row_ids.contains(id)
                            && !self.deleted_committed_row_ids.contains(id)
                    })
                    .map(|(id, row)| (*id, row.clone())),
            );

            // Streaming transactuon allows to update uncommitted undeleted rows.
            candidates.extend(
                self.uncommitted_inserted_rows
                    .iter()
                    .filter(|(id, _)| {
                        !self.deleted_uncommitted_row_ids.contains(id)
                        // Uncommitted updated rows have been included above.
                            && !self.uncommitted_updated_rows.contains_key(id)
                    })
                    .map(|(id, row)| (*id, row.clone())),
            );
        }
        assert!(!candidates.is_empty());

        // Randomly pick one row from the candidates.
        let random_idx = self.rng.random_range(0..candidates.len());
        let (id, row) = candidates[random_idx].clone();

        // Update update rows set.
        let old_entry = self.uncommitted_updated_rows.insert(id, row.clone());
        // For non-streaming transaction doesn't allow repeatedly update one row.
        if self.txn_state == TxnState::InNonStreaming {
            assert!(old_entry.is_none());
        }
        // It's ok for streaming transaction to repeated update the same row.

        row
    }

    /// Attempt to push non table update operations to choices.
    fn try_push_read_snapshot_cmd(&mut self, choices: &mut Vec<EventKind>) {
        if self.last_commit_lsn.is_some()
            && self.cur_lsn - self.non_table_update_cmd_call.read_snapshot_lsn
                >= NON_UPDATE_COMMAND_INTERVAL_LSN
        {
            choices.push(EventKind::ReadSnapshot);
            self.non_table_update_cmd_call.read_snapshot_lsn = self.cur_lsn;
        }
    }
    fn try_push_table_maintenance_cmd(&mut self, choices: &mut Vec<EventKind>) {
        if self.last_commit_lsn.is_none() {
            return;
        }

        // Foreground table maintenance operations happen after a successfully committed transaction.
        if self.uncommitted_inserted_rows.is_empty()
            && self.uncommitted_inserted_rows.is_empty()
            && self.last_txn_is_committed
        {
            if self.cur_lsn - self.non_table_update_cmd_call.force_snapshot_lsn
                >= NON_UPDATE_COMMAND_INTERVAL_LSN
            {
                choices.push(EventKind::ForegroundForceSnapshot);
                self.non_table_update_cmd_call.force_snapshot_lsn = self.cur_lsn;
            }
            if self.cur_lsn - self.non_table_update_cmd_call.force_index_merge_lsn
                >= NON_UPDATE_COMMAND_INTERVAL_LSN
            {
                choices.push(EventKind::ForegroundForceIndexMerge);
                self.non_table_update_cmd_call.force_index_merge_lsn = self.cur_lsn;
            }
            if self.cur_lsn - self.non_table_update_cmd_call.force_data_compaction_lsn
                >= NON_UPDATE_COMMAND_INTERVAL_LSN
            {
                choices.push(EventKind::ForegroundForceDataCompaction);
                self.non_table_update_cmd_call.force_data_compaction_lsn = self.cur_lsn;
            }
        }
    }

    fn generate_random_events(&mut self) -> ChaosEvent {
        let mut choices = vec![];

        self.try_push_read_snapshot_cmd(&mut choices);
        self.try_push_table_maintenance_cmd(&mut choices);
        if self.txn_state == TxnState::Empty {
            // Upsert table doesn't support streaming transaction.
            if !self.is_upsert_table {
                choices.push(EventKind::BeginStreamingTxn);
            }
            choices.push(EventKind::BeginNonStreamingTxn);
        } else {
            if self.can_append() {
                choices.push(EventKind::Append);
            }
            if self.can_delete() {
                choices.push(EventKind::Delete);
            }
            if self.can_update() {
                choices.push(EventKind::Update);
            }
            if self.txn_state == TxnState::InStreaming {
                choices.push(EventKind::StreamFlush);
                choices.push(EventKind::StreamAbort);
            }
            choices.push(EventKind::EndWithFlush);
            choices.push(EventKind::EndNoFlush);
        }
        assert!(!choices.is_empty());

        match *choices.choose(&mut self.rng).unwrap() {
            EventKind::ReadSnapshot => {
                ChaosEvent::create_snapshot_read(self.last_commit_lsn.unwrap())
            }
            EventKind::ForegroundForceSnapshot => {
                ChaosEvent::create_force_snapshot(self.last_commit_lsn.unwrap())
            }
            EventKind::ForegroundForceIndexMerge => {
                ChaosEvent::create_table_maintenance_event(TableEvent::ForceRegularIndexMerge)
            }
            EventKind::ForegroundForceDataCompaction => {
                ChaosEvent::create_table_maintenance_event(TableEvent::ForceRegularDataCompaction)
            }
            EventKind::BeginStreamingTxn => {
                self.begin_streaming_txn();
                let row = self.get_next_row_to_append();
                ChaosEvent::create_table_events(vec![TableEvent::Append {
                    row,
                    xact_id: self.get_cur_xact_id(),
                    lsn: self.get_and_update_cur_lsn(),
                    is_recovery: false,
                }])
            }
            EventKind::BeginNonStreamingTxn => {
                self.begin_non_streaming_txn();
                let row = self.get_next_row_to_append();
                ChaosEvent::create_table_events(vec![TableEvent::Append {
                    row,
                    xact_id: self.get_cur_xact_id(),
                    lsn: self.get_and_update_cur_lsn(),
                    is_recovery: false,
                }])
            }
            EventKind::Append => ChaosEvent::create_table_events(vec![TableEvent::Append {
                row: self.get_next_row_to_append(),
                xact_id: self.get_cur_xact_id(),
                lsn: self.get_and_update_cur_lsn(),
                is_recovery: false,
            }]),
            EventKind::Delete => ChaosEvent::create_table_events(vec![TableEvent::Delete {
                row: self.get_random_row_to_delete(),
                xact_id: self.get_cur_xact_id(),
                lsn: self.get_and_update_cur_lsn(),
                delete_if_exists: self.is_upsert_table,
                is_recovery: false,
            }]),
            EventKind::Update => {
                let row = self.get_random_row_to_update();
                ChaosEvent::create_table_events(vec![
                    TableEvent::Delete {
                        row: row.clone(),
                        xact_id: self.get_cur_xact_id(),
                        lsn: self.get_and_update_cur_lsn(),
                        delete_if_exists: self.is_upsert_table,
                        is_recovery: false,
                    },
                    TableEvent::Append {
                        row: row.clone(),
                        xact_id: self.get_cur_xact_id(),
                        lsn: self.get_and_update_cur_lsn(),
                        is_recovery: false,
                    },
                ])
            }
            EventKind::StreamFlush => {
                ChaosEvent::create_table_events(vec![TableEvent::StreamFlush {
                    xact_id: self.get_cur_xact_id().unwrap(),
                    is_recovery: false,
                }])
            }
            EventKind::StreamAbort => {
                let xact_id = self.get_cur_xact_id().unwrap();
                self.stream_abort_transaction();
                ChaosEvent::create_table_events(vec![TableEvent::StreamAbort {
                    xact_id,
                    is_recovery: false,
                    closes_incomplete_wal_transaction: false,
                }])
            }
            EventKind::EndWithFlush => {
                let lsn = self.get_and_update_cur_lsn();
                let xact_id = self.get_cur_xact_id();
                self.commit_transaction(lsn);
                ChaosEvent::create_table_events(vec![TableEvent::CommitFlush {
                    lsn,
                    xact_id,
                    is_recovery: false,
                }])
            }
            EventKind::EndNoFlush => {
                let lsn = self.get_and_update_cur_lsn();
                let xact_id = self.get_cur_xact_id();
                self.commit_transaction(lsn);
                ChaosEvent::create_table_events(vec![TableEvent::Commit {
                    lsn,
                    xact_id,
                    is_recovery: false,
                }])
            }
        }
    }
}

#[derive(Clone, Debug)]
enum TableMaintenanceOption {
    /// No table maintenance in background.
    NoTableMaintenance,
    /// Index merge is enabled by default: merge take place as long as there're at least two index files.
    IndexMerge,
    /// Data compaction is enabled by default: compaction take place as long as there're at least two data files.
    DataCompaction,
}

#[derive(Clone, Debug, PartialEq)]
enum SpecialTableOption {
    /// No special table option.
    None,
    /// Upsert/ delete if exists.
    UpsertDeleteIfExists,
    /// Append only.
    AppendOnly,
}

#[derive(Clone, Debug)]
struct TestEnvConfig {
    /// Test name.
    test_name: &'static str,
    /// Whether to enable disk slice writer chaos option.
    disk_slice_write_chaos_enabled: bool,
    /// Whether to enable local filesystem optimization for object storage cache.
    local_filesystem_optimization_enabled: bool,
    /// Special table option.
    special_table_option: SpecialTableOption,
    /// Table background maintenance option.
    maintenance_option: TableMaintenanceOption,
    /// Event count.
    event_count: usize,
    /// Whether error injection is enabled.
    error_injection_enabled: bool,
    /// Filesystem storage config for persistence.
    storage_config: StorageConfig,
}

#[allow(dead_code)]
struct TestEnvironment {
    chaos_test_arg: ChaosTestArgs,
    test_env_config: TestEnvConfig,
    chaos_dump_filepath: String,
    cache_temp_dir: TempDir,
    table_temp_dir: TempDir,
    object_storage_cache: ObjectStorageCache,
    read_state_manager: ReadStateManager,
    table_event_manager: TableEventManager,
    table_handler: TableHandler,
    event_sender: mpsc::Sender<TableEvent>,
    handler_event_replay_rx: mpsc::UnboundedReceiver<TableEvent>,
    wal_flush_lsn_rx: watch::Receiver<u64>,
    last_visibility_lsn_tx: watch::Sender<VisibilityLsn>,
    replication_lsn_tx: watch::Sender<u64>,
    mooncake_table_metadata: Arc<TableMetadata>,
    iceberg_table_config: IcebergTableConfig,
}

impl TestEnvironment {
    async fn new(config: TestEnvConfig) -> Self {
        let table_temp_dir = tempdir().unwrap();
        let chaos_test_arg = parse_chaos_test_args();
        let disk_slice_write_config = create_disk_slice_write_option(
            config.disk_slice_write_chaos_enabled,
            chaos_test_arg.seed,
        );
        let identity = get_random_identity(
            chaos_test_arg.seed,
            config.special_table_option == SpecialTableOption::AppendOnly,
            config.special_table_option == SpecialTableOption::UpsertDeleteIfExists,
        );
        let mooncake_table_metadata = match &config.maintenance_option {
            TableMaintenanceOption::NoTableMaintenance => create_test_table_metadata_disable_flush(
                table_temp_dir.path().to_str().unwrap().to_string(),
                disk_slice_write_config,
                identity.clone(),
            ),
            TableMaintenanceOption::IndexMerge => {
                create_test_table_metadata_with_index_merge_disable_flush(
                    table_temp_dir.path().to_str().unwrap().to_string(),
                    disk_slice_write_config,
                    identity.clone(),
                )
            }
            TableMaintenanceOption::DataCompaction => {
                create_test_table_metadata_with_data_compaction_disable_flush(
                    table_temp_dir.path().to_str().unwrap().to_string(),
                    disk_slice_write_config,
                    identity.clone(),
                )
            }
        };

        // Local filesystem to store read-through cache.
        let cache_temp_dir = tempdir().unwrap();
        let object_storage_cache = if config.local_filesystem_optimization_enabled {
            let config = ObjectStorageCacheConfig::new(
                /*max_bytes=*/ 1 << 30, // 1GiB
                cache_temp_dir.path().to_str().unwrap().to_string(),
                /*optimize_local_filesystem=*/ true,
            );
            ObjectStorageCache::new(config)
        } else {
            ObjectStorageCache::default_for_test(&cache_temp_dir)
        };

        // Create mooncake table and table event notification receiver.
        let iceberg_table_config = if config.error_injection_enabled {
            get_iceberg_table_config_with_chaos_injection(
                config.storage_config.clone(),
                chaos_test_arg.seed,
            )
        } else {
            get_iceberg_table_config_with_storage_config(config.storage_config.clone())
        };
        let table = create_mooncake_table(
            mooncake_table_metadata.clone(),
            iceberg_table_config.clone(),
            Arc::new(object_storage_cache.clone()),
        )
        .await;
        let (replication_lsn_tx, replication_lsn_rx) = watch::channel(0u64);
        let (visibility_tx, visibility_rx) = watch::channel(VisibilityLsn {
            commit_lsn: 0,
            replication_lsn: 0,
        });
        let read_state_filepath_remap =
            std::sync::Arc::new(|local_filepath: String| local_filepath);
        let read_state_manager = ReadStateManager::new(
            &table,
            replication_lsn_rx.clone(),
            visibility_rx,
            read_state_filepath_remap,
        );
        let (table_event_sync_sender, table_event_sync_receiver) = create_table_event_syncer();
        let (handler_event_replay_tx, handler_event_replay_rx) = mpsc::unbounded_channel();
        let (table_event_replay_tx, table_event_replay_rx) = mpsc::unbounded_channel();
        let table_handler = TableHandler::new(
            table,
            table_event_sync_sender,
            create_table_handler_timers(),
            replication_lsn_rx.clone(),
            Some(handler_event_replay_tx),
            Some(table_event_replay_tx),
        )
        .await;
        let wal_flush_lsn_rx = table_event_sync_receiver.wal_flush_lsn_rx.clone();
        let table_event_manager =
            TableEventManager::new(table_handler.get_event_sender(), table_event_sync_receiver);
        let event_sender = table_handler.get_event_sender();

        // Start a background task to dump serialized mooncake table event.
        // TODO(hjiang): Synchronize the background task and gracefully shutdown.
        let table_metadata_replay = ReplayTableMetadata {
            config: mooncake_table_metadata.config.clone(),
            local_filesystem_optimization_enabled: config.local_filesystem_optimization_enabled,
            storage_config: config.storage_config.clone(),
            is_upsert_table: config.special_table_option
                == SpecialTableOption::UpsertDeleteIfExists,
        };

        let chaos_dump_filepath = format!("/tmp/chaos_test_{}", Self::generate_random_filename());
        let chaos_dump_filepath_clone = chaos_dump_filepath.clone();
        println!(
            "Mooncake table events for test {} dumped to {}",
            config.test_name, chaos_dump_filepath
        );
        tokio::spawn(async move {
            Self::dump_table_event(
                chaos_dump_filepath,
                table_event_replay_rx,
                table_metadata_replay,
            )
            .await;
        });

        Self {
            chaos_test_arg,
            test_env_config: config,
            chaos_dump_filepath: chaos_dump_filepath_clone,
            cache_temp_dir,
            table_temp_dir,
            object_storage_cache,
            table_event_manager,
            read_state_manager,
            table_handler,
            event_sender,
            handler_event_replay_rx,
            wal_flush_lsn_rx,
            replication_lsn_tx,
            last_visibility_lsn_tx: visibility_tx,
            mooncake_table_metadata,
            iceberg_table_config,
        }
    }

    /// Generate a random string to use as filename.
    fn generate_random_filename() -> String {
        const TEST_FILENAME_LEN: usize = 12;
        const ALLOWED_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let mut rng = rand::rng();
        let random_string: String = (0..TEST_FILENAME_LEN)
            .map(|_| {
                let idx = rng.random_range(0..ALLOWED_CHARS.len());
                ALLOWED_CHARS[idx] as char
            })
            .collect();
        random_string
    }

    /// Continuously read from table replay channel and dump to local json file.
    async fn dump_table_event(
        chaos_dump_filepath: String,
        mut table_event_replay_rx: mpsc::UnboundedReceiver<MooncakeTableEvent>,
        table_metadata_replay: ReplayTableMetadata,
    ) {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(chaos_dump_filepath)
            .await
            .unwrap();

        // Persist table metadata for replay.
        let json_str = serde_json::to_string(&table_metadata_replay).unwrap();
        file.write_all(json_str.as_bytes()).await.unwrap();
        file.write_all(b"\n").await.unwrap();

        // Persist mooncake table events.
        let mut written_events_count: usize = 0;
        const WRITREN_EVENT_FLUSH_INTERVAL: usize = 100;
        while let Some(table_event) = table_event_replay_rx.recv().await {
            let json_str = serde_json::to_string(&table_event).unwrap();
            file.write_all(json_str.as_bytes()).await.unwrap();
            file.write_all(b"\n").await.unwrap();

            written_events_count += 1;
            if written_events_count % WRITREN_EVENT_FLUSH_INTERVAL == 0 {
                file.flush().await.unwrap();
            }
        }
        file.flush().await.unwrap();
        drop(file);
    }
}

/// Test util function to check whether iceberg snapshot contains expected content.
async fn validate_persisted_iceberg_table(
    mooncake_table_metadata: Arc<TableMetadata>,
    iceberg_table_config: IcebergTableConfig,
    snapshot_lsn: u64,
    expected_ids: Vec<i32>,
) {
    let (event_sender, _event_receiver) = mpsc::channel(100);
    let (replication_lsn_tx, replication_lsn_rx) = watch::channel(0u64);
    let (visibility_tx, visibility_rx) = watch::channel(VisibilityLsn {
        commit_lsn: 0,
        replication_lsn: 0,
    });
    replication_lsn_tx.send(snapshot_lsn).unwrap();
    visibility_tx
        .send(VisibilityLsn {
            commit_lsn: snapshot_lsn,
            replication_lsn: snapshot_lsn,
        })
        .unwrap();

    // Use a fresh new cache for new iceberg table manager.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    let mut table = create_mooncake_table(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache,
    )
    .await;
    table.register_table_notify(event_sender).await;

    let read_state_filepath_remap = std::sync::Arc::new(|local_filepath: String| local_filepath);
    let read_state_manager = ReadStateManager::new(
        &table,
        replication_lsn_rx.clone(),
        visibility_rx,
        read_state_filepath_remap,
    );
    check_read_snapshot(
        &read_state_manager,
        Some(snapshot_lsn),
        /*expected_ids=*/ &expected_ids,
    )
    .await;
}

async fn chaos_test_impl(mut env: TestEnvironment) {
    let test_env_config = env.test_env_config.clone();
    let event_sender = env.event_sender.clone();
    let read_state_manager = env.read_state_manager;
    let mut table_event_manager = env.table_event_manager;
    let last_visibility_lsn_tx = env.last_visibility_lsn_tx;
    let replication_lsn_tx = env.replication_lsn_tx.clone();

    // Fields used to recreate a new mooncake table.
    let mooncake_table_metadata = env.mooncake_table_metadata.clone();
    let iceberg_table_config = env.iceberg_table_config.clone();
    let cloned_args = env.chaos_test_arg.clone();

    let task = tokio::spawn(async move {
        let mut state = ChaosState::new(
            read_state_manager,
            cloned_args.seed,
            test_env_config.special_table_option == SpecialTableOption::AppendOnly,
            test_env_config.special_table_option == SpecialTableOption::UpsertDeleteIfExists,
        );
        println!(
            "Test {} is with random seed {}",
            test_env_config.test_name, state.random_seed
        );

        // Attempt read on empty table.
        check_read_snapshot(
            &state.read_state_manager,
            /*requested_read_lsn=*/ Some(0),
            /*expected_ids=*/ &[],
        )
        .await;

        for _ in 0..test_env_config.event_count {
            let chaos_events = state.generate_random_events();

            // Perform table maintenance operations.
            if let Some(TableEvent::ForceRegularIndexMerge) = &chaos_events.table_maintenance_event
            {
                let mut rx = table_event_manager.initiate_index_merge().await;
                rx.recv().await.unwrap().unwrap();
            }
            if let Some(TableEvent::ForceRegularDataCompaction) =
                &chaos_events.table_maintenance_event
            {
                let mut rx = table_event_manager.initiate_data_compaction().await;
                rx.recv().await.unwrap().unwrap();
            }

            // Perform table update operations.
            for cur_event in chaos_events.table_events.into_iter() {
                // For commit events, need to set up corresponding replication and commit LSN.
                if let TableEvent::Commit { lsn, .. } = cur_event {
                    replication_lsn_tx.send(lsn).unwrap();
                    last_visibility_lsn_tx
                        .send(VisibilityLsn {
                            commit_lsn: lsn,
                            replication_lsn: lsn,
                        })
                        .unwrap();
                } else if let TableEvent::CommitFlush { lsn, .. } = cur_event {
                    replication_lsn_tx.send(lsn).unwrap();
                    last_visibility_lsn_tx
                        .send(VisibilityLsn {
                            commit_lsn: lsn,
                            replication_lsn: lsn,
                        })
                        .unwrap();
                }
                event_sender.send(cur_event).await.unwrap();
            }

            // Perform snapshot read operation and check.
            if let Some(read_lsn) = chaos_events.snapshot_read_lsn {
                let requested_read_lsn = if read_lsn == 0 { None } else { Some(read_lsn) };
                let expected_ids = state.get_valid_ids();
                check_read_snapshot(
                    &state.read_state_manager,
                    requested_read_lsn,
                    /*expected_ids=*/ &expected_ids,
                )
                .await;
            }

            // Perform force snapshot and check.
            if let Some(snapshot_lsn) = chaos_events.force_snapshot_lsn {
                let rx = table_event_manager.initiate_snapshot(snapshot_lsn).await;
                TableEventManager::synchronize_force_snapshot_request(rx, snapshot_lsn)
                    .await
                    .unwrap();

                // Now iceberg snapshot content should be exactly the same as moooncake table, recover states from persistence layer and perform another read.
                validate_persisted_iceberg_table(
                    mooncake_table_metadata.clone(),
                    iceberg_table_config.clone(),
                    snapshot_lsn,
                    state.get_valid_ids(),
                )
                .await;
            }
        }

        // TODO(hjiang): Temporarily hard code a sleep time to trigger background tasks.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // If anything bad happens in the eventloop, drop table would fail.
        table_event_manager.drop_table().await.unwrap();
    });

    // Await the task directly and handle its result.
    let task_result = task.await;

    // Print out events in order if chaos test fails.
    if let Err(e) = task_result {
        // Display all enqueued events for debugging and replay.
        while let Some(cur_event) = env.handler_event_replay_rx.recv().await {
            println!("{cur_event:?}");
        }
        // Propagate the panic to fail the test.
        if let Ok(panic) = e.try_into_panic() {
            std::panic::resume_unwind(panic);
        }
    } else if env.chaos_test_arg.print_events_on_success {
        // Optionally print events even when the test succeeded, for debugging.
        while let Some(cur_event) = env.handler_event_replay_rx.recv().await {
            println!("{cur_event:?}");
        }
    }
}

/// ============================
/// Disk slice write with chaos
/// ============================
///
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_disk_slice_chaos_on_local_fs() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: true,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::NoTableMaintenance,
        error_injection_enabled: false,
        event_count: 1500,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// ============================
/// Local filesystem persistence
/// ============================
///
/// Chaos test with no background table maintenance enabled.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos_on_local_fs_with_no_background_maintenance() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::NoTableMaintenance,
        error_injection_enabled: false,
        event_count: 4000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with index merge enabled by default.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos_on_local_fs_with_index_merge() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::IndexMerge,
        error_injection_enabled: false,
        event_count: 5000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with data compaction enabled by default.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos_on_local_fs_with_data_compaction() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::DataCompaction,
        error_injection_enabled: false,
        event_count: 5000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// ============================
/// Local filesystem persistence with optimization
/// ============================
///
/// Chaos test with no background table maintenance enabled.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_local_system_optimization_chaos_with_no_background_maintenance() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: true,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::NoTableMaintenance,
        error_injection_enabled: false,
        event_count: 4000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with index merge enabled by default.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_local_system_optimization_chaos_with_index_merge() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: true,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::IndexMerge,
        error_injection_enabled: false,
        event_count: 5000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with data compaction enabled by default.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_local_system_optimization_chaos_with_data_compaction() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: true,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::DataCompaction,
        error_injection_enabled: false,
        event_count: 5000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// ============================
/// S3 persistence
/// ============================
///
/// Chaos test with no background table maintenance enabled.
#[named]
#[cfg(feature = "storage-s3")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_chaos_with_no_background_maintenance() {
    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let accessor_config = create_s3_storage_config(&warehouse_uri);
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::NoTableMaintenance,
        error_injection_enabled: false,
        event_count: 4000,
        storage_config: accessor_config.storage_config,
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with index merge enabled by default.
#[named]
#[cfg(feature = "storage-s3")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_chaos_with_index_merge() {
    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let accessor_config = create_s3_storage_config(&warehouse_uri);
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::IndexMerge,
        error_injection_enabled: false,
        event_count: 5000,
        storage_config: accessor_config.storage_config,
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with data compaction enabled by default.
#[named]
#[cfg(feature = "storage-s3")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_chaos_with_data_compaction() {
    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let accessor_config = create_s3_storage_config(&warehouse_uri);
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::DataCompaction,
        error_injection_enabled: false,
        event_count: 5000,
        storage_config: accessor_config.storage_config,
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// ============================
/// GCS persistence
/// ============================
///
/// Chaos test with no background table maintenance enabled.
#[named]
#[cfg(feature = "storage-gcs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gcs_chaos_with_no_background_maintenance() {
    let (bucket, warehouse_uri) = get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let accessor_config = create_gcs_storage_config(&warehouse_uri);
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::NoTableMaintenance,
        error_injection_enabled: false,
        event_count: 4000,
        storage_config: accessor_config.storage_config,
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with index merge enabled by default.
#[named]
#[cfg(feature = "storage-gcs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gcs_chaos_with_index_merge() {
    let (bucket, warehouse_uri) = get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let accessor_config = create_gcs_storage_config(&warehouse_uri);
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::IndexMerge,
        error_injection_enabled: false,
        event_count: 5000,
        storage_config: accessor_config.storage_config,
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with data compaction enabled by default.
#[named]
#[cfg(feature = "storage-gcs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gcs_chaos_with_data_compaction() {
    let (bucket, warehouse_uri) = get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let accessor_config = create_gcs_storage_config(&warehouse_uri);
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::DataCompaction,
        error_injection_enabled: false,
        event_count: 5000,
        storage_config: accessor_config.storage_config,
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// ============================
/// Delay and error injection
/// ============================
///
/// Chaos test with no background table maintenance enabled.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos_injection_with_no_background_maintenance() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::NoTableMaintenance,
        error_injection_enabled: true,
        event_count: 100,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with index merge enabled by default.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos_injection_with_index_merge() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::IndexMerge,
        error_injection_enabled: true,
        event_count: 100,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with data compaction enabled by default.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos_injection_with_data_compaction() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::DataCompaction,
        error_injection_enabled: true,
        event_count: 100,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// ============================
/// Append-only operation
/// ============================
///
/// Chaos test with no background table maintenance enabled.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_append_only_chaos_on_local_fs_with_no_background_maintenance() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::AppendOnly,
        maintenance_option: TableMaintenanceOption::NoTableMaintenance,
        error_injection_enabled: false,
        event_count: 4000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with data compaction enabled by default.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_append_only_chaos_on_local_fs_with_data_compaction() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: false,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::AppendOnly,
        maintenance_option: TableMaintenanceOption::DataCompaction,
        error_injection_enabled: false,
        event_count: 5000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// ============================
/// Upsert table operations
/// ============================
///
/// Chaos test with no background table maintenance enabled.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upsert_chaos_with_no_background_maintenance() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: true,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::UpsertDeleteIfExists,
        maintenance_option: TableMaintenanceOption::NoTableMaintenance,
        error_injection_enabled: false,
        event_count: 4000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with index merge enabled by default.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upsert_chaos_with_index_merge() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: true,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::UpsertDeleteIfExists,
        maintenance_option: TableMaintenanceOption::IndexMerge,
        error_injection_enabled: false,
        event_count: 5000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with data compaction enabled by default.
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upsert_chaos_with_data_compaction() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: true,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::UpsertDeleteIfExists,
        maintenance_option: TableMaintenanceOption::DataCompaction,
        error_injection_enabled: false,
        event_count: 5000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// ============================
/// Replay system validation
/// ============================
///
#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_replay_chaos_with_no_background_maintenance() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: true,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::NoTableMaintenance,
        error_injection_enabled: false,
        event_count: 4000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    let chaos_dump_filepath = env.chaos_dump_filepath.clone();
    chaos_test_impl(env).await;

    // Replay mooncake table events.
    replay(&chaos_dump_filepath).await;
}

#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_replay_chaos_with_data_compaction() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: true,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::DataCompaction,
        error_injection_enabled: false,
        event_count: 4000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    let chaos_dump_filepath = env.chaos_dump_filepath.clone();
    chaos_test_impl(env).await;

    // Replay mooncake table events.
    replay(&chaos_dump_filepath).await;
}

#[named]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_replay_chaos_with_index_merge() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!(),
        local_filesystem_optimization_enabled: true,
        disk_slice_write_chaos_enabled: false,
        special_table_option: SpecialTableOption::None,
        maintenance_option: TableMaintenanceOption::IndexMerge,
        error_injection_enabled: false,
        event_count: 4000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    let chaos_dump_filepath = env.chaos_dump_filepath.clone();
    chaos_test_impl(env).await;

    // Replay mooncake table events.
    replay(&chaos_dump_filepath).await;
}
