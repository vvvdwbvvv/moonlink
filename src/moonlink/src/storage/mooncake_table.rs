pub mod batch_id_counter;
mod batch_ingestion;
pub mod data_batches;
pub(crate) mod delete_vector;
mod disk_slice;
mod iceberg_persisted_records;
mod mem_slice;
mod persistence_buffer;
pub(crate) mod replay;
mod shared_array;
pub(crate) mod snapshot;
mod snapshot_cache_utils;
mod snapshot_maintenance;
mod snapshot_persistence;
mod snapshot_read;
pub mod snapshot_read_output;
mod snapshot_validation;
pub mod table_config;
pub mod table_event_manager;
pub mod table_secret;
mod table_snapshot;
pub mod table_status;
pub mod table_status_reader;
mod transaction_stream;

use super::iceberg::puffin_utils::PuffinBlobRef;
use super::index::{FileIndex, MemIndex, MooncakeIndex};
use super::storage_utils::{MooncakeDataFileRef, RawDeletionRecord, RecordLocation};
use crate::error::Result;
use crate::row::{IdentityProp, MoonlinkRow};
use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::compaction::compactor::{CompactionBuilder, CompactionFileParams};
pub(crate) use crate::storage::compaction::table_compaction::{
    DataCompactionPayload, DataCompactionResult,
};
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::iceberg::iceberg_table_config::IcebergTableConfig;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::iceberg::table_manager::{PersistenceFileParams, TableManager};
use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;
use crate::storage::mooncake_table::batch_id_counter::BatchIdCounter;
use crate::storage::mooncake_table::iceberg_persisted_records::IcebergPersistedRecords;
use crate::storage::mooncake_table::replay::replay_events;
use crate::storage::mooncake_table::replay::replay_events::MooncakeTableEvent;
use crate::storage::mooncake_table::shared_array::SharedRowBufferSnapshot;
use crate::storage::mooncake_table::snapshot::MooncakeSnapshotOutput;
pub use crate::storage::mooncake_table::snapshot_read_output::ReadOutput as SnapshotReadOutput;
#[cfg(test)]
pub(crate) use crate::storage::mooncake_table::table_snapshot::IcebergSnapshotDataCompactionPayload;
pub(crate) use crate::storage::mooncake_table::table_snapshot::{
    take_data_files_to_import, take_data_files_to_remove, take_file_indices_to_import,
    take_file_indices_to_remove, FileIndiceMergePayload, FileIndiceMergeResult,
    IcebergSnapshotDataCompactionResult, IcebergSnapshotImportPayload,
    IcebergSnapshotIndexMergePayload, IcebergSnapshotPayload, IcebergSnapshotResult,
};
use crate::storage::mooncake_table_config::MooncakeTableConfig;
use crate::storage::snapshot_options::MaintenanceOption;
use crate::storage::snapshot_options::SnapshotOption;
use crate::storage::storage_utils::{FileId, TableId};
use crate::storage::wal::{WalManager, WalPersistenceUpdateResult};
use crate::table_notify::TableEvent;
use crate::NonEvictableHandle;
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use delete_vector::BatchDeletionVector;
pub(crate) use disk_slice::DiskSliceWriter;
use mem_slice::MemSlice;
use more_asserts as ma;
pub(crate) use snapshot::SnapshotTableState;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use table_snapshot::{IcebergSnapshotImportResult, IcebergSnapshotIndexMergeResult};
#[cfg(test)]
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::{watch, RwLock};
use tracing::info_span;
use tracing::Instrument;
use transaction_stream::{TransactionStreamOutput, TransactionStreamState};

#[derive(Debug)]
pub struct TableMetadata {
    /// table name
    pub(crate) name: String,
    /// table id
    /// Notice it's transient, which means it's subject to change after recovery.
    pub(crate) table_id: u32,
    /// table schema
    pub(crate) schema: Arc<Schema>,
    /// table config
    pub(crate) config: MooncakeTableConfig,
    /// storage path
    pub(crate) path: PathBuf,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AlterTableRequest {
    pub(crate) new_columns: Vec<arrow_schema::FieldRef>,
    pub(crate) dropped_columns: Vec<String>,
}

impl TableMetadata {
    pub fn new_for_alter_table(
        previous_metadata: Arc<TableMetadata>,
        alter_table_request: AlterTableRequest,
    ) -> Self {
        let mut new_columns = vec![];
        for field in previous_metadata.schema.fields.iter() {
            if !alter_table_request.dropped_columns.contains(field.name()) {
                new_columns.push(field.clone());
            }
        }
        new_columns.extend(alter_table_request.new_columns);
        let new_schema =
            Schema::new_with_metadata(new_columns, previous_metadata.schema.metadata.clone());
        Self {
            name: previous_metadata.name.clone(),
            table_id: previous_metadata.table_id,
            schema: Arc::new(new_schema),
            config: previous_metadata.config.clone(),
            path: previous_metadata.path.clone(),
        }
    }

    /// Validate metadata invariants.
    pub fn validate(&self) {
        // Validate identity property.
        if self.config.row_identity == IdentityProp::None {
            assert!(self.config.append_only);
        }
        if self.config.append_only {
            assert_eq!(self.config.row_identity, IdentityProp::None);
        }
        // Validate table config.
        self.config.validate();
    }
}
#[derive(Clone, Debug)]
pub(crate) struct DiskFileEntry {
    /// Cache handle. If assigned, it's pinned in object storage cache.
    pub(crate) cache_handle: Option<NonEvictableHandle>,
    /// Number of rows.
    pub(crate) num_rows: usize,
    /// File size.
    pub(crate) file_size: usize,
    /// Committed deletion vector, used for new deletion records in-memory processing.
    pub(crate) committed_deletion_vector: BatchDeletionVector,
    /// Persisted iceberg deletion vector puffin blob.
    pub(crate) puffin_deletion_blob: Option<PuffinBlobRef>,
}

/// Snapshot contains state of the table at a given time.
/// A snapshot maps directly to an iceberg snapshot.
///
#[derive(Clone)]
pub struct Snapshot {
    /// table metadata
    pub(crate) metadata: Arc<TableMetadata>,
    /// datafile and their deletion vector.
    ///
    /// TODO(hjiang):
    /// 1. For the initial release and before we figure out a cache design, disk files are always local ones.
    /// 2. Add corresponding file indices into the value part, so when data file gets compacted, we make sure all related file indices get rewritten and compacted as well.
    pub(crate) disk_files: HashMap<MooncakeDataFileRef, DiskFileEntry>,
    /// Current snapshot version, which is the mooncake table commit point.
    pub(crate) snapshot_version: u64,
    /// LSN which last data file flush operation happens.
    ///
    /// There're two important time points: commit and flush.
    /// - Data files are persisted at flush point, which could span across multiple commit points;
    /// - Batch deletion vector, which is the value for `Snapshot::disk_files` updates at commit points.
    ///   So likely they are not consistent from LSN's perspective.
    ///
    /// At iceberg snapshot creation, we should only dump consistent data files and deletion logs.
    /// Data file flush LSN is recorded here, to get corresponding deletion logs from "committed deletion logs".
    pub(crate) flush_lsn: Option<u64>,
    /// LSN of largest completed flush operations.
    pub(crate) largest_flush_lsn: Option<u64>,
    /// indices
    pub(crate) indices: MooncakeIndex,
}

impl Snapshot {
    pub(crate) fn new(metadata: Arc<TableMetadata>) -> Self {
        Self {
            metadata,
            disk_files: HashMap::new(),
            snapshot_version: 0,
            flush_lsn: None,
            largest_flush_lsn: None,
            indices: MooncakeIndex::new(),
        }
    }

    /// Get the number of rows in the current snapshot, including the ones get deleted.
    pub fn get_cardinality(&self) -> u64 {
        self.disk_files
            .values()
            .map(|entry| entry.num_rows as u64)
            .sum()
    }

    pub fn get_name_for_inmemory_file(&self) -> PathBuf {
        let mut directory = PathBuf::from(&self.metadata.config.temp_files_directory);
        directory.push(format!(
            "inmemory_{}_{}_{}.parquet",
            self.metadata.name, self.metadata.table_id, self.snapshot_version
        ));
        directory
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("disk files count", &self.disk_files.len())
            .field("file indices count", &self.indices.file_indices.len())
            .field("flush_lsn", &self.flush_lsn)
            .field("snapshot_version", &self.snapshot_version)
            .finish()
    }
}

struct RecordBatchWithDeletionVector {
    batch_id: u64,
    record_batch: Arc<RecordBatch>,
    deletion_vector: Option<BatchDeletionVector>,
}

#[derive(Default)]
pub struct SnapshotTask {
    /// Mooncake table config.
    mooncake_table_config: MooncakeTableConfig,

    /// ---- States not recorded by mooncake snapshot ----
    ///
    new_disk_slices: Vec<DiskSliceWriter>,
    new_deletions: Vec<RawDeletionRecord>,
    /// Pair of <batch id, record batch, optional deletion vector for streaming batches>.
    new_record_batches: Vec<RecordBatchWithDeletionVector>,
    new_rows: Option<SharedRowBufferSnapshot>,
    new_mem_indices: Vec<Arc<MemIndex>>,

    // Prevent attempting to delete a row created after the deletion's lsn.
    new_disk_file_lsn_map: HashMap<FileId, u64>,
    flushing_batch_lsn_map: HashMap<u64, u64>,

    /// Assigned (non-zero) after a commit event.
    /// Inherits the previous snapshot tasks commit LSN baseline on snapshot.
    commit_lsn_baseline: u64,
    /// Commit LSN baseline of the previous snapshot task.
    /// We use this to determine if commit_lsn_baseline has been updated.
    prev_commit_lsn_baseline: u64,
    /// Assigned at a flush operation completion, which means all flushes with LSN <= [`new_flush_lsn`] have finished.
    new_flush_lsn: Option<u64>,
    /// Assigned at a flush operation completion, which records the largest flush LSN completed.
    new_largest_flush_lsn: Option<u64>,

    new_commit_point: Option<RecordLocation>,

    /// streaming xact
    new_streaming_xact: Vec<TransactionStreamOutput>,

    /// Schema change, or force snapshot.
    force_empty_iceberg_payload: bool,

    /// Committed deletion records, which have been persisted into iceberg, and should be pruned from mooncake snapshot.
    committed_deletion_logs: HashSet<(FileId, usize /*row idx*/)>,

    /// --- States related to file indices merge operation ---
    /// These persisted items will be reflected to mooncake snapshot in the next invocation of periodic mooncake snapshot operation.
    index_merge_result: FileIndiceMergeResult,

    /// --- States related to data compaction operation ---
    /// Disk file ids which take part in the compaction.
    compacting_data_files: HashSet<FileId>,
    /// These persisted items will be reflected to mooncake snapshot in the next invocation of periodic mooncake snapshot operation.
    data_compaction_result: DataCompactionResult,

    /// ---- States have been recorded by mooncake snapshot, and persisted into iceberg table ----
    /// These persisted items will be reflected to mooncake snapshot in the next invocation of periodic mooncake snapshot operation.
    iceberg_persisted_records: IcebergPersistedRecords,

    /// Minimum LSN of ongoing flushes.
    min_ongoing_flush_lsn: u64,
}

impl SnapshotTask {
    pub fn new(mooncake_table_config: MooncakeTableConfig) -> Self {
        Self {
            mooncake_table_config,
            new_disk_slices: Vec::new(),
            new_disk_file_lsn_map: HashMap::new(),
            flushing_batch_lsn_map: HashMap::new(),
            new_deletions: Vec::new(),
            new_record_batches: Vec::new(),
            new_rows: None,
            new_mem_indices: Vec::new(),
            commit_lsn_baseline: 0,
            prev_commit_lsn_baseline: 0,
            new_flush_lsn: None,
            new_largest_flush_lsn: None,
            new_commit_point: None,
            new_streaming_xact: Vec::new(),
            force_empty_iceberg_payload: false,
            // Committed deletion logs which have been persisted, and should be pruned from mooncake snapshot.
            committed_deletion_logs: HashSet::new(),
            // Index merge related fields.
            index_merge_result: FileIndiceMergeResult::default(),
            // Data compaction related fields.
            compacting_data_files: HashSet::new(),
            data_compaction_result: DataCompactionResult::default(),
            // Iceberg persistence result.
            iceberg_persisted_records: IcebergPersistedRecords::default(),
            min_ongoing_flush_lsn: u64::MAX,
        }
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        if !self.new_disk_slices.is_empty() {
            assert!(!self.new_disk_file_lsn_map.is_empty());
            assert!(self.new_flush_lsn.is_some());
            return false;
        }
        if !self.new_deletions.is_empty() {
            return false;
        }
        if !self.new_mem_indices.is_empty() {
            return false;
        }
        if !self.new_streaming_xact.is_empty() {
            return false;
        }
        if !self.index_merge_result.is_empty() {
            return false;
        }
        if !self.data_compaction_result.is_empty() {
            return false;
        }
        if !self.iceberg_persisted_records.is_empty() {
            return false;
        }
        true
    }

    pub fn should_create_snapshot(&self) -> bool {
        // If mooncake has new transaction commits.
        (self.commit_lsn_baseline > 0 && self.commit_lsn_baseline != self.prev_commit_lsn_baseline)
            || self.force_empty_iceberg_payload
        // If mooncake table has completed streaming transactions.
            || !self.new_streaming_xact.is_empty()
        // If mooncake table accumulated large enough writes.
            || !self.new_disk_slices.is_empty()
            || self.new_deletions.len()
                >= self.mooncake_table_config.snapshot_deletion_record_count()
            // If iceberg snapshot is already performed, update mooncake snapshot accordingly.
            // On local filesystem, potentially we could double storage as soon as possible.
            || !self.iceberg_persisted_records.import_result.is_empty()
            || !self.iceberg_persisted_records.index_merge_result.is_empty()
            || !self.iceberg_persisted_records.data_compaction_result.is_empty()
    }

    /// Get newly created data files, including both batch write ones and stream write ones.
    pub(crate) fn get_new_data_files(&self) -> Vec<MooncakeDataFileRef> {
        let mut new_files = vec![];

        // Batch write data files.
        for cur_disk_slice in self.new_disk_slices.iter() {
            new_files.extend(
                cur_disk_slice
                    .output_files()
                    .iter()
                    .map(|(file, _)| file.clone()),
            );
        }

        // Stream write data files.
        for cur_stream_xact in self.new_streaming_xact.iter() {
            if let TransactionStreamOutput::Commit(cur_stream_commit) = cur_stream_xact {
                new_files.extend(cur_stream_commit.get_flushed_data_files());
            }
        }

        new_files
    }

    /// Get newly created file indices, including both batch write ones and stream write ones.
    pub(crate) fn get_new_file_indices(&self) -> Vec<FileIndex> {
        let mut new_file_indices = vec![];

        // Batch write file indices.
        for cur_disk_slice in self.new_disk_slices.iter() {
            let file_index = cur_disk_slice.get_file_index();
            if let Some(file_index) = file_index {
                new_file_indices.push(file_index);
            }
        }

        // Stream write file indices.
        for cur_stream_xact in self.new_streaming_xact.iter() {
            if let TransactionStreamOutput::Commit(cur_stream_commit) = cur_stream_xact {
                new_file_indices.extend(cur_stream_commit.get_file_indices());
            }
        }

        new_file_indices
    }

    /// Attempt to set largest flush LSN.
    pub(crate) fn try_set_largest_flush_lsn(&mut self, flush_lsn: u64) {
        if self.new_largest_flush_lsn.is_some() && self.new_largest_flush_lsn.unwrap() >= flush_lsn
        {
            return;
        }
        self.new_largest_flush_lsn = Some(flush_lsn);
    }
}

/// Background task (i.e., mooncake snapshot) status, which is used for validation.
#[derive(Clone, Debug, Default)]
struct BackgroundTaskStatus {
    mooncake_snapshot_ongoing: bool,
    iceberg_snapshot_ongoing: bool,
    index_merge_ongoing: bool,
    data_compaction_ongoing: bool,
}

/// MooncakeTable is a disk table + mem slice.
/// Transactions will append data to the mem slice.
///
/// And periodically disk slices will be merged and compacted.
/// Single thread is used to write to the table.
///
/// LSN is used for visibility control of mooncake table.
/// Currently it has following rules:
/// For read at lsn X, any record committed at lsn <= X is visible.
/// For commit at lsn X, any record whose lsn < X is committed.
///
/// COMMIT_LSN_xact_1 <= DELETE_LSN_xact_2 < COMMIT_LSN_xact_2
///
pub struct MooncakeTable {
    /// Current metadata of the table.
    ///
    metadata: Arc<TableMetadata>,

    /// The mem slice
    ///
    mem_slice: MemSlice,

    /// Current snapshot of the table
    snapshot: Arc<RwLock<SnapshotTableState>>,

    /// Background task status, which is ONLY used for invariant validation.
    background_task_status_for_validation: BackgroundTaskStatus,

    table_snapshot_watch_sender: watch::Sender<u64>,
    table_snapshot_watch_receiver: watch::Receiver<u64>,

    /// Records all the write operations since last snapshot.
    next_snapshot_task: SnapshotTask,

    /// Stream state per transaction, keyed by xact-id.
    transaction_stream_states: HashMap<u32, TransactionStreamState>,

    /// Auto increment id for generating unique file ids.
    /// Note, these ids is only used locally, and not persisted.
    next_file_id: u32,

    /// Batch ID counters for the two-counter allocation strategy
    non_streaming_batch_id_counter: Arc<BatchIdCounter>,
    streaming_batch_id_counter: Arc<BatchIdCounter>,

    /// Iceberg table manager, used to sync snapshot to the corresponding iceberg table.
    iceberg_table_manager: Option<Box<dyn TableManager>>,

    /// LSN of the latest iceberg snapshot.
    last_iceberg_snapshot_lsn: Option<u64>,

    /// Table notifier, which is used to sent multiple types of event completion information.
    table_notify: Option<Sender<TableEvent>>,

    /// WAL manager, used to persist WAL events.
    wal_manager: WalManager,

    /// LSN of ongoing flushes.
    /// Maps from LSN to its count.
    pub ongoing_flush_lsns: BTreeMap<u64, u32>,

    /// Unrecorded completed flush LSNs.
    ///
    /// All flush operations are async, which means it's possible that flushes with larger LSN finish before those with smaller LSNs, so they cannot be reflected to snapshot flush LSN immediately.
    /// To avoid losing LSNs, we need to keep track of early completed unrecorded LSNs as well.
    pub completed_unrecorded_flush_lsns: BTreeSet<u64>,

    /// Table replay sender.
    event_replay_tx: Option<mpsc::UnboundedSender<MooncakeTableEvent>>,
}

impl MooncakeTable {
    /// foreground functions
    ///
    /// Note that wal manager is constructed outside of the constructor as
    /// it may be recovered from persistent wal metadata.
    /// TODO(hjiang): Provide a struct to hold all parameters.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        schema: Schema,
        name: String,
        table_id: u32,
        base_path: PathBuf,
        iceberg_table_config: IcebergTableConfig,
        table_config: MooncakeTableConfig,
        wal_manager: WalManager,
        object_storage_cache: Arc<dyn CacheTrait>,
        table_filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
    ) -> Result<Self> {
        let metadata = Arc::new(TableMetadata {
            name,
            table_id,
            schema: Arc::new(schema.clone()),
            config: table_config.clone(),
            path: base_path,
        });
        let iceberg_table_manager = Box::new(
            IcebergTableManager::new(
                metadata.clone(),
                object_storage_cache.clone(),
                table_filesystem_accessor.clone(),
                iceberg_table_config,
            )
            .await?,
        );

        Self::new_with_table_manager(
            metadata,
            iceberg_table_manager,
            object_storage_cache,
            table_filesystem_accessor,
            wal_manager,
        )
        .await
    }

    pub(crate) async fn new_with_table_manager(
        table_metadata: Arc<TableMetadata>,
        mut table_manager: Box<dyn TableManager>,
        object_storage_cache: Arc<dyn CacheTrait>,
        table_filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
        wal_manager: WalManager,
    ) -> Result<Self> {
        table_metadata.validate();
        let (table_snapshot_watch_sender, table_snapshot_watch_receiver) = watch::channel(u64::MAX);
        let (next_file_id, current_snapshot) = table_manager.load_snapshot_from_table().await?;
        let last_iceberg_snapshot_lsn = current_snapshot.flush_lsn;
        if let Some(last_iceberg_snapshot_lsn) = last_iceberg_snapshot_lsn {
            // We should NOT send the wal_highest_completion_lsn, because those events are not applied at this point yet.
            // They will replayed through the event stream, and re-applied to the table.
            table_snapshot_watch_sender
                .send(last_iceberg_snapshot_lsn)
                .unwrap();
        }

        let non_streaming_batch_id_counter = Arc::new(BatchIdCounter::new(false));
        let streaming_batch_id_counter = Arc::new(BatchIdCounter::new(true));

        Ok(Self {
            mem_slice: MemSlice::new(
                table_metadata.schema.clone(),
                table_metadata.config.batch_size,
                table_metadata.config.row_identity.clone(),
                Arc::clone(&non_streaming_batch_id_counter),
            ),
            metadata: table_metadata.clone(),
            snapshot: Arc::new(RwLock::new(
                SnapshotTableState::new(
                    table_manager.get_warehouse_location(),
                    table_metadata.clone(),
                    object_storage_cache,
                    table_filesystem_accessor,
                    current_snapshot,
                    Arc::clone(&non_streaming_batch_id_counter),
                )
                .await?,
            )),
            background_task_status_for_validation: BackgroundTaskStatus::default(),
            next_snapshot_task: SnapshotTask::new(table_metadata.as_ref().config.clone()),
            transaction_stream_states: HashMap::new(),
            table_snapshot_watch_sender,
            table_snapshot_watch_receiver,
            next_file_id,
            non_streaming_batch_id_counter,
            streaming_batch_id_counter,
            iceberg_table_manager: Some(table_manager),
            last_iceberg_snapshot_lsn,
            table_notify: None,
            wal_manager,
            ongoing_flush_lsns: BTreeMap::new(),
            completed_unrecorded_flush_lsns: BTreeSet::new(),
            event_replay_tx: None,
        })
    }

    pub(crate) fn alter_table(
        &mut self,
        alter_table_request: AlterTableRequest,
    ) -> Arc<TableMetadata> {
        assert!(
            self.mem_slice.is_empty(),
            "Cannot alter table with non-empty mem slice"
        );
        assert!(
            self.next_snapshot_task.is_empty(),
            "Cannot alter table with pending snapshot task"
        );

        // Create new table metadata.
        let new_metadata = Arc::new(TableMetadata::new_for_alter_table(
            self.metadata.clone(),
            alter_table_request,
        ));

        // Follow the same initialization order as mooncake table, which is used to decide batch id assignment.
        self.mem_slice = MemSlice::new(
            new_metadata.schema.clone(),
            new_metadata.config.batch_size,
            new_metadata.config.row_identity.clone(),
            Arc::clone(&self.non_streaming_batch_id_counter),
        );
        let mut guard = self.snapshot.try_write().unwrap();
        guard.reset_for_alter(new_metadata.clone());
        assert!(
            self.metadata.schema.fields.len() != new_metadata.schema.fields.len(),
            "Only support alter table with add/drop fields"
        );

        self.metadata = new_metadata.clone();
        new_metadata
    }

    /// Register event completion notifier.
    /// Notice it should be registered only once, which could be used to notify multiple events.
    pub(crate) async fn register_table_notify(&mut self, table_notify: Sender<TableEvent>) {
        assert!(self.table_notify.is_none());
        self.table_notify = Some(table_notify.clone());
        self.snapshot
            .write()
            .await
            .register_table_notify(table_notify);
    }

    /// Register event replay sender.
    /// Notice it should be registered only once, which could be used to notify multiple events.
    pub(crate) fn register_event_replay_tx(
        &mut self,
        event_replay_tx: Option<mpsc::UnboundedSender<MooncakeTableEvent>>,
    ) {
        assert!(self.event_replay_tx.is_none());
        self.event_replay_tx = event_replay_tx;
    }

    /// Assert flush LSN doesn't regress.
    /// There're several cases for equal flush LSN, for example, force snapshot, table maintenance, etc.
    fn assert_flush_lsn_on_iceberg_snapshot_res(
        persistence_lsn: Option<u64>,
        iceberg_snapshot_res: &IcebergSnapshotResult,
    ) {
        let flush_lsn = iceberg_snapshot_res.flush_lsn;
        assert!(
                persistence_lsn.is_none()
                    || persistence_lsn.unwrap() <= flush_lsn,
                "Last iceberg snapshot LSN is {:?}, flush LSN is {:?}, imported data file number is {}, imported puffin file number is {}",
                persistence_lsn,
                flush_lsn,
                iceberg_snapshot_res.import_result.new_data_files.len(),
                iceberg_snapshot_res.import_result.puffin_blob_ref.len(),
            );
    }

    /// Set iceberg snapshot flush LSN, called after a snapshot operation.
    pub(crate) fn set_iceberg_snapshot_res(&mut self, iceberg_snapshot_res: IcebergSnapshotResult) {
        assert!(
            self.background_task_status_for_validation
                .iceberg_snapshot_ongoing
        );
        self.background_task_status_for_validation
            .iceberg_snapshot_ongoing = false;

        // ---- Update mooncake table fields ----
        let flush_lsn = iceberg_snapshot_res.flush_lsn;
        Self::assert_flush_lsn_on_iceberg_snapshot_res(
            self.last_iceberg_snapshot_lsn,
            &iceberg_snapshot_res,
        );
        self.last_iceberg_snapshot_lsn = Some(flush_lsn);

        if let Some(new_table_schema) = iceberg_snapshot_res.new_table_schema {
            assert!(Arc::ptr_eq(&self.metadata, &new_table_schema));
        }

        assert!(self.iceberg_table_manager.is_none());
        self.iceberg_table_manager = Some(iceberg_snapshot_res.table_manager.unwrap());

        // ---- Buffer iceberg persisted content to next snapshot task ---
        assert!(self.next_snapshot_task.committed_deletion_logs.is_empty());
        self.next_snapshot_task.committed_deletion_logs =
            iceberg_snapshot_res.committed_deletion_logs;

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .flush_lsn
            .is_none());
        self.next_snapshot_task.iceberg_persisted_records.flush_lsn = Some(flush_lsn);

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .import_result
            .is_empty());
        self.next_snapshot_task
            .iceberg_persisted_records
            .import_result = iceberg_snapshot_res.import_result;

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .index_merge_result
            .is_empty());
        self.next_snapshot_task
            .iceberg_persisted_records
            .index_merge_result = iceberg_snapshot_res.index_merge_result;

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .data_compaction_result
            .is_empty());
        self.next_snapshot_task
            .iceberg_persisted_records
            .data_compaction_result = iceberg_snapshot_res.data_compaction_result;
    }

    /// Set file indices merge result, which will be sync-ed to mooncake and iceberg snapshot in the next periodic snapshot iteration.
    pub(crate) fn set_file_indices_merge_res(&mut self, file_indices_res: FileIndiceMergeResult) {
        assert!(
            self.background_task_status_for_validation
                .index_merge_ongoing
        );
        self.background_task_status_for_validation
            .index_merge_ongoing = false;

        // TODO(hjiang): Should be able to use HashSet at beginning so no need to convert.
        assert!(self.next_snapshot_task.index_merge_result.is_empty());
        self.next_snapshot_task.index_merge_result = file_indices_res;
    }

    /// Set data compaction result, which will be sync-ed to mooncake and iceberg snapshot in the next periodic snapshot iteration.
    pub(crate) fn set_data_compaction_res(&mut self, data_compaction_res: DataCompactionResult) {
        assert!(
            self.background_task_status_for_validation
                .data_compaction_ongoing
        );
        self.background_task_status_for_validation
            .data_compaction_ongoing = false;

        assert!(self.next_snapshot_task.data_compaction_result.is_empty());
        self.next_snapshot_task.data_compaction_result = data_compaction_res;
    }

    /// Record index merge completion event.
    pub(crate) fn record_index_merge_completion(&self, file_indices_res: &FileIndiceMergeResult) {
        if let Some(event_replay_tx) = &self.event_replay_tx {
            let table_event =
                replay_events::create_index_merge_event_completion(file_indices_res.uuid);
            event_replay_tx
                .send(MooncakeTableEvent::IndexMergeCompletion(table_event))
                .unwrap();
        }
    }
    /// Record data compaction completion result.
    pub(crate) fn record_data_compaction_completion(
        &self,
        data_compaction_res: &DataCompactionResult,
    ) {
        if let Some(event_replay_tx) = &self.event_replay_tx {
            let table_event = replay_events::create_data_compaction_event_completion(
                data_compaction_res.uuid,
                data_compaction_res
                    .new_data_files
                    .iter()
                    .map(|file| file.0.file_id())
                    .collect(),
            );
            event_replay_tx
                .send(MooncakeTableEvent::DataCompactionCompletion(table_event))
                .unwrap();
        }
    }
    /// Record mooncake snapshot completion result.
    pub(crate) fn record_mooncake_snapshot_completion(
        &self,
        mooncake_snapshot_res: &MooncakeSnapshotOutput,
    ) {
        if let Some(event_replay_tx) = &self.event_replay_tx {
            let table_event = replay_events::create_mooncake_snapshot_event_completion(
                mooncake_snapshot_res.uuid,
                mooncake_snapshot_res.commit_lsn,
            );
            event_replay_tx
                .send(MooncakeTableEvent::MooncakeSnapshotCompletion(table_event))
                .unwrap();
        }
    }
    /// Record iceberg snapshot completion result.
    pub(crate) fn record_iceberg_snapshot_completion(
        &self,
        iceberg_snapshot_res: &IcebergSnapshotResult,
    ) {
        if let Some(event_replay_tx) = &self.event_replay_tx {
            let table_event = replay_events::create_iceberg_snapshot_event_completion(
                iceberg_snapshot_res.uuid,
                iceberg_snapshot_res.flush_lsn,
            );
            event_replay_tx
                .send(MooncakeTableEvent::IcebergSnapshotCompletion(table_event))
                .unwrap();
        }
    }

    /// Get iceberg snapshot flush LSN.
    pub fn get_iceberg_snapshot_lsn(&self) -> Option<u64> {
        self.last_iceberg_snapshot_lsn
    }

    pub(crate) fn get_state_for_reader(
        &self,
    ) -> (Arc<RwLock<SnapshotTableState>>, watch::Receiver<u64>) {
        (
            self.snapshot.clone(),
            self.table_snapshot_watch_receiver.clone(),
        )
    }

    pub fn should_flush(&self) -> bool {
        self.mem_slice.get_num_rows() >= self.metadata.config.mem_slice_size
    }

    /// Drains the current mem slice and prepares a disk slice for flushing.
    /// Adds current mem slice batches and indices to `next_snapshot_task`.
    fn prepare_disk_slice(&mut self, lsn: u64) -> Result<DiskSliceWriter> {
        // Finalize the current batch (if needed)
        let (new_batch, batches, index) = self.mem_slice.drain()?;

        let index = Arc::new(index);
        if let Some(batch) = new_batch {
            self.next_snapshot_task
                .new_record_batches
                .push(RecordBatchWithDeletionVector {
                    batch_id: batch.0,
                    record_batch: batch.1,
                    deletion_vector: None,
                });
        }
        for batch in batches.iter() {
            assert!(
                self.next_snapshot_task
                    .flushing_batch_lsn_map
                    .insert(batch.id, lsn)
                    .is_none(),
                "batch id {} already in flushing_batch_lsn_map",
                batch.id
            );
        }
        self.next_snapshot_task.new_mem_indices.push(index.clone());

        let path = self.metadata.path.clone();
        let next_file_id = self.next_file_id;
        self.next_file_id += 1;

        let disk_slice = DiskSliceWriter::new(
            self.metadata.schema.clone(),
            path,
            batches,
            Some(lsn),
            next_file_id,
            index,
            self.metadata.config.disk_slice_writer_config.clone(),
        );

        Ok(disk_slice)
    }

    /// Flushes the disk slice for the transaction.
    ///
    /// # Arguments
    ///
    /// * ongoing_flush_count: used to increment ongoing flush count for the given LSN.
    fn flush_disk_slice(
        &mut self,
        disk_slice: &mut DiskSliceWriter,
        table_notify_tx: Sender<TableEvent>,
        xact_id: Option<u32>,
        ongoing_flush_count: u32,
        event_id: uuid::Uuid,
    ) {
        if let Some(lsn) = disk_slice.lsn() {
            self.insert_ongoing_flush_lsn(lsn, ongoing_flush_count);
        } else {
            assert!(
                xact_id.is_some(),
                "LSN should be none for non streaming flush"
            );
        }

        let mut disk_slice_clone = disk_slice.clone();
        tokio::task::spawn(async move {
            let flush_result = disk_slice_clone.write().await;
            match flush_result {
                Ok(()) => {
                    table_notify_tx
                        .send(TableEvent::FlushResult {
                            event_id,
                            xact_id,
                            flush_result: Some(Ok(disk_slice_clone)),
                        })
                        .await
                        .unwrap();
                }
                Err(e) => {
                    table_notify_tx
                        .send(TableEvent::FlushResult {
                            event_id,
                            xact_id,
                            flush_result: Some(Err(e)),
                        })
                        .await
                        .unwrap();
                }
            }
        });
    }

    /// Applies the result of a flush to the snapshot task.
    /// Adds the disk slice to `next_snapshot_task`.
    pub fn apply_flush_result(&mut self, disk_slice: DiskSliceWriter, flush_event_id: uuid::Uuid) {
        // Record events for flush completion.
        if let Some(event_replay_tx) = &self.event_replay_tx {
            let table_event = replay_events::create_flush_event_completion(
                flush_event_id,
                disk_slice
                    .output_files()
                    .iter()
                    .map(|(file, _)| file.file_id)
                    .collect(),
            );
            event_replay_tx
                .send(MooncakeTableEvent::FlushCompletion(table_event))
                .unwrap();
        }

        // Perform table flush completion notification.
        let lsn = disk_slice
            .lsn()
            .expect("LSN should never be none for non streaming flush");
        self.remove_ongoing_flush_lsn(lsn);
        self.try_set_next_flush_lsn(lsn);
        self.next_snapshot_task.new_disk_slices.push(disk_slice);
    }

    // Attempts to set the flush LSN for the next iceberg snapshot. Note that we can only set the flush LSN if it's less than the current min pending flush LSN. Otherwise, LSNs will be persisted to iceberg in the wrong order.
    fn try_set_next_flush_lsn(&mut self, lsn: u64) {
        self.next_snapshot_task.try_set_largest_flush_lsn(lsn);
        let min_pending_lsn = self.get_min_ongoing_flush_lsn();
        if lsn < min_pending_lsn {
            if let Some(old_flush_lsn) = self.next_snapshot_task.new_flush_lsn {
                ma::assert_le!(old_flush_lsn, lsn);
            }
            self.next_snapshot_task.new_flush_lsn = Some(lsn);
        } else {
            // It's possible to have multiple flushes with the same LSN.
            self.completed_unrecorded_flush_lsns.insert(lsn);
        }

        // Check whether we need to add completed unrecorded flush LSNs into next snapshot.
        if self.ongoing_flush_lsns.is_empty() {
            if let Some(largest_lsn) = self.completed_unrecorded_flush_lsns.last() {
                // Till this point, flush LSN is already set.
                if *largest_lsn > self.next_snapshot_task.new_flush_lsn.unwrap() {
                    self.next_snapshot_task.new_flush_lsn = Some(*largest_lsn);
                }
            }
            self.completed_unrecorded_flush_lsns.clear();
        }
    }

    // We fallback to u64::MAX if there are no pending flush LSNs so that the LSN is always greater than the flush LSN and the iceberg snapshot can proceed.
    pub fn get_min_ongoing_flush_lsn(&self) -> u64 {
        if let Some((lsn, _)) = self.ongoing_flush_lsns.first_key_value() {
            return *lsn;
        }
        u64::MAX
    }

    pub fn insert_ongoing_flush_lsn(&mut self, lsn: u64, count: u32) {
        *self.ongoing_flush_lsns.entry(lsn).or_insert(0) += count;
    }

    pub fn remove_ongoing_flush_lsn(&mut self, lsn: u64) {
        use std::collections::btree_map::Entry;

        match self.ongoing_flush_lsns.entry(lsn) {
            Entry::Occupied(mut entry) => {
                let counter = entry.get_mut();
                if *counter > 1 {
                    *counter -= 1;
                } else {
                    entry.remove();
                }
            }
            Entry::Vacant(_) => {
                panic!("Tried to remove LSN {lsn}, but it is not tracked");
            }
        }
    }

    pub fn has_ongoing_flush(&self) -> bool {
        !self.ongoing_flush_lsns.is_empty()
    }

    // Create a snapshot of the last committed version, return current snapshot's version and payload to perform iceberg snapshot.
    fn create_snapshot_impl(&mut self, opt: SnapshotOption) {
        // Record mooncake snapshot event initiation.
        if let Some(event_replay_tx) = &self.event_replay_tx {
            let table_event =
                replay_events::create_mooncake_snapshot_event_initiation(opt.uuid, opt.clone());
            event_replay_tx
                .send(MooncakeTableEvent::MooncakeSnapshotInitiation(table_event))
                .unwrap();
        }

        // Check invariant: there should be at most one ongoing mooncake snapshot.
        assert!(
            !self
                .background_task_status_for_validation
                .mooncake_snapshot_ongoing
        );
        self.background_task_status_for_validation
            .mooncake_snapshot_ongoing = true;

        self.next_snapshot_task.new_rows = Some(self.mem_slice.get_latest_rows());
        let mut next_snapshot_task = std::mem::take(&mut self.next_snapshot_task);

        // Re-initialize mooncake table fields.
        self.next_snapshot_task = SnapshotTask::new(self.metadata.config.clone());
        // Carry forward the commit baseline
        // This is important if we have a pending flush that will be added to the next snapshot task.
        // Otherwise, if we simply reset the `commit_lsn_baseline` to zero, the ongoing flush will finish and set `flush_lsn`. Then we have a snapshot task where `commit_lsn` < `flush_lsn` which breaks our invariant.
        self.next_snapshot_task.commit_lsn_baseline = next_snapshot_task.commit_lsn_baseline;
        self.next_snapshot_task.prev_commit_lsn_baseline = next_snapshot_task.commit_lsn_baseline;

        let cur_snapshot = self.snapshot.clone();

        let min_ongoing_flush_lsn = self.get_min_ongoing_flush_lsn();
        next_snapshot_task.min_ongoing_flush_lsn = min_ongoing_flush_lsn;
        // Create a detached task, whose completion will be notified separately.
        tokio::task::spawn(
            Self::create_snapshot_async(
                cur_snapshot,
                next_snapshot_task,
                opt,
                self.table_notify.as_ref().unwrap().clone(),
            )
            .instrument(info_span!("create_snapshot_async")),
        );
    }

    /// Notify mooncake snapshot as completed.
    pub fn mark_mooncake_snapshot_completed(&mut self) {
        assert!(
            self.background_task_status_for_validation
                .mooncake_snapshot_ongoing
        );
        self.background_task_status_for_validation
            .mooncake_snapshot_ongoing = false;
    }

    /// Mark next iceberg snapshot as force, even if the payload is empty.
    pub(crate) fn force_empty_iceberg_payload(&mut self) {
        self.next_snapshot_task.force_empty_iceberg_payload = true;
    }

    pub(crate) fn notify_snapshot_reader(&self, lsn: u64) {
        self.table_snapshot_watch_sender.send(lsn).unwrap();
    }

    /// Drop a mooncake table.
    pub(crate) async fn drop_mooncake_table(&mut self) -> Result<()> {
        tokio::fs::remove_dir_all(&self.metadata.path).await?;
        Ok(())
    }

    /// Drop an iceberg table.
    pub(crate) async fn drop_iceberg_table(&mut self) -> Result<()> {
        assert!(self.iceberg_table_manager.is_some());
        self.iceberg_table_manager
            .as_mut()
            .unwrap()
            .drop_table()
            .await?;
        Ok(())
    }

    /// Uses the latest wal metadata and iceberg LSN from the latest iceberg snapshot
    /// to determine the files to truncate.
    ///
    /// # Arguments
    ///
    /// * uuid: WAL persistence event unique id.
    #[must_use]
    pub(crate) fn do_wal_persistence_update(&mut self, uuid: uuid::Uuid) -> bool {
        let latest_iceberg_snapshot_lsn = self.get_iceberg_snapshot_lsn();

        let wal_persistence_update_result = self
            .wal_manager
            .prepare_persistent_update(latest_iceberg_snapshot_lsn);

        if wal_persistence_update_result.should_do_persistence() {
            let event_sender_clone = self.table_notify.as_ref().unwrap().clone();
            let file_system_accessor = self.wal_manager.get_file_system_accessor();
            tokio::spawn(async move {
                WalManager::wal_persist_truncate_async(
                    uuid,
                    wal_persistence_update_result,
                    file_system_accessor,
                    event_sender_clone,
                )
                .await;
            });
            true
        } else {
            false
        }
    }

    /// Handles the result of a persist and truncate operation.
    /// Returns the highest LSN that has been persisted into WAL.
    pub(crate) fn handle_completed_wal_persistence_update(
        &mut self,
        result: &WalPersistenceUpdateResult,
    ) -> Option<u64> {
        self.wal_manager
            .handle_complete_wal_persistence_update(result)
    }

    /// Drop the WAL files. Note that at the moment this drops WAL files in the mooncake table's local filesystem
    /// and so behaves the same as MooncakeTable.drop_table(),
    /// but in the future this is needed to support object storage
    pub(crate) async fn drop_wal(&mut self) -> Result<()> {
        self.wal_manager.drop_wal().await
    }

    pub fn push_wal_event(&mut self, event: &TableEvent) {
        self.wal_manager.push(event);
    }

    pub fn get_wal_highest_completion_lsn(&self) -> u64 {
        self.wal_manager.get_highest_completion_lsn()
    }

    pub fn get_wal_curr_file_number(&self) -> u64 {
        self.wal_manager.get_curr_file_number()
    }

    /// Shutdown the current table, which unpins all referenced data files in the global data file.
    pub async fn shutdown(&mut self) -> Result<()> {
        let evicted_files_to_delete = {
            let mut guard = self.snapshot.write().await;
            guard.unreference_and_delete_all_cache_handles().await
        };

        for cur_file in evicted_files_to_delete.into_iter() {
            tokio::fs::remove_file(cur_file).await?;
        }

        Ok(())
    }

    /// =======================
    /// Table state updates
    /// =======================
    ///
    /// The following events contains updates to the mooncake table, which will be recorded into event replay system if enabled.
    pub fn append(&mut self, row: MoonlinkRow) -> Result<()> {
        // Record events for replay.
        if let Some(event_replay_tx) = &self.event_replay_tx {
            let table_event =
                replay_events::create_append_event(row.clone(), /*xact_id=*/ None);
            event_replay_tx
                .send(MooncakeTableEvent::Append(table_event))
                .unwrap();
        }

        // Perform append operation.
        let lookup_key = self.metadata.config.row_identity.get_lookup_key(&row);
        let identity_for_key = self
            .metadata
            .config
            .row_identity
            .extract_identity_for_key(&row);
        if let Some(batch) = self.mem_slice.append(lookup_key, row, identity_for_key)? {
            self.next_snapshot_task
                .new_record_batches
                .push(RecordBatchWithDeletionVector {
                    batch_id: batch.0,
                    record_batch: batch.1,
                    deletion_vector: None,
                });
        }
        Ok(())
    }

    async fn delete_impl(&mut self, row: MoonlinkRow, lsn: u64, delete_if_exists: bool) {
        // Check if this is an append-only table
        if matches!(self.metadata.config.row_identity, IdentityProp::None) {
            tracing::error!("Delete operation not supported for append-only tables");
            return;
        }

        // Record events for replay.
        if let Some(event_replay_tx) = &self.event_replay_tx {
            let table_event = replay_events::create_delete_event(
                row.clone(),
                /*lsn=*/ Some(lsn),
                /*xact_id=*/ None,
            );
            event_replay_tx
                .send(MooncakeTableEvent::Delete(table_event))
                .unwrap();
        }

        // Perform delete operation.
        let lookup_key = self.metadata.config.row_identity.get_lookup_key(&row);
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn,
            pos: None,
            row_identity: self
                .metadata
                .config
                .row_identity
                .extract_identity_columns(row),
            delete_if_exists,
        };
        let pos = self
            .mem_slice
            .delete(&record, &self.metadata.config.row_identity)
            .await;
        record.pos = pos;
        self.next_snapshot_task.new_deletions.push(record);
    }

    pub async fn delete(&mut self, row: MoonlinkRow, lsn: u64) {
        self.delete_impl(row, lsn, /*delete_if_exists=*/ false)
            .await;
    }

    pub async fn delete_if_exists(&mut self, row: MoonlinkRow, lsn: u64) {
        self.delete_impl(row, lsn, /*delete_if_exists=*/ true).await;
    }

    pub fn commit(&mut self, lsn: u64) {
        // Record events for commit.
        if let Some(event_replay_tx) = &self.event_replay_tx {
            let table_event =
                replay_events::create_commit_event(/*lsn=*/ lsn, /*xact_id=*/ None);
            event_replay_tx
                .send(MooncakeTableEvent::Commit(table_event))
                .unwrap();
        }

        // Perform commit operation.
        ma::assert_ge!(lsn, self.next_snapshot_task.commit_lsn_baseline);
        self.next_snapshot_task.commit_lsn_baseline = lsn;
        self.next_snapshot_task.new_commit_point = Some(self.mem_slice.get_commit_check_point());
        assert!(
            self.next_snapshot_task.new_deletions.is_empty()
                || self.next_snapshot_task.new_deletions.last().unwrap().lsn >= transaction_stream::LSN_START_FOR_STREAMING_XACT
                || self.next_snapshot_task.new_deletions.last().unwrap().lsn < lsn,
            "We expect commit LSN to be strictly greater than the last deletion LSN, but got commit LSN {} and last deletion LSN {}",
            lsn,
            self.next_snapshot_task.new_deletions.last().unwrap().lsn,
        );
    }

    /// Drains the current mem slice and create a disk slice.
    /// Flushes the disk slice.
    /// Adds the disk slice to `next_snapshot_task`.
    pub fn flush(&mut self, lsn: u64, event_id: uuid::Uuid) -> Result<()> {
        // Sanity check flush LSN doesn't regress.
        assert!(
            self.next_snapshot_task.new_flush_lsn.is_none()
                || self.next_snapshot_task.new_flush_lsn.unwrap() <= lsn,
            "Current flush LSN is {:?}, new flush LSN is {}",
            self.next_snapshot_task.new_flush_lsn,
            lsn,
        );

        let table_notify_tx = self.table_notify.as_ref().unwrap().clone();
        if self.mem_slice.is_empty() || self.ongoing_flush_lsns.contains_key(&lsn) {
            self.try_set_next_flush_lsn(lsn);
            tokio::task::spawn(async move {
                table_notify_tx
                    .send(TableEvent::FlushResult {
                        event_id,
                        xact_id: None,
                        flush_result: None,
                    })
                    .await
                    .unwrap();
            });
            return Ok(());
        }

        // Record events for flush initialization.
        if let Some(event_replay_tx) = &self.event_replay_tx {
            let table_event = replay_events::create_flush_event_initiation(
                event_id,
                /*xact_id=*/ None,
                Some(lsn),
                self.mem_slice.get_commit_check_point(),
            );
            event_replay_tx
                .send(MooncakeTableEvent::FlushInitiation(table_event))
                .unwrap();
        }

        let mut disk_slice = self.prepare_disk_slice(lsn)?;
        self.flush_disk_slice(
            &mut disk_slice,
            table_notify_tx,
            /*xact_id=*/ None,
            /*ongoing_flush_count=*/ 1,
            event_id,
        );

        Ok(())
    }

    /// Perform index merge, whose completion will be notified separately in async style.
    pub(crate) fn perform_index_merge(
        &mut self,
        file_indice_merge_payload: FileIndiceMergePayload,
    ) {
        assert!(
            !self
                .background_task_status_for_validation
                .index_merge_ongoing
        );
        self.background_task_status_for_validation
            .index_merge_ongoing = true;

        // Record index merge event initiation.
        let table_event_id = file_indice_merge_payload.uuid;
        if let Some(event_replay_tx) = &self.event_replay_tx {
            let table_event = replay_events::create_index_merge_event_initiation(
                table_event_id,
                &file_indice_merge_payload,
            );
            event_replay_tx
                .send(MooncakeTableEvent::IndexMergeInitiation(table_event))
                .unwrap();
        }

        // Perform index merge operation.
        let cur_file_id = self.next_file_id as u64;
        self.next_file_id += 1;
        let table_directory = std::path::PathBuf::from(self.metadata.path.to_str().unwrap());
        let table_notify_tx_copy = self.table_notify.as_ref().unwrap().clone();

        // Create a detached task, whose completion will be notified separately.
        tokio::task::spawn(async move {
            let result: Result<()> = async move {
                let mut builder = GlobalIndexBuilder::new();
                builder.set_directory(table_directory);
                let merged = builder
                    .build_from_merge(file_indice_merge_payload.file_indices.clone(), cur_file_id)
                    .await;

                match merged {
                    Ok(merged) => {
                        let index_merge_result = FileIndiceMergeResult {
                            uuid: file_indice_merge_payload.uuid,
                            old_file_indices: file_indice_merge_payload.file_indices,
                            new_file_indices: vec![merged],
                        };

                        // Send back completion notification to table handler.
                        table_notify_tx_copy
                            .send(TableEvent::IndexMergeResult {
                                index_merge_result: Ok(index_merge_result),
                            })
                            .await
                            .unwrap();
                    }
                    Err(e) => {
                        table_notify_tx_copy
                            .send(TableEvent::IndexMergeResult {
                                index_merge_result: Err(e),
                            })
                            .await
                            .unwrap();
                    }
                }
                Ok(())
            }
            .await;

            if let Err(e) = result {
                tracing::error!("Index merge task failed: {}", e);
            }
        });
    }

    /// Perform data compaction, whose completion will be notified separately in async style.
    pub(crate) fn perform_data_compaction(&mut self, compaction_payload: DataCompactionPayload) {
        assert!(
            !self
                .background_task_status_for_validation
                .data_compaction_ongoing
        );
        self.background_task_status_for_validation
            .data_compaction_ongoing = true;

        // Record index merge event initiation.
        let table_event_id = compaction_payload.uuid;
        if let Some(event_replay_tx) = &self.event_replay_tx {
            let table_event = replay_events::create_data_compaction_event_initiation(
                table_event_id,
                &compaction_payload,
            );
            event_replay_tx
                .send(MooncakeTableEvent::DataCompactionInitiation(table_event))
                .unwrap();
        }

        // Perform data compaction operation.
        let data_compaction_new_file_ids =
            compaction_payload.get_new_compacted_data_file_ids_number();
        let table_auto_incr_ids =
            self.next_file_id..(self.next_file_id + data_compaction_new_file_ids);
        self.next_file_id += data_compaction_new_file_ids;
        let file_params = CompactionFileParams {
            dir_path: self.metadata.path.clone(),
            table_auto_incr_ids,
            data_file_final_size: self
                .metadata
                .config
                .data_compaction_config
                .data_file_final_size,
        };
        let schema_ref = self.metadata.schema.clone();
        let table_notify_tx_copy = self.table_notify.as_ref().unwrap().clone();

        // Record data files being compacted.
        assert!(self.next_snapshot_task.compacting_data_files.is_empty());
        self.next_snapshot_task.compacting_data_files = compaction_payload.get_data_files();

        // Create a detached task, whose completion will be notified separately.
        tokio::task::spawn(
            async move {
                let builder = CompactionBuilder::new(compaction_payload, schema_ref, file_params);
                let data_compaction_result = builder.build().await;
                table_notify_tx_copy
                    .send(TableEvent::DataCompactionResult {
                        data_compaction_result,
                    })
                    .await
                    .unwrap();
            }
            .instrument(info_span!("data_compaction")),
        );
    }

    /// Attempts to create a mooncake snapshot.
    /// If a mooncake snapshot is not going to be created, return false immediately.
    #[must_use]
    pub fn try_create_mooncake_snapshot(&mut self, opt: SnapshotOption) -> bool {
        if !self.next_snapshot_task.should_create_snapshot() && !opt.force_create {
            return false;
        }
        self.create_snapshot_impl(opt);
        true
    }

    async fn create_snapshot_async(
        snapshot: Arc<RwLock<SnapshotTableState>>,
        next_snapshot_task: SnapshotTask,
        opt: SnapshotOption,
        table_notify: Sender<TableEvent>,
    ) {
        let mooncake_snapshot_result = snapshot
            .write()
            .await
            .update_snapshot(next_snapshot_task, opt)
            .await;

        // Send back completion notification to table handler.
        table_notify
            .send(TableEvent::MooncakeTableSnapshotResult {
                mooncake_snapshot_result,
            })
            .await
            .unwrap();
    }

    /// Persist an iceberg snapshot.
    async fn persist_iceberg_snapshot_impl(
        mut iceberg_table_manager: Box<dyn TableManager>,
        snapshot_payload: IcebergSnapshotPayload,
        table_notify: Sender<TableEvent>,
        table_auto_incr_ids: std::ops::Range<u32>,
        event_replay_tx: Option<mpsc::UnboundedSender<MooncakeTableEvent>>,
        table_event_id: uuid::Uuid,
    ) {
        // Record index merge event initiation.
        if let Some(event_replay_tx) = &event_replay_tx {
            let table_event = replay_events::create_iceberg_snapshot_event_initiation(
                table_event_id,
                &snapshot_payload,
            );
            event_replay_tx
                .send(MooncakeTableEvent::IcebergSnapshotInitiation(Box::new(
                    table_event,
                )))
                .unwrap();
        }

        // Perform iceberg snapshot operation.
        let flush_lsn = snapshot_payload.flush_lsn;
        let new_table_schema = snapshot_payload.new_table_schema.clone();
        let committed_deletion_logs = snapshot_payload.committed_deletion_logs.clone();

        let new_imported_data_files_count = snapshot_payload.import_payload.data_files.len();
        let new_compacted_data_files_count = snapshot_payload
            .data_compaction_payload
            .new_data_files_to_import
            .len();

        let new_new_file_indices_count = snapshot_payload.import_payload.file_indices.len();
        let new_merged_file_indices_count = snapshot_payload
            .index_merge_payload
            .new_file_indices_to_import
            .len();
        let new_compacted_file_indices_count = snapshot_payload
            .data_compaction_payload
            .new_file_indices_to_import
            .len();

        let old_file_indices_to_remove_by_index_merge = snapshot_payload
            .index_merge_payload
            .old_file_indices_to_remove
            .clone();
        let old_data_files_to_remove_by_compaction = snapshot_payload
            .data_compaction_payload
            .old_data_files_to_remove
            .clone();
        let old_file_indices_to_remove_by_compaction = snapshot_payload
            .data_compaction_payload
            .old_file_indices_to_remove
            .clone();
        let data_file_record_remap_by_compaction = snapshot_payload
            .data_compaction_payload
            .data_file_records_remap
            .clone();

        let persistence_file_params = PersistenceFileParams {
            table_auto_incr_ids,
        };

        let iceberg_persistence_res = iceberg_table_manager
            .sync_snapshot(snapshot_payload, persistence_file_params)
            .await;

        // Notify on event error.
        if iceberg_persistence_res.is_err() {
            table_notify
                .send(TableEvent::IcebergSnapshotResult {
                    iceberg_snapshot_result: Err(iceberg_persistence_res.unwrap_err()),
                })
                .await
                .unwrap();
            return;
        }

        // Notify on event success.
        let iceberg_persistence_res = iceberg_persistence_res.unwrap();

        // Persisted data files and file indices will be cut into multiple sections: imported part, index merge part, data compaction part.
        // Get the cut-off indices to slice from returned iceberg persistence result.
        let new_data_files_cutoff_index_1 = new_imported_data_files_count;
        let new_data_files_cutoff_index_2 =
            new_data_files_cutoff_index_1 + new_compacted_data_files_count;
        assert_eq!(
            new_data_files_cutoff_index_2,
            iceberg_persistence_res.remote_data_files.len()
        );

        let new_file_indices_cutoff_index_1 = new_new_file_indices_count;
        let new_file_indices_cutoff_index_2 =
            new_file_indices_cutoff_index_1 + new_merged_file_indices_count;
        let new_file_indices_cutoff_index_3 =
            new_file_indices_cutoff_index_2 + new_compacted_file_indices_count;
        assert_eq!(
            new_file_indices_cutoff_index_3,
            iceberg_persistence_res.remote_file_indices.len()
        );

        let snapshot_result = IcebergSnapshotResult {
            uuid: table_event_id,
            table_manager: Some(iceberg_table_manager),
            flush_lsn,
            new_table_schema,
            committed_deletion_logs,
            import_result: IcebergSnapshotImportResult {
                new_data_files: iceberg_persistence_res.remote_data_files
                    [0..new_data_files_cutoff_index_1]
                    .to_vec(),
                puffin_blob_ref: iceberg_persistence_res.puffin_blob_ref,
                new_file_indices: iceberg_persistence_res.remote_file_indices
                    [0..new_file_indices_cutoff_index_1]
                    .to_vec(),
            },
            index_merge_result: IcebergSnapshotIndexMergeResult {
                new_file_indices_imported: iceberg_persistence_res.remote_file_indices
                    [new_file_indices_cutoff_index_1..new_file_indices_cutoff_index_2]
                    .to_vec(),
                old_file_indices_removed: old_file_indices_to_remove_by_index_merge,
            },
            data_compaction_result: IcebergSnapshotDataCompactionResult {
                new_data_files_imported: iceberg_persistence_res.remote_data_files
                    [new_data_files_cutoff_index_1..new_data_files_cutoff_index_2]
                    .to_vec(),
                old_data_files_removed: old_data_files_to_remove_by_compaction,
                new_file_indices_imported: iceberg_persistence_res.remote_file_indices
                    [new_file_indices_cutoff_index_2..new_file_indices_cutoff_index_3]
                    .to_vec(),
                old_file_indices_removed: old_file_indices_to_remove_by_compaction,
                data_file_records_remap: data_file_record_remap_by_compaction,
            },
            evicted_files_to_delete: iceberg_persistence_res.evicted_files_to_delete,
        };

        // Send back completion notification to table handler.
        table_notify
            .send(TableEvent::IcebergSnapshotResult {
                iceberg_snapshot_result: Ok(snapshot_result),
            })
            .await
            .unwrap();
    }

    /// Create an iceberg snapshot.
    pub(crate) fn persist_iceberg_snapshot(&mut self, snapshot_payload: IcebergSnapshotPayload) {
        // Check invariant: there's at most one ongoing iceberg snapshot.
        let iceberg_table_manager = self.iceberg_table_manager.take().unwrap();
        assert!(
            !self
                .background_task_status_for_validation
                .iceberg_snapshot_ongoing
        );
        self.background_task_status_for_validation
            .iceberg_snapshot_ongoing = true;

        // Create a detached task, whose completion will be notified separately.
        let new_file_ids_to_create = snapshot_payload.get_new_file_ids_num();
        let table_auto_incr_ids = self.next_file_id..(self.next_file_id + new_file_ids_to_create);
        self.next_file_id += new_file_ids_to_create;
        let table_event_id = snapshot_payload.uuid;
        tokio::task::spawn(
            Self::persist_iceberg_snapshot_impl(
                iceberg_table_manager,
                snapshot_payload,
                self.table_notify.as_ref().unwrap().clone(),
                table_auto_incr_ids,
                self.event_replay_tx.clone(),
                table_event_id,
            )
            .instrument(info_span!("persist_iceberg_snapshot")),
        );
    }
}

#[cfg(test)]
mod mooncake_tests {
    use super::*;
    use crate::storage::storage_utils::create_data_file;

    #[test]
    fn test_flush_lsn_assertion() {
        // Only iceberg imported result.
        let iceberg_snapshot_result = IcebergSnapshotResult {
            uuid: uuid::Uuid::new_v4(),
            table_manager: None,
            flush_lsn: 1,
            new_table_schema: None,
            committed_deletion_logs: HashSet::new(),
            import_result: IcebergSnapshotImportResult {
                new_data_files: vec![create_data_file(
                    /*file_id=*/ 0,
                    "file_path".to_string(),
                )],
                puffin_blob_ref: HashMap::new(),
                new_file_indices: vec![],
            },
            index_merge_result: IcebergSnapshotIndexMergeResult::default(),
            data_compaction_result: IcebergSnapshotDataCompactionResult::default(),
            evicted_files_to_delete: Vec::new(),
        };
        // Valid snapshot result.
        MooncakeTable::assert_flush_lsn_on_iceberg_snapshot_res(
            /*persistence_lsn=*/ None,
            &iceberg_snapshot_result,
        );
        // Invalid snapshot result.
        let res_copy = iceberg_snapshot_result.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            MooncakeTable::assert_flush_lsn_on_iceberg_snapshot_res(Some(2), &res_copy);
        }));
        assert!(result.is_err());

        // Only data compaction result.
        let mut res_copy = iceberg_snapshot_result.clone();
        res_copy.import_result = IcebergSnapshotImportResult::default();
        res_copy.data_compaction_result = IcebergSnapshotDataCompactionResult {
            old_data_files_removed: vec![create_data_file(
                /*file_id=*/ 0,
                "file_path".to_string(),
            )],
            ..Default::default()
        };
        // Valid snapshot result.
        MooncakeTable::assert_flush_lsn_on_iceberg_snapshot_res(
            /*persistence_lsn=*/ Some(1),
            &res_copy,
        );
        // Invalid snapshot result.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            MooncakeTable::assert_flush_lsn_on_iceberg_snapshot_res(Some(2), &res_copy);
        }));
        assert!(result.is_err());

        // Contain both import and data compaction result.
        let mut res_copy = iceberg_snapshot_result.clone();
        res_copy.data_compaction_result = IcebergSnapshotDataCompactionResult {
            old_data_files_removed: vec![create_data_file(
                /*file_id=*/ 0,
                "file_path".to_string(),
            )],
            ..Default::default()
        };
        // Valid snapshot result.
        MooncakeTable::assert_flush_lsn_on_iceberg_snapshot_res(
            /*persistence_lsn=*/ Some(1),
            &res_copy,
        );
    }
}

#[cfg(test)]
impl MooncakeTable {
    pub fn get_table_id(&self) -> u32 {
        self.metadata.table_id
    }

    pub(crate) fn get_snapshot_watch_sender(&self) -> watch::Sender<u64> {
        self.table_snapshot_watch_sender.clone()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_utils;

#[cfg(test)]
mod data_file_state_tests;

#[cfg(test)]
mod deletion_vector_puffin_state_tests;

#[cfg(test)]
mod file_index_state_tests;

#[cfg(test)]
pub(crate) mod table_accessor_test_utils;

#[cfg(test)]
pub(crate) mod table_creation_test_utils;

#[cfg(test)]
pub(crate) mod validation_test_utils;

#[cfg(test)]
pub(crate) mod table_operation_test_utils;

#[cfg(test)]
pub(crate) mod test_utils_commons;

#[cfg(test)]
pub(crate) mod cache_test_utils;
