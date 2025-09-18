use crate::row::IdentityProp;
/// This module contain tests which are not covered by state-machine based test, including complex operations, object storage based tests, etc.
use crate::row::MoonlinkRow;
use crate::row::RowValue;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::gcs_test_utils;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::test_guard::TestGuard as GcsTestGuard;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::s3_test_utils;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::test_guard::TestGuard as S3TestGuard;
use crate::storage::iceberg::data_file_manifest_manager::DEFAULT_MAX_MANIFEST_ENTRY_COUNT;
use crate::storage::iceberg::file_catalog::METADATA_DIRECTORY;
use crate::storage::iceberg::file_catalog::VERSION_HINT_FILENAME;
use crate::storage::iceberg::iceberg_table_config::IcebergTableConfig;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::iceberg::schema_utils::*;
use crate::storage::iceberg::table_manager::PersistenceFileParams;
use crate::storage::iceberg::table_manager::TableManager;
use crate::storage::iceberg::test_utils::*;
use crate::storage::index::index_merge_config::FileIndexMergeConfig;
use crate::storage::index::persisted_bucket_hash_map::GlobalIndex;
use crate::storage::index::MooncakeIndex;
use crate::storage::io_utils;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::storage::mooncake_table::test_utils_commons::ICEBERG_TEST_NAMESPACE;
use crate::storage::mooncake_table::test_utils_commons::ICEBERG_TEST_TABLE;
use crate::storage::mooncake_table::validation_test_utils::*;
use crate::storage::mooncake_table::DataCompactionResult;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::IcebergSnapshotResult;
use crate::storage::mooncake_table::{
    IcebergSnapshotDataCompactionPayload, IcebergSnapshotImportPayload,
    IcebergSnapshotIndexMergePayload,
};
use crate::storage::mooncake_table_config::DiskSliceWriterConfig;
use crate::storage::mooncake_table_config::IcebergPersistenceConfig;
use crate::storage::mooncake_table_config::MooncakeTableConfig;
use crate::storage::snapshot_options::IcebergSnapshotOption;
use crate::storage::snapshot_options::MaintenanceOption;
use crate::storage::snapshot_options::SnapshotOption;
use crate::storage::storage_utils;
use crate::storage::storage_utils::create_data_file;
use crate::storage::storage_utils::FileId;
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::storage::wal::test_utils::WAL_TEST_TABLE_ID;
use crate::storage::MooncakeTable;
use crate::DataCompactionConfig;
use crate::FileSystemAccessor;
use crate::ObjectStorageCache;
use crate::ObjectStorageCacheConfig;
use crate::TableEvent;
use crate::WalConfig;
use crate::WalManager;
use futures::{stream, StreamExt};

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::NamespaceIdent;
use iceberg::TableIdent;
use parquet::arrow::AsyncArrowWriter;
use tempfile::tempdir;
use tokio::sync::mpsc;

/// ================================
/// Test utils
/// ================================
///
/// Create test batch deletion vector.
fn test_committed_deletion_log_1(
    data_file: MooncakeDataFileRef,
) -> HashMap<MooncakeDataFileRef, BatchDeletionVector> {
    let mut deletion_vector = BatchDeletionVector::new(/*max_size=*/ 3);
    assert!(deletion_vector.delete_row(0));

    HashMap::<MooncakeDataFileRef, BatchDeletionVector>::from([(data_file, deletion_vector)])
}
/// Corresponds to [`test_committed_deletion_log_1`].
fn test_committed_deletion_logs_to_persist_1(
    data_file: MooncakeDataFileRef,
) -> HashSet<(FileId, usize)> {
    let mut committed_deletion_logs = HashSet::new();
    committed_deletion_logs.insert((data_file.file_id(), /*row_idx=*/ 0));
    committed_deletion_logs
}
fn test_committed_deletion_log_2(
    data_file: MooncakeDataFileRef,
) -> HashMap<MooncakeDataFileRef, BatchDeletionVector> {
    let mut deletion_vector = BatchDeletionVector::new(/*max_size=*/ 3);
    assert!(deletion_vector.delete_row(1));
    assert!(deletion_vector.delete_row(2));

    HashMap::<MooncakeDataFileRef, BatchDeletionVector>::from([(
        data_file.clone(),
        deletion_vector,
    )])
}
/// Corresponds to [`test_committed_deletion_log_2`].
fn test_committed_deletion_logs_to_persist_2(
    data_file: MooncakeDataFileRef,
) -> HashSet<(FileId, usize)> {
    let mut committed_deletion_logs = HashSet::new();
    committed_deletion_logs.insert((data_file.file_id(), /*row_idx=*/ 1));
    committed_deletion_logs.insert((data_file.file_id(), /*row_idx=*/ 2));
    committed_deletion_logs
}

/// Test util function to create file indices.
/// NOTICE: The util function does write index block file.
fn create_file_index(data_files: Vec<MooncakeDataFileRef>) -> GlobalIndex {
    GlobalIndex {
        files: data_files,
        num_rows: 0,
        hash_bits: 0,
        hash_upper_bits: 0,
        hash_lower_bits: 0,
        seg_id_bits: 0,
        row_id_bits: 0,
        bucket_bits: 0,
        index_blocks: vec![],
    }
}

/// Test util functions to create moonlink rows.
fn test_row_1() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(10),
    ])
}
fn test_row_2() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(20),
    ])
}
fn test_row_3() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ])
}
fn test_row_4() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(4),
        RowValue::ByteArray("David".as_bytes().to_vec()),
        RowValue::Int32(40),
    ])
}

/// Test util function to create moonlink row with updated schema with [`create_test_updated_arrow_schema`].
fn test_row_with_updated_schema() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(100),
        RowValue::ByteArray("new_string".as_bytes().to_vec()),
    ])
}

/// Test util function to get filename without suffix.
fn get_filename_without_suffix(filepath: &str) -> String {
    let filename_without_suffix = std::path::Path::new(filepath)
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    filename_without_suffix
}

/// Test util function to write arrow record batch into local file.
async fn write_arrow_record_batch_to_local<P: AsRef<std::path::Path>>(
    path: P,
    schema: Arc<ArrowSchema>,
    batch: &RecordBatch,
) {
    let file = tokio::fs::File::create(&path).await.unwrap();
    let mut writer = AsyncArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(batch).await.unwrap();
    writer.close().await.unwrap();
}

/// Test util to get file indices filepaths and their corresponding data filepaths.
fn get_file_indices_filepath_and_data_filepaths(
    mooncake_index: &MooncakeIndex,
) -> (
    Vec<String>, /*file indices filepath*/
    Vec<String>, /*data filepaths*/
) {
    let file_indices = &mooncake_index.file_indices;

    let mut data_files: Vec<String> = vec![];
    let mut index_files: Vec<String> = vec![];
    for cur_file_index in file_indices.iter() {
        data_files.extend(
            cur_file_index
                .files
                .iter()
                .map(|cur_file| cur_file.file_path().clone())
                .collect::<Vec<_>>(),
        );
        index_files.extend(
            cur_file_index
                .index_blocks
                .iter()
                .map(|cur_index_block| cur_index_block.index_file.file_path().to_string())
                .collect::<Vec<_>>(),
        );
    }

    (data_files, index_files)
}

/// ================================
/// Test multiple recoveries
/// ================================
///
/// Testing scenario: iceberg snapshot should be loaded only once at recovery, otherwise it panics.
#[tokio::test]
async fn test_snapshot_load_for_multiple_times() {
    let tmp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&tmp_dir);
    let mooncake_table_metadata =
        create_test_table_metadata(tmp_dir.path().to_str().unwrap().to_string());
    let config = create_iceberg_table_config(tmp_dir.path().to_str().unwrap().to_string());
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&config),
        config.clone(),
    )
    .await
    .unwrap();

    iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();

    // Use spawn_blocking to avoid "cannot start runtime from within runtime" error
    let result = tokio::task::spawn_blocking(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                iceberg_table_manager
                    .load_snapshot_from_table()
                    .await
                    .unwrap();
            });
        }))
    })
    .await
    .unwrap();

    assert!(result.is_err());
}

/// ================================
/// Test skip iceberg snapshot
/// ================================
///
/// Test scenario: iceberg snapshot is requested to skip when creating mooncake snapshot.
#[tokio::test]
async fn test_skip_iceberg_snapshot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();
    let warehouse_uri = path.clone().to_str().unwrap().to_string();

    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);
    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &path);
    let wal_manager = WalManager::new(&wal_config);
    let schema = create_test_arrow_schema();
    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*table_id=*/ 1,
        path,
        iceberg_table_config.clone(),
        MooncakeTableConfig::default(),
        wal_manager,
        create_test_object_storage_cache(&temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();
    let (notify_tx, mut notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    // Persist data file to local filesystem, so iceberg snapshot should be created, if skip iceberg not specified.
    let row = test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 10);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 10)
        .await
        .unwrap();

    // Create mooncake snapshot.
    assert!(table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: false,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::Skip,
        index_merge_option: MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
        data_compaction_option: MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
    }));
    let (_, iceberg_snapshot_payload, _, _, _) =
        sync_mooncake_snapshot(&mut table, &mut notify_rx).await;
    assert!(iceberg_snapshot_payload.is_none());
}

/// ================================
/// Test iceberg snapshot store/load with large number of manifest entries
/// ================================
///
/// Testing scenario: write a large number of manifest entries to the manifest file and check pagination.
async fn test_manifest_entries_write_with_pagination_impl(
    iceberg_table_config: IcebergTableConfig,
) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(table_temp_dir.path().to_str().unwrap().to_string());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();

    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate object storage cache.
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();

    let mut data_files_to_import = vec![];
    let tasks = (0..=(DEFAULT_MAX_MANIFEST_ENTRY_COUNT + 1)).map(|file_id| {
        let parquet_path = table_temp_dir
            .path()
            .join(format!("data-{file_id}.parquet"));
        async move {
            let arrow_schema = create_test_arrow_schema();
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                    Arc::new(Int32Array::from(vec![10, 20, 30])),
                ],
            )
            .unwrap();
            let data_file =
                create_data_file(file_id as u64, parquet_path.to_str().unwrap().to_string());
            write_arrow_record_batch_to_local(data_file.file_path(), arrow_schema, &batch).await;
            data_file
        }
    });
    let results: Vec<MooncakeDataFileRef> =
        stream::iter(tasks).buffer_unordered(1024).collect().await;
    data_files_to_import.extend(results);

    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn: 0,
        new_table_schema: None,
        committed_deletion_logs: HashSet::new(),
        import_payload: IcebergSnapshotImportPayload {
            data_files: data_files_to_import.clone(),
            new_deletion_vector: HashMap::new(),
            file_indices: vec![],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![],
            old_data_files_to_remove: vec![],
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
            data_file_records_remap: HashMap::new(),
        },
    };

    let persistence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 0..((DEFAULT_MAX_MANIFEST_ENTRY_COUNT + 1) as u32),
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, persistence_file_params)
        .await
        .unwrap();

    // Remove one data manifest entries.
    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn: 0,
        new_table_schema: None,
        committed_deletion_logs: HashSet::new(),
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![],
            new_deletion_vector: HashMap::new(),
            file_indices: vec![],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![],
            old_data_files_to_remove: vec![data_files_to_import[0].clone()],
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
            data_file_records_remap: HashMap::new(),
        },
    };
    let persistence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 0..(DEFAULT_MAX_MANIFEST_ENTRY_COUNT + 1) as u32, // unused
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, persistence_file_params)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_manifest_entries_write_with_pagination() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    test_manifest_entries_write_with_pagination_impl(iceberg_table_config).await;
}

/// ================================
/// Test iceberg snapshot store/load
/// ================================
///
/// Test snapshot store and load for different types of catalogs based on the given warehouse.
async fn test_store_and_load_snapshot_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(table_temp_dir.path().to_str().unwrap().to_string());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    // ==============
    // Step 1
    // ==============
    //
    // At the beginning of the test, there's nothing in table.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate object storage cache.
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    assert!(iceberg_table_manager.persisted_data_files.is_empty());

    // Create arrow schema and table.
    let arrow_schema = create_test_arrow_schema();

    // Write first snapshot to iceberg table (with deletion vector).
    let data_filename_1 = "data-1.parquet";
    let batch_1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])), // id column
            Arc::new(StringArray::from(vec!["a", "b", "c"])), // name column
            Arc::new(Int32Array::from(vec![10, 20, 30])), // age column
        ],
    )
    .unwrap();
    let parquet_path = table_temp_dir.path().join(data_filename_1);
    let data_file_1 = create_data_file(
        /*file_id=*/ 0,
        parquet_path.to_str().unwrap().to_string(),
    );
    write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch_1).await;
    let file_index_1 = create_file_index(vec![data_file_1.clone()]);

    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn: 0,
        new_table_schema: None,
        committed_deletion_logs: test_committed_deletion_logs_to_persist_1(data_file_1.clone()),
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![data_file_1.clone()],
            new_deletion_vector: test_committed_deletion_log_1(data_file_1.clone()),
            file_indices: vec![file_index_1.clone()],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![],
            old_data_files_to_remove: vec![],
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
            data_file_records_remap: HashMap::new(),
        },
    };

    let persistence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 1..2,
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, persistence_file_params)
        .await
        .unwrap();

    // ==============
    // Step 2
    // ==============
    //
    // Write second snapshot to iceberg table, with updated deletion vector and new data file.
    let data_filename_2 = "data-2.parquet";
    let batch_2 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5, 6])), // id column
            Arc::new(StringArray::from(vec!["d", "e", "f"])), // name column
            Arc::new(Int32Array::from(vec![40, 50, 60])), // age column
        ],
    )
    .unwrap();
    let parquet_path = table_temp_dir.path().join(data_filename_2);
    let data_file_2 = create_data_file(
        /*file_id=*/ 2,
        parquet_path.to_str().unwrap().to_string(),
    );
    write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch_2).await;
    let file_index_2 = create_file_index(vec![data_file_2.clone()]);

    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn: 1,
        new_table_schema: None,
        committed_deletion_logs: test_committed_deletion_logs_to_persist_2(data_file_2.clone()),
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![data_file_2.clone()],
            new_deletion_vector: test_committed_deletion_log_2(data_file_2.clone()),
            file_indices: vec![file_index_2.clone()],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![],
            old_data_files_to_remove: vec![],
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
            data_file_records_remap: HashMap::new(),
        },
    };

    let persistence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 3..4,
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, persistence_file_params)
        .await
        .unwrap();

    // Check persisted items in the iceberg table.
    assert_eq!(
        iceberg_table_manager.persisted_data_files.len(),
        2,
        "Persisted items for table manager is {:?}",
        iceberg_table_manager.persisted_data_files
    );
    assert_eq!(iceberg_table_manager.persisted_file_indices.len(), 2);

    // Check the loaded data file is of the expected format and content.
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io()
        .clone();

    let mut remote_data_files = vec![];
    for (file_id, data_entry) in iceberg_table_manager.persisted_data_files.iter() {
        let file_path = data_entry.data_file.file_path();
        let loaded_arrow_batch = load_arrow_batch(&file_io, file_path).await.unwrap();
        let deleted_rows = data_entry.deletion_vector.collect_deleted_rows();
        assert_eq!(*loaded_arrow_batch.schema_ref(), arrow_schema);
        remote_data_files.push(create_data_file(
            file_id.0,
            data_entry.data_file.file_path().to_string(),
        ));

        // Check second data file and its deletion vector.
        let filename_2_without_suffix = get_filename_without_suffix(data_filename_2);
        if file_path.contains(&filename_2_without_suffix) {
            assert_eq!(loaded_arrow_batch, batch_2,);
            assert_eq!(deleted_rows, vec![1, 2],);
            continue;
        }

        // Check first data file and its deletion vector.
        let filename_1_without_suffix = get_filename_without_suffix(data_filename_1);
        assert!(file_path.contains(&filename_1_without_suffix));
        assert_eq!(loaded_arrow_batch, batch_1,);
        assert_eq!(deleted_rows, vec![0],);
    }

    // ==============
    // Step 3
    // ==============
    //
    // Write third snapshot to iceberg table, with file indices to add and remove.
    let merged_file_index = create_file_index(remote_data_files.clone());
    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn: 2,
        new_table_schema: None,
        committed_deletion_logs: HashSet::new(),
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![],
            new_deletion_vector: HashMap::new(),
            file_indices: vec![],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![merged_file_index.clone()],
            old_file_indices_to_remove: vec![file_index_1.clone(), file_index_2.clone()],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![],
            old_data_files_to_remove: vec![],
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
            data_file_records_remap: HashMap::new(),
        },
    };
    let persistence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 4..5,
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, persistence_file_params)
        .await
        .unwrap();
    assert_eq!(iceberg_table_manager.persisted_file_indices.len(), 1);

    // Create a new iceberg table manager and check persisted content.
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate object storage cache.
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (_, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.flush_lsn.unwrap(), 2);
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Prepare a compacted data file for data file 1 and 2.
    let compacted_data_filename = "compacted-data.parquet";
    let compacted_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2, 3, 4])), // id column
            Arc::new(StringArray::from(vec!["b", "c", "d"])), // name column
            Arc::new(Int32Array::from(vec![20, 30, 40])), // age column
        ],
    )
    .unwrap();
    let parquet_path = table_temp_dir.path().join(compacted_data_filename);
    let compacted_data_file = create_data_file(
        /*file_id=*/ 5,
        parquet_path.to_str().unwrap().to_string(),
    );
    write_arrow_record_batch_to_local(
        parquet_path.as_path(),
        arrow_schema.clone(),
        &compacted_batch,
    )
    .await;
    let compacted_file_index = create_file_index(vec![compacted_data_file.clone()]);

    // ==============
    // Step 4
    // ==============
    //
    // Attempt a fourth snapshot persistence, which goes after data file compaction.
    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn: 3,
        new_table_schema: None,
        committed_deletion_logs: HashSet::new(),
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![],
            new_deletion_vector: HashMap::new(),
            file_indices: vec![],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![compacted_data_file.clone()],
            old_data_files_to_remove: vec![data_file_1.clone(), data_file_2.clone()],
            new_file_indices_to_import: vec![compacted_file_index.clone()],
            old_file_indices_to_remove: vec![merged_file_index.clone()],
            data_file_records_remap: HashMap::new(),
        },
    };
    let persistence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 6..7,
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, persistence_file_params)
        .await
        .unwrap();

    // Create a new iceberg table manager and check persisted content.
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate object storage cache.
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (_, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.flush_lsn.unwrap(), 3);
    assert_eq!(snapshot.disk_files.len(), 1);
    let (data_file, batch_deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    // No deletion vector is expected.
    assert!(batch_deletion_vector.puffin_deletion_blob.is_none());
    assert!(batch_deletion_vector
        .committed_deletion_vector
        .collect_deleted_rows()
        .is_empty());
    // Check data file.
    let loaded_arrow_batch = load_arrow_batch(&file_io, data_file.file_path())
        .await
        .unwrap();
    assert_eq!(loaded_arrow_batch, compacted_batch);
    // Check file indices.
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // ==============
    // Step 5
    // ==============
    //
    // Remove all existing data files and file indices.
    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn: 4,
        new_table_schema: None,
        committed_deletion_logs: HashSet::new(),
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![],
            new_deletion_vector: HashMap::new(),
            file_indices: vec![],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![],
            old_data_files_to_remove: vec![compacted_data_file.clone()],
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![compacted_file_index.clone()],
            data_file_records_remap: HashMap::new(),
        },
    };
    let persistence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 7..8,
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, persistence_file_params)
        .await
        .unwrap();

    // Create a new iceberg table manager and check persisted content.
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate object storage cache.
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (_, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.flush_lsn.unwrap(), 4);
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;
}

/// Basic iceberg snapshot sync and load test via iceberg table manager.
#[tokio::test]
async fn test_sync_snapshots() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    test_store_and_load_snapshot_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_sync_snapshot_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_store_and_load_snapshot_impl(iceberg_table_config.clone()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_sync_snapshot_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_store_and_load_snapshot_impl(iceberg_table_config.clone()).await;
}

/// ================================
/// Test drop table
/// ================================
///
/// Test iceberg table manager drop table.
async fn test_drop_table_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(table_temp_dir.path().to_str().unwrap().to_string());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    iceberg_table_manager
        .initialize_iceberg_table_for_once()
        .await
        .unwrap();

    // Perform whitebox testing, which assume there's an indicator file at `<warehouse>/<namespace>/<table>/metadata/version-hint.text` stored at object storage.
    let mut indicator_filepath = PathBuf::new();
    assert_eq!(iceberg_table_config.namespace.len(), 1);
    indicator_filepath.push(iceberg_table_config.namespace.first().unwrap());
    indicator_filepath.push(iceberg_table_config.table_name);
    indicator_filepath.push(METADATA_DIRECTORY);
    indicator_filepath.push(VERSION_HINT_FILENAME);
    let object_exists = filesystem_accessor
        .object_exists(indicator_filepath.to_str().unwrap())
        .await
        .unwrap();
    assert!(object_exists);

    // Drop table and check directory existence.
    iceberg_table_manager.drop_table().await.unwrap();
    let object_exists = filesystem_accessor
        .object_exists(indicator_filepath.to_str().unwrap())
        .await
        .unwrap();
    assert!(!object_exists);
}

#[tokio::test]
async fn test_drop_table() {
    // Local filesystem for iceberg table.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);
    // Common testing logic.
    test_drop_table_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_drop_table_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_drop_table_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_drop_table_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_drop_table_impl(iceberg_table_config).await;
}

/// ================================
/// Test empty table load
/// ================================
///
/// Testing scenario: attempt an iceberg snapshot load with no preceding store.
async fn test_empty_snapshot_load_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(table_temp_dir.path().to_str().unwrap().to_string());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    // Recover from iceberg snapshot, and check mooncake table snapshot version.
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_table_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_table_id, 0);
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.flush_lsn.is_none());
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;
}

#[tokio::test]
async fn test_empty_snapshot_load() {
    // Local filesystem for iceberg table.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);
    // Common testing logic.
    test_empty_snapshot_load_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_empty_snapshot_load_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_empty_snapshot_load_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_empty_snapshot_load_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_empty_snapshot_load_impl(iceberg_table_config).await;
}

/// ================================
/// Test recover from failed snapshot persistence
/// ================================
///
/// Testing scenario: previous iceberg snapshot persistence fails, recover mooncake table to empty state.
async fn test_recover_from_failed_snapshot_impl(iceberg_table_config: IcebergTableConfig) {
    let arrow_schema = create_test_arrow_schema();
    let record_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])), // id column
            Arc::new(StringArray::from(vec!["a", "b", "c"])), // name column
            Arc::new(Int32Array::from(vec![10, 20, 30])), // age column
        ],
    )
    .unwrap();

    // Local filesystem to store write-through cache.
    let temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let file_path = temp_dir.path().join("file.parquet");
    let data_file = create_data_file(/*file_id=*/ 0, file_path.to_str().unwrap().to_string());
    write_arrow_record_batch_to_local(file_path.as_path(), arrow_schema.clone(), &record_batch)
        .await;

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_persistence = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn: 1,
        new_table_schema: None,
        committed_deletion_logs: HashSet::new(),
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![data_file],
            new_deletion_vector: HashMap::new(),
            file_indices: Vec::new(),
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload::default(),
        data_compaction_payload: IcebergSnapshotDataCompactionPayload::default(),
    };

    let persistence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 0..1,
    };
    iceberg_table_manager_for_persistence
        .sync_snapshot(iceberg_snapshot_payload, persistence_file_params)
        .await
        .unwrap();

    // Craft such situation: there's only initial metadata file (with version 0).
    let version_hint = format!(
        "{}/{}/{}/metadata/{}",
        iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        ICEBERG_TEST_NAMESPACE,
        ICEBERG_TEST_TABLE,
        VERSION_HINT_FILENAME
    );
    tokio::fs::remove_file(&version_hint)
        .await
        .unwrap_or_else(|_| panic!("failed to remove {version_hint}"));
    tokio::fs::write(&version_hint, "0")
        .await
        .unwrap_or_else(|_| panic!("failed to write {version_hint}"));

    // Recover from iceberg snapshot, and check mooncake table snapshot version.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 0);
    assert_eq!(snapshot.disk_files.len(), 0);
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.flush_lsn.is_none());
}

#[tokio::test]
async fn test_recover_from_failed_snapshot() {
    // Local filesystem for iceberg table.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);
    // Common testing logic.
    test_recover_from_failed_snapshot_impl(iceberg_table_config).await;
}

/// ================================
/// Test index merge
/// ================================
///
/// Testing scenario: create iceberg snapshot for index merge.
async fn test_index_merge_and_create_snapshot_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let file_index_config = FileIndexMergeConfig {
        min_file_indices_to_merge: 2,
        max_file_indices_to_merge: 2,
        index_block_final_size: u64::MAX,
    };
    let mut config = MooncakeTableConfig::new(table_temp_dir.path().to_str().unwrap().to_string());
    config.file_index_config = file_index_config;
    let mooncake_table_metadata = create_test_table_metadata_with_config(
        table_temp_dir.path().to_str().unwrap().to_string(),
        config,
    );

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();

    // Create mooncake table and table event notification receiver.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
    )
    .await;
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);

    // Append one row and commit/flush, so we have one file indice persisted.
    let row_1 = test_row_1();
    table.append(row_1.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 1)
        .await
        .unwrap();

    // Append one row and commit/flush, so we have one file indice persisted.
    let row_2 = test_row_2();
    table.append(row_2.clone()).unwrap();
    table.commit(/*lsn=*/ 2);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 2)
        .await
        .unwrap();

    // Append one row and commit/flush, so we have one file indice persisted.
    let row_3 = test_row_3();
    table.append(row_3.clone()).unwrap();
    table.commit(/*lsn=*/ 3);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 3)
        .await
        .unwrap();

    // Attempt index merge and flush to iceberg table.
    create_mooncake_and_iceberg_snapshot_for_index_merge_for_test(&mut table, &mut notify_rx).await;

    // Create a new iceberg table manager and check states.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 5); // three data files, two index block file
    assert_eq!(snapshot.disk_files.len(), 3);
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.flush_lsn.unwrap(), 3);
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    // Delete rows after merge, to make sure file indices are serving correctly.
    table.delete(row_1.clone(), /*lsn=*/ 3).await;
    table.delete(row_2.clone(), /*lsn=*/ 4).await;
    table.commit(/*lsn=*/ 5);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 5)
        .await
        .unwrap();

    // Attempt index merge and flush to iceberg table.
    create_mooncake_and_iceberg_snapshot_for_index_merge_for_test(&mut table, &mut notify_rx).await;

    // Create a new iceberg table manager and check states.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 6); // three data files, one index block file, two deletion vectors
    assert_eq!(snapshot.disk_files.len(), 3);
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.flush_lsn.unwrap(), 5);
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
}

#[tokio::test]
async fn test_index_merge_and_create_snapshot() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    test_index_merge_and_create_snapshot_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_index_merge_and_create_snapshot_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_index_merge_and_create_snapshot_impl(iceberg_table_config.clone()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_index_merge_and_create_snapshot_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_index_merge_and_create_snapshot_impl(iceberg_table_config.clone()).await;
}

/// ================================
/// Test data compaction
/// ================================
///
/// Testing scenario: create iceberg snapshot for data compaction.
async fn test_data_compaction_and_create_snapshot_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let data_compaction_config = DataCompactionConfig {
        min_data_file_to_compact: 2,
        max_data_file_to_compact: 2,
        data_file_final_size: u64::MAX,
        data_file_deletion_percentage: 0,
    };
    let mut config = MooncakeTableConfig::new(table_temp_dir.path().to_str().unwrap().to_string());
    config.data_compaction_config = data_compaction_config;
    let mooncake_table_metadata = create_test_table_metadata_with_config(
        table_temp_dir.path().to_str().unwrap().to_string(),
        config,
    );

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();

    // Create mooncake table and table event notification receiver.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
    )
    .await;
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);

    // Append one row and commit/flush, so we have one file indice persisted.
    let row_1 = test_row_1();
    table.append(row_1.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 1)
        .await
        .unwrap();

    // Append one row and commit/flush, so we have one file indice persisted.
    let row_2 = test_row_2();
    table.append(row_2.clone()).unwrap();
    table.commit(/*lsn=*/ 2);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 2)
        .await
        .unwrap();

    // Append one row and commit/flush, so we have one file indice persisted.
    let row_3 = test_row_3();
    table.append(row_3.clone()).unwrap();
    table.commit(/*lsn=*/ 3);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 3)
        .await
        .unwrap();

    // Attempt data compaction and flush to iceberg table.
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut notify_rx,
        /*injected_committed_deletion_rows=*/ vec![],
        /*injected_uncommitted_deletion_rows=*/ vec![],
    )
    .await;

    // Create a new iceberg table manager and check states.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 4); // two data files, two index block file
    assert_eq!(snapshot.disk_files.len(), 2);
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.flush_lsn.unwrap(), 3);
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    // Delete rows after merge, to make sure file indices are serving correctly.
    table.delete(row_1.clone(), /*lsn=*/ 3).await;
    table.delete(row_2.clone(), /*lsn=*/ 4).await;
    table.commit(/*lsn=*/ 5);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 5)
        .await
        .unwrap();

    // Attempt index merge and flush to iceberg table.
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut notify_rx,
        /*injected_committed_deletion_rows=*/ vec![],
        /*injected_uncommitted_deletion_rows=*/ vec![],
    )
    .await;

    // Create a new iceberg table manager and check states.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one index block file
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.flush_lsn.unwrap(), 5);
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
}

#[tokio::test]
async fn test_data_compaction_and_create_snapshot() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    test_data_compaction_and_create_snapshot_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_data_compaction_and_create_snapshot_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_data_compaction_and_create_snapshot_impl(iceberg_table_config.clone()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_data_compaction_and_create_snapshot_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_data_compaction_and_create_snapshot_impl(iceberg_table_config.clone()).await;
}

/// ================================
/// Test data compaction with update
/// ================================
///
/// Testing scenario: create iceberg snapshot for data compaction.
async fn test_data_compaction_with_update_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let data_compaction_config = DataCompactionConfig {
        min_data_file_to_compact: 2,
        max_data_file_to_compact: 3,
        data_file_final_size: u64::MAX,
        data_file_deletion_percentage: 0,
    };
    let mut config = MooncakeTableConfig::new(table_temp_dir.path().to_str().unwrap().to_string());
    config.data_compaction_config = data_compaction_config;
    let mooncake_table_metadata = create_test_table_metadata_with_config(
        table_temp_dir.path().to_str().unwrap().to_string(),
        config,
    );

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();

    // Create mooncake table and table event notification receiver.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
    )
    .await;
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);

    // Append one row.
    let row = test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 1)
        .await
        .unwrap();

    // Delete and append the row again.
    table.delete(row.clone(), /*lsn=*/ 2).await;
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 3);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 3)
        .await
        .unwrap();

    // Delete and append the row again.
    table.delete(row.clone(), /*lsn=*/ 4).await;
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 5);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 5)
        .await
        .unwrap();

    // Attempt data compaction and flush to iceberg table.
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut notify_rx,
        /*injected_committed_deletion_rows=*/ vec![],
        /*injected_uncommitted_deletion_rows=*/ vec![],
    )
    .await;

    // Create a new iceberg table manager and check states.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one file index
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.flush_lsn.unwrap(), 5);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    // Validate iceberg snapshot.
    verify_recovered_mooncake_snapshot(&snapshot, /*expected_ids=*/ &[1]).await;
}

#[tokio::test]
async fn test_data_compaction_with_update() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    test_data_compaction_with_update_impl(iceberg_table_config).await;
}

/// ================================
/// Test delayed data compaction
/// ================================
///
/// Testing scenario and testing order:
/// - mooncake snapshot, and get iceberg snapshot payload and data compaction payload
/// - create iceberg snapshot, and reflect the change to mooncake snapshot
/// - trigger data compaction
async fn test_delayed_compaction_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let data_compaction_config = DataCompactionConfig {
        min_data_file_to_compact: 2,
        max_data_file_to_compact: 3,
        data_file_final_size: u64::MAX,
        data_file_deletion_percentage: 0,
    };
    let mut config = MooncakeTableConfig::new(table_temp_dir.path().to_str().unwrap().to_string());
    config.data_compaction_config = data_compaction_config;
    let mooncake_table_metadata = create_test_table_metadata_with_config(
        table_temp_dir.path().to_str().unwrap().to_string(),
        config,
    );

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();

    // Create mooncake table and table event notification receiver.
    let object_storage_cache = ObjectStorageCache::new(ObjectStorageCacheConfig::new(
        /*max_bytes=*/ u64::MAX,
        cache_temp_dir.path().to_str().unwrap().to_string(),
        /*optimize_local_filesystem=*/ false,
    ));
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        Arc::new(object_storage_cache.clone()), // Use separate cache for each table.
    )
    .await;
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);

    // Append two row, used for two deletions later.
    let row_1 = test_row_1();
    let row_2 = test_row_2();
    table.append(row_1.clone()).unwrap();
    table.append(row_2.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 1)
        .await
        .unwrap();

    // Append another row, delete one row in the current transaction.
    let row_3 = test_row_3();
    table.delete(row_1.clone(), /*lsn=*/ 2).await;
    table.append(row_3.clone()).unwrap();
    table.commit(/*lsn=*/ 3);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 3)
        .await
        .unwrap();

    // Attempt data compaction and flush to iceberg table.
    let (_, iceberg_snapshot_payload, _, _, _) =
        create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;

    // Persist iceberg snapshot and reflect to mooncake snapshot.
    let iceberg_snapshot_payload = iceberg_snapshot_payload.unwrap();
    let (_, _, _, data_compaction_payload, _) =
        create_iceberg_snapshot_and_reflect_to_mooncake_snapshot(
            iceberg_snapshot_payload,
            &mut table,
            &mut notify_rx,
        )
        .await;
    // Now we're eligible to perform data compaction, the data compaction payload should contains deletion vector for one row deleted.

    // Append a new row, delete one existing row to trigger a new puffin blob file; create mooncake and iceberg snapshot.
    let row_4 = test_row_4();
    table.delete(row_2, /*lsn=*/ 4).await;
    table.append(row_4.clone()).unwrap();
    table.commit(/*lsn=*/ 5);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 5)
        .await
        .unwrap();

    // Create mooncake and iceberg snapshot, the old puffin blob will gets deleted.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;
    let (_, _, _, _, evicted_files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;
    io_utils::delete_local_files(&evicted_files_to_delete)
        .await
        .unwrap();

    // Now trigger a data compaction operation and block wait its completion.
    let data_compaction_payload = data_compaction_payload.take_payload().unwrap();
    table.perform_data_compaction(data_compaction_payload);
    let data_compaction_result = sync_data_compaction(&mut notify_rx).await;
    table.set_data_compaction_res(data_compaction_result);

    // Persist iceberg snapshot and reflect to mooncake snapshot.
    let (_, iceberg_snapshot_payload, _, _, _) =
        create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;
    let iceberg_snapshot_payload = iceberg_snapshot_payload.unwrap();
    create_iceberg_snapshot_and_reflect_to_mooncake_snapshot(
        iceberg_snapshot_payload,
        &mut table,
        &mut notify_rx,
    )
    .await;

    // Create a new iceberg table manager and check states.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    // Data compaction only take place on two data files.
    assert_eq!(next_file_id, 5); // two data files (one compacted, one uncompacted), one deletion vector, two file index
    assert_eq!(snapshot.disk_files.len(), 2);
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.flush_lsn.unwrap(), 5);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    // Validate iceberg snapshot.
    verify_recovered_mooncake_snapshot(&snapshot, /*expected_ids=*/ &[3, 4]).await;
}

#[tokio::test]
async fn test_delayed_compaction() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    test_delayed_compaction_impl(iceberg_table_config).await;
}

/// ================================
/// Test data compaction with append-only table
/// ================================
///
/// Testing scenario: create iceberg snapshot for data compaction.
async fn test_data_compaction_append_only_and_create_snapshot_impl(
    iceberg_table_config: IcebergTableConfig,
) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let data_compaction_config = DataCompactionConfig {
        min_data_file_to_compact: 2,
        max_data_file_to_compact: 2,
        data_file_final_size: u64::MAX,
        data_file_deletion_percentage: 0,
    };
    let mut config = MooncakeTableConfig::new(table_temp_dir.path().to_str().unwrap().to_string());
    config.data_compaction_config = data_compaction_config;
    config.append_only = true;
    config.row_identity = IdentityProp::None;
    let mooncake_table_metadata = create_test_table_metadata_with_config(
        table_temp_dir.path().to_str().unwrap().to_string(),
        config,
    );

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();

    // Create mooncake table and table event notification receiver.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
    )
    .await;
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);

    // Append one row and commit/flush, so we have one file indice persisted.
    let row_1 = test_row_1();
    table.append(row_1.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 1)
        .await
        .unwrap();

    // Append one row and commit/flush, so we have one file indice persisted.
    let row_2 = test_row_2();
    table.append(row_2.clone()).unwrap();
    table.commit(/*lsn=*/ 2);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 2)
        .await
        .unwrap();

    // Attempt data compaction and flush to iceberg table.
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut notify_rx,
        /*injected_committed_deletion_rows=*/ vec![],
        /*injected_uncommitted_deletion_rows=*/ vec![],
    )
    .await;

    // Create a new iceberg table manager and check states.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 1); // one compacted data file
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(snapshot.indices.file_indices.len(), 0);
    assert_eq!(snapshot.flush_lsn.unwrap(), 2);
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
}

#[tokio::test]
async fn test_data_compaction_append_only_and_create_snapshot() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    test_data_compaction_append_only_and_create_snapshot_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_data_compaction_append_only_and_create_snapshot_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_data_compaction_append_only_and_create_snapshot_impl(iceberg_table_config.clone()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_data_compaction_append_only_and_create_snapshot_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_data_compaction_append_only_and_create_snapshot_impl(iceberg_table_config.clone()).await;
}

/// ================================
/// Test data compaction by deletion
/// ================================
///
/// Testing scenario: create iceberg snapshot for index merge.
async fn test_data_compaction_by_deletion_and_create_snapshot_impl(
    iceberg_table_config: IcebergTableConfig,
) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let data_compaction_config = DataCompactionConfig {
        min_data_file_to_compact: 2,
        max_data_file_to_compact: 2,
        data_file_final_size: 1,
        data_file_deletion_percentage: 50,
    };
    let mut config = MooncakeTableConfig::new(table_temp_dir.path().to_str().unwrap().to_string());
    config.data_compaction_config = data_compaction_config;
    let mooncake_table_metadata = create_test_table_metadata_with_config(
        table_temp_dir.path().to_str().unwrap().to_string(),
        config,
    );

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();

    // Create mooncake table and table event notification receiver.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
    )
    .await;
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);

    // Append two rows and commit/flush.
    let row_1 = test_row_1();
    table.append(row_1.clone()).unwrap();
    table.commit(/*lsn=*/ 1);

    let row_2 = test_row_2();
    table.append(row_2.clone()).unwrap();
    table.commit(/*lsn=*/ 2);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 2)
        .await
        .unwrap();

    // Append two rows and commit/flush.
    let row_3 = test_row_3();
    table.append(row_3.clone()).unwrap();
    table.commit(/*lsn=*/ 3);

    let row_4 = test_row_4();
    table.append(row_4.clone()).unwrap();
    table.commit(/*lsn=*/ 4);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 4)
        .await
        .unwrap();

    // Delete two rows within each data file.
    table.delete(row_1.clone(), /*lsn=*/ 5).await;
    table.delete(row_3.clone(), /*lsn=*/ 6).await;
    table.commit(/*lsn=*/ 7);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 8)
        .await
        .unwrap();

    // Attempt data compaction and flush to iceberg table.
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut notify_rx,
        /*injected_committed_deletion_rows=*/ vec![],
        /*injected_uncommitted_deletion_rows=*/ vec![],
    )
    .await;

    // Create a new iceberg table manager and check states.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 3); // two data files, and one file index
    assert_eq!(snapshot.disk_files.len(), 2);
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.flush_lsn.unwrap(), 8);
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
}

#[tokio::test]
async fn test_data_compaction_by_deletion_and_create_snapshot() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    test_data_compaction_by_deletion_and_create_snapshot_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_data_compaction_by_deletion_and_create_snapshot_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_data_compaction_by_deletion_and_create_snapshot_impl(iceberg_table_config.clone()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_data_compaction_by_deletion_and_create_snapshot_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_data_compaction_by_deletion_and_create_snapshot_impl(iceberg_table_config.clone()).await;
}

/// ================================
/// Test empty snapshot creation
/// ================================
///
/// Testing scenario: attempt an iceberg snapshot when no data file, deletion vector or index files generated.
async fn test_empty_content_snapshot_creation_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(table_temp_dir.path().to_str().unwrap().to_string());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_persistence = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn: 0,
        new_table_schema: None,
        committed_deletion_logs: HashSet::new(),
        import_payload: IcebergSnapshotImportPayload::default(),
        index_merge_payload: IcebergSnapshotIndexMergePayload::default(),
        data_compaction_payload: IcebergSnapshotDataCompactionPayload::default(),
    };

    let persistence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 0..1,
    };
    iceberg_table_manager_for_persistence
        .sync_snapshot(iceberg_snapshot_payload, persistence_file_params)
        .await
        .unwrap();

    // Recover from iceberg snapshot, and check mooncake table snapshot version.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 0);
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert_eq!(snapshot.flush_lsn.unwrap(), 0);
}

#[tokio::test]
async fn test_empty_content_snapshot_creation() {
    // Local filesystem for iceberg table.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);
    // Common testing logic.
    test_empty_content_snapshot_creation_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_empty_content_snapshot_creation_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_empty_content_snapshot_creation_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_empty_content_snapshot_creation_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_empty_content_snapshot_creation_impl(iceberg_table_config).await;
}

/// ================================
/// Test duplicate local filename
/// ================================
///
/// Testing scenario: attempt an iceberg snapshot when local data files have duplicate filename.
async fn test_snapshot_creation_with_duplicate_filename_impl(
    iceberg_table_config: IcebergTableConfig,
) {
    let arrow_schema = create_test_arrow_schema();
    let record_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])), // id column
            Arc::new(StringArray::from(vec!["a", "b", "c"])), // name column
            Arc::new(Int32Array::from(vec![10, 20, 30])), // age column
        ],
    )
    .unwrap();

    // Local filesystem to store write-through cache.
    let temp_dir_1 = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir_1.path().to_str().unwrap().to_string());
    let file_path_1 = temp_dir_1.path().join("a_duplicate_file.parquet");
    let data_file_1 = create_data_file(
        /*file_id=*/ 0,
        file_path_1.to_str().unwrap().to_string(),
    );
    write_arrow_record_batch_to_local(file_path_1.as_path(), arrow_schema.clone(), &record_batch)
        .await;

    // Create another file with the filename.
    let temp_dir_2 = tempdir().unwrap();
    let file_path_2 = temp_dir_2.path().join("a_duplicate_file.parquet");
    let data_file_2 = create_data_file(
        /*file_id=*/ 1,
        file_path_2.to_str().unwrap().to_string(),
    );
    write_arrow_record_batch_to_local(file_path_2.as_path(), arrow_schema.clone(), &record_batch)
        .await;

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_persistence = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn: 1,
        new_table_schema: None,
        committed_deletion_logs: HashSet::new(),
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![data_file_1, data_file_2],
            new_deletion_vector: HashMap::new(),
            file_indices: Vec::new(),
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload::default(),
        data_compaction_payload: IcebergSnapshotDataCompactionPayload::default(),
    };

    let persistence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 0..1,
    };
    iceberg_table_manager_for_persistence
        .sync_snapshot(iceberg_snapshot_payload, persistence_file_params)
        .await
        .unwrap();

    // Recover from iceberg snapshot, and check mooncake table snapshot version.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2);
    assert_eq!(snapshot.disk_files.len(), 2);
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert_eq!(snapshot.flush_lsn.unwrap(), 1);
}

#[tokio::test]
async fn test_snapshot_creation_with_duplicate_filename() {
    // Local filesystem for iceberg table.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);
    // Common testing logic.
    test_snapshot_creation_with_duplicate_filename_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_snapshot_creation_with_duplicate_filename_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_snapshot_creation_with_duplicate_filename_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_snapshot_creation_with_duplicate_filename_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_snapshot_creation_with_duplicate_filename_impl(iceberg_table_config).await;
}

/// Test scenario: small batch size and large parquet file, which means:
/// 1. all rows live within their own record batch, and potentially their own batch deletion vector.
/// 2. when flushed to on-disk parquet files, they're grouped into one file but different arrow batch records.
/// Fixed issue: https://github.com/Mooncake-Labs/moonlink/issues/343
#[tokio::test]
async fn test_small_batch_size_and_large_parquet_size() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let object_storage_cache = create_test_object_storage_cache(&temp_dir);
    let path = temp_dir.path().to_path_buf();
    let warehouse_uri = path.clone().to_str().unwrap().to_string();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());

    let iceberg_table_config = create_iceberg_table_config(warehouse_uri.clone());
    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &path);
    let wal_manager = WalManager::new(&wal_config);
    let schema = create_test_arrow_schema();
    let mooncake_table_config = MooncakeTableConfig {
        append_only: false,
        batch_size: 1,
        disk_slice_writer_config: DiskSliceWriterConfig {
            parquet_file_size: 1000,
            chaos_config: None,
        },
        // Trigger iceberg snapshot as long as there're any commit deletion log.
        persistence_config: IcebergPersistenceConfig {
            new_committed_deletion_log: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*table_id=*/ 1,
        path,
        iceberg_table_config.clone(),
        mooncake_table_config,
        wal_manager,
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();
    let (notify_tx, mut notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    // Append first row.
    let row_1 = test_row_1();
    table.append(row_1.clone()).unwrap();

    // Append second row.
    let row_2 = test_row_2();
    table.append(row_2.clone()).unwrap();

    // Commit, flush and create snapshots.
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 1)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Delete the second record.
    table.delete(/*row=*/ row_2.clone(), /*lsn=*/ 2).await;
    table.commit(/*lsn=*/ 3);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 3)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 3); // one data file, one index block file, one deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 1);
    let deletion_vector = snapshot.disk_files.iter().next().unwrap().1.clone();
    assert_eq!(
        deletion_vector
            .committed_deletion_vector
            .collect_deleted_rows(),
        vec![1]
    );
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(&snapshot, &warehouse_uri, filesystem_accessor.as_ref()).await;
}

/// Testing scenario: a large number of deletion records are requested to persist to iceberg, thus multiple table auto increment ids are needed.
/// For more details, please refer to https://github.com/Mooncake-Labs/moonlink/issues/640
#[tokio::test]
async fn test_multiple_table_ids_for_deletion_vector() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();

    let iceberg_table_config = get_iceberg_table_config(&temp_dir);
    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &path);
    let wal_manager = WalManager::new(&wal_config);
    let schema = create_test_arrow_schema();
    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*table_id=*/ 1,
        path,
        iceberg_table_config.clone(),
        MooncakeTableConfig::default(),
        wal_manager,
        create_test_object_storage_cache(&temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();
    let (notify_tx, mut notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    // Create a large number of data files, which is larger than [`NUM_FILES_PER_FLUSH`].
    let target_data_files_num = storage_utils::NUM_FILES_PER_FLUSH + 1;
    let mut all_rows = Vec::with_capacity(target_data_files_num as usize);
    for idx in 0..target_data_files_num {
        let cur_row = MoonlinkRow::new(vec![
            RowValue::Int32(idx as i32),
            RowValue::ByteArray(idx.to_be_bytes().to_vec()),
            RowValue::Int32(idx as i32),
        ]);
        all_rows.push(cur_row.clone());
        table.append(cur_row).unwrap();
        table.commit(/*lsn=*/ idx);
        flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ idx)
            .await
            .unwrap();
    }

    // Create the first mooncake and iceberg snapshot, which include [`target_data_files_num`] number of files.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Delete all rows, which corresponds to large number of deletion vector puffin files.
    for cur_row in all_rows.into_iter() {
        table.delete(cur_row, /*lsn=*/ target_data_files_num).await;
    }
    table.commit(/*lsn=*/ target_data_files_num + 1);
    flush_table_and_sync(
        &mut table,
        &mut notify_rx,
        /*lsn=*/ target_data_files_num + 1,
    )
    .await
    .unwrap();

    // Create the second mooncake and iceberg snapshot, which include [`target_data_files_num`] number of deletion vector puffin files.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Load snapshot from iceberg table to validate.
    let (_, mut iceberg_table_manager_for_recovery, _) =
        create_table_and_iceberg_manager(&temp_dir).await;
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id as u64, target_data_files_num * 3); // one for data file, one for deletion vector puffin, one for file indices.

    // Check deletion vector puffin files' file id.
    assert_eq!(snapshot.disk_files.len() as u64, target_data_files_num);
    for (_, cur_disk_file_entry) in snapshot.disk_files.iter() {
        assert!(cur_disk_file_entry.puffin_deletion_blob.is_some());
    }
}

/// ================================
/// Test async iceberg snapshot
/// ================================
///
/// Testing scenario: mooncake snapshot and iceberg snapshot doesn't correspond to each other 1-1.
/// In the test case we perform one iceberg snapshot after three mooncake snapshots.
async fn test_async_iceberg_snapshot_impl(iceberg_table_config: IcebergTableConfig) {
    let expected_arrow_batch_1 = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["John"])),
            Arc::new(Int32Array::from(vec![10])),
        ],
    )
    .unwrap();
    let expected_arrow_batch_2 = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["Bob"])),
            Arc::new(Int32Array::from(vec![20])),
        ],
    )
    .unwrap();
    let expected_arrow_batch_3 = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["Cat"])),
            Arc::new(Int32Array::from(vec![30])),
        ],
    )
    .unwrap();

    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(table_temp_dir.path().to_str().unwrap().to_string());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    // Create mooncake table and table event notification receiver.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache.clone(),
    )
    .await;
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);

    // Operation group 1: Append new rows and create mooncake snapshot.
    let row_1 = test_row_1();
    table.append(row_1.clone()).unwrap();
    table.commit(/*lsn=*/ 10);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 10)
        .await
        .unwrap();
    let (_, iceberg_snapshot_payload, _, _, _) =
        create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;

    // Operation group 2: Append new rows and create mooncake snapshot.
    let row_2 = test_row_2();
    table.append(row_2.clone()).unwrap();
    table.delete(row_1.clone(), /*lsn=*/ 20).await;
    table.commit(/*lsn=*/ 30);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 30)
        .await
        .unwrap();
    let (_, _, _, _, _) = create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;

    // Create iceberg snapshot for the first mooncake snapshot.
    let iceberg_snapshot_result =
        create_iceberg_snapshot(&mut table, iceberg_snapshot_payload, &mut notify_rx).await;
    table.set_iceberg_snapshot_res(iceberg_snapshot_result.unwrap());

    // Load and check iceberg snapshot.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate and fresh new cache for each recovery.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one index block file
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.flush_lsn.unwrap(), 10);

    // Get file io after load snapshot.
    let file_io = iceberg_table_manager_for_recovery
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io()
        .clone();
    let (data_file_1, deletion_vector_1) = snapshot.disk_files.iter().next().unwrap();
    let actual_arrow_batch = load_arrow_batch(&file_io, data_file_1.file_path())
        .await
        .unwrap();
    assert_eq!(actual_arrow_batch, expected_arrow_batch_1);
    assert!(deletion_vector_1
        .committed_deletion_vector
        .collect_deleted_rows()
        .is_empty());
    assert!(deletion_vector_1.puffin_deletion_blob.is_none());
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    // Operation group 3: Append new rows and create mooncake snapshot.
    let row_3 = test_row_3();
    table.append(row_3.clone()).unwrap();
    table.commit(/*lsn=*/ 40);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 40)
        .await
        .unwrap();
    let (_, iceberg_snapshot_payload, _, _, _) =
        create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;

    // Create iceberg snapshot for the mooncake snapshot.
    let iceberg_snapshot_result =
        create_iceberg_snapshot(&mut table, iceberg_snapshot_payload, &mut notify_rx).await;
    table.set_iceberg_snapshot_res(iceberg_snapshot_result.unwrap());

    // Load and check iceberg snapshot.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate and fresh new cache for each recovery.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, mut snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 7); // three data files, three index block files, one deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 3);
    assert_eq!(snapshot.indices.file_indices.len(), 3);
    assert_eq!(snapshot.flush_lsn.unwrap(), 40);

    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    // Find the key-value pair, which correspond to old snapshot's only key.
    let mut old_data_file: Option<MooncakeDataFileRef> = None;
    for (cur_data_file, _) in snapshot.disk_files.iter() {
        if cur_data_file.file_path() == data_file_1.file_path() {
            old_data_file = Some(cur_data_file.clone());
            break;
        }
    }
    let old_data_file = old_data_file.unwrap();

    // Get file io after load snapshot.
    let file_io = iceberg_table_manager_for_recovery
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io()
        .clone();

    // Left arrow record 2 and 3, both don't have deletion vector.
    let deletion_entry = snapshot.disk_files.remove(&old_data_file).unwrap();
    assert_eq!(
        deletion_entry
            .committed_deletion_vector
            .collect_deleted_rows(),
        vec![0]
    );
    let mut arrow_batch_2_persisted = false;
    let mut arrow_batch_3_persisted = false;
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.iter() {
        assert!(cur_deletion_vector.puffin_deletion_blob.is_none());
        assert!(cur_deletion_vector
            .committed_deletion_vector
            .collect_deleted_rows()
            .is_empty());

        let actual_arrow_batch = load_arrow_batch(&file_io, cur_data_file.file_path())
            .await
            .unwrap();
        if actual_arrow_batch == expected_arrow_batch_2 {
            arrow_batch_2_persisted = true;
        } else if actual_arrow_batch == expected_arrow_batch_3 {
            arrow_batch_3_persisted = true;
        }
    }
    assert!(arrow_batch_2_persisted);
    assert!(arrow_batch_3_persisted);
}

#[tokio::test]
async fn test_async_iceberg_snapshot() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    test_async_iceberg_snapshot_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_async_iceberg_snapshot_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_async_iceberg_snapshot_impl(iceberg_table_config.clone()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_async_iceberg_snapshot_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_async_iceberg_snapshot_impl(iceberg_table_config.clone()).await;
}

/// ================================
/// Test mooncake snapshot
/// ================================
///
async fn mooncake_table_snapshot_persist_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(table_temp_dir.path().to_str().unwrap().to_string());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    // Create mooncake table and table event notification receiver.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache.clone(),
    )
    .await;
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);

    // Perform a few table write operations.
    //
    // Operation series 1: append three rows, delete one of them, flush, commit and create snapshot.
    // Expects to see one data file with no deletion vector, because mooncake table handle deletion inline before persistence, and all record batches are dumped into one single data file.
    // The three rows are deleted in three operations series respectively.
    let row1 = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row1.clone()).unwrap();
    let row2 = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Alice".as_bytes().to_vec()),
        RowValue::Int32(10),
    ]);
    table.append(row2.clone()).unwrap();
    let row3 = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row3.clone()).unwrap();
    // First deletion of row1, which happens in MemSlice.
    table.delete(row1.clone(), /*flush_lsn=*/ 100).await;
    table.commit(/*flush_lsn=*/ 200);
    flush_table_and_sync(&mut table, &mut notify_rx, /*flush_lsn=*/ 200)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate and fresh new cache for each recovery.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one index block file
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(
        snapshot.indices.file_indices.len(),
        1,
        "Snapshot data files and file indices are {:?}",
        get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
    );
    check_row_index_nonexistent(&snapshot, &row1).await;
    check_row_index_on_disk(&snapshot, &row2, filesystem_accessor.as_ref()).await;
    check_row_index_on_disk(&snapshot, &row3, filesystem_accessor.as_ref()).await;
    assert_eq!(snapshot.flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check the loaded data file is of the expected format and content.
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.file_path().as_str())
        .await
        .unwrap();
    let expected_arrow_batch = RecordBatch::try_new(
        create_test_arrow_schema(),
        // row2 and row3
        vec![
            Arc::new(Int32Array::from(vec![2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(Int32Array::from(vec![10, 50])),
        ],
    )
    .unwrap();
    assert_eq!(
        loaded_arrow_batch, expected_arrow_batch,
        "Expected arrow data is {expected_arrow_batch:?}, actual data is {loaded_arrow_batch:?}"
    );
    let deleted_rows = deletion_vector
        .committed_deletion_vector
        .collect_deleted_rows();
    assert!(
        deleted_rows.is_empty(),
        "There should be no deletion vector in iceberg table."
    );

    // --------------------------------------
    // Operation series 2: no more additional rows appended, only to delete the first row in the table.
    // Expects to see a new deletion vector, because its corresponding data file has been persisted.
    table.delete(row2.clone(), /*flush_lsn=*/ 200).await;
    table.commit(/*flush_lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*flush_lsn=*/ 300)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate and fresh new cache for each recovery.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 3); // one data file, one index block file, one deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(
        snapshot.indices.file_indices.len(),
        1,
        "Snapshot data files and file indices are {:?}",
        get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
    );
    // row1 is deleted in-memory, so file index doesn't track it
    check_row_index_nonexistent(&snapshot, &row1).await;
    // row2 is deleted, but still exist in data file
    check_row_index_on_disk(&snapshot, &row2, filesystem_accessor.as_ref()).await;
    check_row_index_on_disk(&snapshot, &row3, filesystem_accessor.as_ref()).await;
    assert_eq!(snapshot.flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check the loaded data file is of the expected format and content.
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.file_path().as_str())
        .await
        .unwrap();
    assert_eq!(
        loaded_arrow_batch, expected_arrow_batch,
        "Expected arrow data is {expected_arrow_batch:?}, actual data is {loaded_arrow_batch:?}"
    );

    let deleted_rows = deletion_vector
        .committed_deletion_vector
        .collect_deleted_rows();
    let expected_deleted_rows = vec![0_u64];
    assert_eq!(deleted_rows, expected_deleted_rows);

    // --------------------------------------
    // Operation series 3: no more additional rows appended, only to delete the last row in the table.
    // Expects to see the existing deletion vector updated, because its corresponding data file has been persisted.
    table.delete(row3.clone(), /*flush_lsn=*/ 300).await;
    flush_table_and_sync(&mut table, &mut notify_rx, /*flush_lsn=*/ 400)
        .await
        .unwrap();
    table.commit(/*flush_lsn=*/ 400);
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate and fresh new cache for each recovery.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 3); // one data file, one index block file, one deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(
        snapshot.indices.file_indices.len(),
        1,
        "Snapshot data files and file indices are {:?}",
        get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
    );
    check_row_index_nonexistent(&snapshot, &row1).await;
    // row2 and row3 are deleted, but still exist in data file
    check_row_index_on_disk(&snapshot, &row2, filesystem_accessor.as_ref()).await;
    check_row_index_on_disk(&snapshot, &row3, filesystem_accessor.as_ref()).await;
    assert_eq!(snapshot.flush_lsn.unwrap(), 400);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config
            .metadata_accessor_config
            .get_warehouse_uri(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check the loaded data file is of the expected format and content.
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.file_path().as_str())
        .await
        .unwrap();
    assert_eq!(
        loaded_arrow_batch, expected_arrow_batch,
        "Expected arrow data is {expected_arrow_batch:?}, actual data is {loaded_arrow_batch:?}"
    );

    let deleted_rows = deletion_vector
        .committed_deletion_vector
        .collect_deleted_rows();
    let expected_deleted_rows = vec![0_u64, 1_u64];
    assert_eq!(deleted_rows, expected_deleted_rows);

    // --------------------------------------
    // Operation series 4: append a new row, and don't delete any rows.
    // Expects to see the existing deletion vector unchanged and new data file created.
    let row4 = MoonlinkRow::new(vec![
        RowValue::Int32(4),
        RowValue::ByteArray("Tom".as_bytes().to_vec()),
        RowValue::Int32(40),
    ]);
    table.append(row4.clone()).unwrap();
    table.commit(/*flush_lsn=*/ 500);
    flush_table_and_sync(&mut table, &mut notify_rx, /*flush_lsn=*/ 500)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate and fresh new cache for each recovery.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, mut snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 5); // two data file, two index block file, one deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 2);
    assert_eq!(
        snapshot.indices.file_indices.len(),
        2,
        "Snapshot data files and file indices are {:?}",
        get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
    );
    check_row_index_nonexistent(&snapshot, &row1).await;
    check_row_index_on_disk(&snapshot, &row2, filesystem_accessor.as_ref()).await;
    check_row_index_on_disk(&snapshot, &row3, filesystem_accessor.as_ref()).await;
    check_row_index_on_disk(&snapshot, &row4, filesystem_accessor.as_ref()).await;
    assert_eq!(snapshot.flush_lsn.unwrap(), 500);

    let (file_in_new_snapshot, _) = snapshot
        .disk_files
        .iter()
        .find(|(path, _)| path.file_path() == loaded_path.file_path())
        .unwrap();
    snapshot.disk_files.remove(&file_in_new_snapshot.file_id());

    // Check new data file is correctly managed by iceberg table with no deletion vector.
    let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.file_path().as_str())
        .await
        .unwrap();

    let expected_arrow_batch = RecordBatch::try_new(
        create_test_arrow_schema(),
        // row4
        vec![
            Arc::new(Int32Array::from(vec![4])),
            Arc::new(StringArray::from(vec!["Tom"])),
            Arc::new(Int32Array::from(vec![40])),
        ],
    )
    .unwrap();
    assert_eq!(loaded_arrow_batch, expected_arrow_batch);

    let deleted_rows = deletion_vector
        .committed_deletion_vector
        .collect_deleted_rows();
    assert!(
        deleted_rows.is_empty(),
        "The new appended data file should have no deletion vector aside, but actually it contains deletion vector {deleted_rows:?}"
    );
}

#[tokio::test]
async fn test_filesystem_sync_snapshots() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    mooncake_table_snapshot_persist_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_object_storage_sync_snapshots_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    mooncake_table_snapshot_persist_impl(iceberg_table_config.clone()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_object_storage_sync_snapshots_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    mooncake_table_snapshot_persist_impl(iceberg_table_config.clone()).await;
}

/// ================================
/// Test table creation
/// ================================
///
/// Testing scenrio: after table creation, table schema (especially field ids assignment) match arrow schema.
async fn test_schema_for_table_creation_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(table_temp_dir.path().to_str().unwrap().to_string());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    // Append, commit, flush and persist.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache.clone(),
    )
    .await;
    let row = test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 10);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 10)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Now the iceberg table has been created, create an iceberg table manager and check table status.
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor,
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();

    // Load table metadata and verify schema.
    let namespace_ident =
        NamespaceIdent::from_strs(iceberg_table_config.namespace.clone()).unwrap();
    let table_ident = TableIdent::new(namespace_ident, iceberg_table_config.table_name.clone());
    let table = iceberg_table_manager
        .catalog
        .load_table(&table_ident)
        .await
        .unwrap();
    let actual_schema = table.metadata().current_schema();
    let expected_schema = arrow_schema_to_schema(mooncake_table_metadata.schema.as_ref()).unwrap();
    assert_is_same_schema(actual_schema.as_ref().clone(), expected_schema);
}

#[tokio::test]
async fn test_schema_for_table_creation() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    test_schema_for_table_creation_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_schema_for_table_creation_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_schema_for_table_creation_impl(iceberg_table_config.clone()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_schema_for_table_creation_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_schema_for_table_creation_impl(iceberg_table_config.clone()).await;
}

/// ================================
/// Test update schema with update
/// ================================
///
/// Testing scenario: perform a table schema update when there's no table update.
async fn test_schema_update_with_no_table_write_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let local_table_directory = table_temp_dir.path().to_str().unwrap().to_string();
    let mooncake_table_metadata = create_test_table_metadata(local_table_directory.clone());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    // Append, commit, flush and persist.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache.clone(),
    )
    .await;

    let updated_mooncake_table_metadata =
        alter_table_and_persist_to_iceberg(&mut table, &mut notify_rx).await;

    // Now the iceberg table has been created, create an iceberg table manager and check table status.
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        updated_mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor,
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 0);
    assert_eq!(snapshot.flush_lsn, Some(0));
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());

    let loaded_table = iceberg_table_manager_for_load
        .iceberg_table
        .as_ref()
        .unwrap();
    let actual_schema = loaded_table.metadata().current_schema();
    let expected_schema =
        arrow_schema_to_schema(updated_mooncake_table_metadata.schema.as_ref()).unwrap();
    assert_is_same_schema(actual_schema.as_ref().clone(), expected_schema);

    // =======================================
    // Table write after schema update
    // =======================================
    //
    // Perform more data file with the new schema should go through with no issue.
    let row = test_row_with_updated_schema();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 20);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 20)
        .await
        .unwrap();

    // Create a mooncake and iceberg snapshot to reflect new data file changes.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Now the iceberg table has been created, create an iceberg table manager and check table status.
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        updated_mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor,
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one file index
    assert_eq!(snapshot.flush_lsn, Some(20));
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(snapshot.indices.file_indices.len(), 1);

    let loaded_table = iceberg_table_manager_for_load
        .iceberg_table
        .as_ref()
        .unwrap();
    let actual_schema = loaded_table.metadata().current_schema();
    let expected_schema =
        arrow_schema_to_schema(updated_mooncake_table_metadata.schema.as_ref()).unwrap();
    assert_is_same_schema(actual_schema.as_ref().clone(), expected_schema);
}

#[tokio::test]
async fn test_schema_update_with_no_table_write() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    test_schema_update_with_no_table_write_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_schema_update_with_no_table_write_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_schema_update_with_no_table_write_impl(iceberg_table_config.clone()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_schema_update_with_no_table_write_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_schema_update_with_no_table_write_impl(iceberg_table_config.clone()).await;
}

/// ================================
/// Test update schema
/// ================================
///
/// Testing scenario: perform schema update after a sync operation.
async fn test_schema_update_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let local_table_directory = table_temp_dir.path().to_str().unwrap().to_string();
    let mooncake_table_metadata = create_test_table_metadata(local_table_directory.clone());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    // Append, commit, flush and persist.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache.clone(),
    )
    .await;
    let row = test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 10);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 10)
        .await
        .unwrap();

    // Perform an schema update.
    let updated_mooncake_table_metadata =
        alter_table_and_persist_to_iceberg(&mut table, &mut notify_rx).await;

    // Now the iceberg table has been created, create an iceberg table manager and check table status.
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        updated_mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor,
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2);
    assert_eq!(snapshot.flush_lsn, Some(10));
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(snapshot.indices.file_indices.len(), 1);

    let loaded_table = iceberg_table_manager_for_load
        .iceberg_table
        .as_ref()
        .unwrap();
    let actual_schema = loaded_table.metadata().current_schema();
    let expected_schema =
        arrow_schema_to_schema(updated_mooncake_table_metadata.schema.as_ref()).unwrap();
    assert_is_same_schema(actual_schema.as_ref().clone(), expected_schema);

    // Perform more data file with the new schema should go through with no issue.
    let row = test_row_with_updated_schema();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 20);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 20)
        .await
        .unwrap();

    // Create a mooncake and iceberg snapshot to reflect new data file changes.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Check iceberg snapshot after write following schema update.
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        updated_mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor,
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 4); // two data files, two file indices
    assert_eq!(snapshot.flush_lsn, Some(20));
    assert_eq!(snapshot.disk_files.len(), 2);
    assert_eq!(snapshot.indices.file_indices.len(), 2);
}

#[tokio::test]
async fn test_schema_update() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Common testing logic.
    test_schema_update_impl(iceberg_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_schema_update_with_s3() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_schema_update_impl(iceberg_table_config.clone()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_schema_update_with_gcs() {
    // Remote object storage for iceberg.
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);

    // Common testing logic.
    test_schema_update_impl(iceberg_table_config.clone()).await;
}

/// ================================
/// Test deletion record remap for compaction
/// ================================
///
/// Committed deletion record could live in either committed deletion logs, or iceberg puffin file.
/// This test verifies remap on already persisted deletion logs.
#[tokio::test]
async fn test_persisted_deletion_record_remap() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let local_table_directory = table_temp_dir.path().to_str().unwrap().to_string();

    // Create mooncake metadata.
    let file_index_config = FileIndexMergeConfig::disabled();
    let data_compaction_config = DataCompactionConfig {
        min_data_file_to_compact: 2,
        max_data_file_to_compact: u32::MAX,
        data_file_final_size: u64::MAX,
        data_file_deletion_percentage: 0,
    };
    let iceberg_persistence_config = IcebergPersistenceConfig {
        new_data_file_count: 1,
        new_committed_deletion_log: 1,
        new_compacted_data_file_count: 1,
        old_compacted_data_file_count: 1,
        old_merged_file_indices_count: usize::MAX,
    };
    let mut config = MooncakeTableConfig::new(table_temp_dir.path().to_str().unwrap().to_string());
    config.file_index_config = file_index_config;
    config.data_compaction_config = data_compaction_config;
    config.persistence_config = iceberg_persistence_config;
    let mooncake_table_metadata =
        create_test_table_metadata_with_config(local_table_directory, config);

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache.clone(),
    )
    .await;
    let row = test_row_1();

    // Data file and deletion records setup:
    //
    // disk file 1:
    // one row, in mooncake and iceberg
    // deleted, in batch deletion vector and puffin
    //
    // disk file 2:
    // one row, in mooncake and iceberg
    // deleted, in batch deletion vector but not puffin
    //
    // disk file 3:
    // one row, not in iceberg
    // no deletion
    //
    // Initial append.
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 1)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Overwrite.
    table.delete(row.clone(), /*lsn=*/ 2).await;
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 3);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 3)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Overwrite.
    table.delete(row.clone(), /*lsn=*/ 4).await;
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 5);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 5)
        .await
        .unwrap();
    let (_, iceberg_payload, _, data_compaction_payload, _) =
        create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;

    // Initiate an iceberg snapshot.
    table.persist_iceberg_snapshot(iceberg_payload.unwrap());

    // Perform data compaction.
    let data_compaction_payload = data_compaction_payload.take_payload().unwrap();
    table.perform_data_compaction(data_compaction_payload);

    // Block wait both operations to finish.
    let mut stored_data_compaction_result: Option<DataCompactionResult> = None;
    let mut stored_iceberg_snapshot_result: Option<IcebergSnapshotResult> = None;

    for _ in 0..2 {
        let notification = notify_rx.recv().await.unwrap();
        if let TableEvent::DataCompactionResult {
            data_compaction_result,
        } = notification
        {
            assert!(stored_data_compaction_result.is_none());
            stored_data_compaction_result = Some(data_compaction_result.unwrap());
        } else if let TableEvent::IcebergSnapshotResult {
            iceberg_snapshot_result,
        } = notification
        {
            assert!(stored_iceberg_snapshot_result.is_none());
            stored_iceberg_snapshot_result = Some(iceberg_snapshot_result.unwrap());
        } else {
            panic!(
                "Expect either iceberg snapshot result and data compaction result but get {notification:?}"
            );
        }
    }
    assert!(stored_data_compaction_result.is_some());
    assert!(stored_iceberg_snapshot_result.is_some());

    // Reflect iceberg snapshot result to mooncake snapshot.
    table.set_iceberg_snapshot_res(stored_iceberg_snapshot_result.unwrap());

    // Create mooncake snapshot and sync.
    create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;

    // Reflect data compaction result.
    table.set_data_compaction_res(stored_data_compaction_result.unwrap());

    // Create mooncake snapshot and sync.
    let (_, iceberg_payload, _, _, _) =
        create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;
    assert!(iceberg_payload.is_some());

    // Create iceberg snapshot and sync.
    let iceberg_snapshot_result =
        create_iceberg_snapshot(&mut table, iceberg_payload, &mut notify_rx)
            .await
            .unwrap();
    table.set_iceberg_snapshot_res(iceberg_snapshot_result);

    // Validate iceberg snapshot content.
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(&cache_temp_dir), // Use separate cache for each table.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (_, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();

    // Validate iceberg snapshot.
    verify_recovered_mooncake_snapshot(&snapshot, /*expected_ids=*/ &[1]).await;
}

/// ================================
/// Test iceberg snapshot creation with partial stream flush results
/// ================================
///
/// Testing scenario and event stream:
/// - a non-streaming transaction finishes
/// - start a streaming transaction, start an async flush
/// - async flush finishes
/// - commit the streaming transaction, still async flush ongoing
/// - create mooncake and iceberg snapshot
///
/// Linked issue: https://github.com/Mooncake-Labs/moonlink/issues/1946
async fn test_async_flush_for_streaming_partial_finish_impl(
    iceberg_table_config: IcebergTableConfig,
) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(table_temp_dir.path().to_str().unwrap().to_string());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    // Create mooncake table and table event notification receiver.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache.clone(),
    )
    .await;

    // Perform a non-streaming append, commit and flush.
    let row1 = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("Alice".as_bytes().to_vec()),
        RowValue::Int32(10),
    ]);
    table.append(row1.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 1)
        .await
        .unwrap();

    // Perform a streaming append and flush, but not commit.
    let row2 = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table
        .append_in_stream_batch(row2.clone(), /*xact_id=*/ 1)
        .unwrap();
    let disk_slice = flush_stream_and_sync_no_apply(
        &mut table,
        &mut notify_rx,
        /*xact_id=*/ 1,
        /*lsn=*/ None,
    )
    .await
    .unwrap();

    // Perform a streaming append, flush and commit.
    let row3 = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table
        .append_in_stream_batch(row3.clone(), /*xact_id=*/ 1)
        .unwrap();
    table
        .commit_transaction_stream(
            /*xact_id=*/ 1,
            /*lsn=*/ 2,
            /*event_id=*/ uuid::Uuid::new_v4(),
        )
        .unwrap();
    // Block wait its completion but not apply it to snapshot buffer.
    let _ = get_flush_results(&mut notify_rx, /*expected_flushes=*/ 1).await;

    // Apply the first flush result to snapshot buffer.
    table.apply_stream_flush_result(
        /*xact_id=*/ 1,
        disk_slice,
        /*flush_event_id=*/ uuid::Uuid::new_v4(),
    );

    // Create mooncake and iceberg snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate iceberg snapshot content.
    verify_iceberg_content(
        iceberg_table_config,
        mooncake_table_metadata,
        &cache_temp_dir,
        /*expected_ids=*/ &[],
    )
    .await;
}

#[tokio::test]
async fn test_async_flush_for_streaming_partial_finish() {
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);
    test_async_flush_for_streaming_partial_finish_impl(iceberg_table_config).await;
}

/// ================================
/// Test iceberg snapshot creation with completely reversed order of stream flush
/// ================================
///
/// Testing scenario and event stream:
/// - a non-streaming transaction finishes
/// - start streaming txn 1, async flush, and commit
/// - start streaming txn 2, async flush, commit and complete
/// - commit the streaming transaction, still async flush ongoing
/// - create mooncake and iceberg snapshot
async fn test_reversed_order_of_completed_streaming_flush_impl(
    iceberg_table_config: IcebergTableConfig,
) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(table_temp_dir.path().to_str().unwrap().to_string());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    // Create mooncake table and table event notification receiver.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache.clone(),
    )
    .await;

    // Perform a non-streaming append, commit and flush.
    let row1 = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("Alice".as_bytes().to_vec()),
        RowValue::Int32(10),
    ]);
    table.append(row1.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 1)
        .await
        .unwrap();

    // Perform a streaming append and commit, but not apply.
    let row2 = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table
        .append_in_stream_batch(row2.clone(), /*xact_id=*/ 2)
        .unwrap();
    table
        .commit_transaction_stream(
            /*xact_id=*/ 2,
            /*lsn=*/ 2,
            /*event_id=*/ uuid::Uuid::new_v4(),
        )
        .unwrap();
    // Block wait its completion but not apply it to snapshot buffer.
    let _ = get_flush_results(&mut notify_rx, /*expected_flushes=*/ 1).await;

    // Perform a streaming append, commit, and apply.
    let row3 = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table
        .append_in_stream_batch(row3.clone(), /*xact_id=*/ 3)
        .unwrap();
    table
        .commit_transaction_stream(
            /*xact_id=*/ 3,
            /*lsn=*/ 3,
            /*event_id=*/ uuid::Uuid::new_v4(),
        )
        .unwrap();
    // Block wait its completion but not apply it to snapshot buffer.
    let disk_slices = get_flush_results(&mut notify_rx, /*expected_flushes=*/ 1).await;
    let disk_slice = disk_slices.into_values().next().unwrap();
    assert_eq!(*disk_slice.lsn().as_ref().unwrap(), 3);
    table.apply_stream_flush_result(
        /*xact_id=*/ 3,
        disk_slice,
        /*flush_event_id=*/ uuid::Uuid::new_v4(),
    );

    // Create mooncake and iceberg snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate iceberg snapshot content.
    verify_iceberg_content(
        iceberg_table_config,
        mooncake_table_metadata,
        &cache_temp_dir,
        /*expected_ids=*/ &[],
    )
    .await;
}

#[tokio::test]
async fn test_reversed_order_of_completed_streaming_flush() {
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);
    test_reversed_order_of_completed_streaming_flush_impl(iceberg_table_config).await;
}

/// ================================
/// Test iceberg snapshot creation with completely ordered stream flush
/// ================================
///
/// Testing scenario and event stream:
/// - a non-streaming transaction finishes
/// - start streaming txn 1, async flush, commit
/// - start streaming txn 2, async flush, commit
/// - complete txn 1 stream flush and apply
/// - create mooncake and iceberg snapshot
async fn test_ordered_completed_streaming_flush_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(table_temp_dir.path().to_str().unwrap().to_string());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    // Create mooncake table and table event notification receiver.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache.clone(),
    )
    .await;

    // Perform a non-streaming append, commit and flush.
    let row1 = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("Alice".as_bytes().to_vec()),
        RowValue::Int32(10),
    ]);
    table.append(row1.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 1)
        .await
        .unwrap();

    // Perform a streaming append and commit, but not apply.
    let row2 = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table
        .append_in_stream_batch(row2.clone(), /*xact_id=*/ 2)
        .unwrap();
    table
        .commit_transaction_stream(
            /*xact_id=*/ 2,
            /*lsn=*/ 2,
            /*event_id=*/ uuid::Uuid::new_v4(),
        )
        .unwrap();
    // Block wait its completion but not apply it to snapshot buffer.
    let disk_slices = get_flush_results(&mut notify_rx, /*expected_flushes=*/ 1).await;
    let disk_slice = disk_slices.into_values().next().unwrap();
    assert_eq!(*disk_slice.lsn().as_ref().unwrap(), 2);

    // Perform a streaming append, commit, and apply.
    let row3 = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table
        .append_in_stream_batch(row3.clone(), /*xact_id=*/ 3)
        .unwrap();
    table
        .commit_transaction_stream(
            /*xact_id=*/ 3,
            /*lsn=*/ 3,
            /*event_id=*/ uuid::Uuid::new_v4(),
        )
        .unwrap();
    // Block wait its completion but not apply it to snapshot buffer.
    let _ = get_flush_results(&mut notify_rx, /*expected_flushes=*/ 1).await;

    // Apply the first arriving flush result.
    table.apply_stream_flush_result(
        /*xact_id=*/ 2,
        disk_slice,
        /*flush_event_id=*/ uuid::Uuid::new_v4(),
    );

    // Create mooncake and iceberg snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate iceberg snapshot content.
    verify_iceberg_content(
        iceberg_table_config,
        mooncake_table_metadata,
        &cache_temp_dir,
        /*expected_ids=*/ &[1, 2],
    )
    .await;
}

#[tokio::test]
async fn test_ordered_completed_streaming_flush() {
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);
    test_ordered_completed_streaming_flush_impl(iceberg_table_config).await;
}

/// ================================
/// Test iceberg snapshot creation with partial completed flush before streaming commit
/// ================================
///
/// Testing scenario and event stream:
/// - a non-streaming transaction finishes
/// - start streaming txn, async flush, finish
/// - async flush, not finished
/// - commit transaction
/// - create mooncake and iceberg snapshot
async fn test_partial_completed_streaming_flush_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(table_temp_dir.path().to_str().unwrap().to_string());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    // Create mooncake table and table event notification receiver.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache.clone(),
    )
    .await;

    // Perform a non-streaming append, commit and flush.
    let row1 = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("Alice".as_bytes().to_vec()),
        RowValue::Int32(10),
    ]);
    table.append(row1.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 1)
        .await
        .unwrap();

    // Perform a streaming append and block wait its completion, but not apply.
    let row2 = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table
        .append_in_stream_batch(row2.clone(), /*xact_id=*/ 2)
        .unwrap();
    table
        .flush_stream(
            /*xact_id=*/ 2,
            /*lsn=*/ None,
            /*event_id=*/ uuid::Uuid::new_v4(),
        )
        .unwrap();
    // Block wait its completion but and apply it to snapshot buffer.
    let disk_slices = get_flush_results(&mut notify_rx, /*expected_flushes=*/ 1).await;
    let disk_slice = disk_slices.into_values().next().unwrap();
    assert!(disk_slice.lsn().is_none());
    // Apply the first arriving flush result.
    table.apply_stream_flush_result(
        /*xact_id=*/ 2,
        disk_slice,
        /*flush_event_id=*/ uuid::Uuid::new_v4(),
    );

    // Commit streaming transaction, which internally flushes but not wait its completion.
    let row3 = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table
        .append_in_stream_batch(row3.clone(), /*xact_id=*/ 2)
        .unwrap();
    table
        .commit_transaction_stream(
            /*xact_id=*/ 2,
            /*lsn=*/ 2,
            /*event_id=*/ uuid::Uuid::new_v4(),
        )
        .unwrap();
    // Block wait its completion but not apply it to snapshot buffer.
    let _ = get_flush_results(&mut notify_rx, /*expected_flushes=*/ 1).await;

    // Create mooncake and iceberg snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate iceberg snapshot content.
    verify_iceberg_content(
        iceberg_table_config,
        mooncake_table_metadata,
        &cache_temp_dir,
        /*expected_ids=*/ &[],
    )
    .await;
}

#[tokio::test]
async fn test_partial_completed_streaming_flush() {
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);
    test_partial_completed_streaming_flush_impl(iceberg_table_config).await;
}
