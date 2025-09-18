use super::*;
use crate::row::{IdentityProp, RowValue};
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::iceberg::deletion_vector::DeletionVector as IcebergDeletionVector;
use crate::storage::iceberg::iceberg_table_config::IcebergTableConfig;
use crate::storage::iceberg::puffin_utils;
use crate::storage::mooncake_table::snapshot_read_output::DataFileForRead;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::storage::wal::test_utils::WAL_TEST_TABLE_ID;
use crate::storage::wal::WalManager;
use crate::{StorageConfig, WalConfig};
use arrow::array::Int32Array;
use arrow_array::Array;
use futures::future::join_all;
use iceberg::io::FileIOBuilder;
use moonlink_table_metadata::{DeletionVector, PositionDelete};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use tempfile::{tempdir, TempDir};

pub struct TestContext {
    pub temp_dir: TempDir,
    pub test_dir: PathBuf,
}

impl TestContext {
    pub fn new(subdir_name: &str) -> Self {
        let temp_dir = tempdir().unwrap();
        let test_dir = temp_dir.path().join(subdir_name);
        create_dir_all(&test_dir).unwrap();

        Self { temp_dir, test_dir }
    }

    pub fn path(&self) -> PathBuf {
        self.test_dir.clone()
    }
}

pub fn test_row(id: i32, name: &str, age: i32) -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(id),
        RowValue::ByteArray(name.as_bytes().to_vec()),
        RowValue::Int32(age),
    ])
}

/// Test util function to get iceberg table config for testing purpose.
pub fn test_iceberg_table_config(context: &TestContext, table_name: &str) -> IcebergTableConfig {
    let root_directory = context.path().to_str().unwrap().to_string();
    let storage_config = StorageConfig::FileSystem {
        root_directory,
        atomic_write_dir: None,
    };
    IcebergTableConfig {
        namespace: vec!["default".to_string()],
        table_name: table_name.to_string(),
        data_accessor_config: AccessorConfig::new_with_storage_config(storage_config.clone()),
        metadata_accessor_config: crate::IcebergCatalogConfig::File {
            accessor_config: AccessorConfig::new_with_storage_config(storage_config),
        },
    }
}

/// Test util function to get mooncake table config.
pub fn test_mooncake_table_config(context: &TestContext) -> MooncakeTableConfig {
    MooncakeTableConfig::new(context.temp_dir.path().to_str().unwrap().to_string())
}

pub async fn test_table(
    context: &TestContext,
    table_name: &str,
    identity: IdentityProp,
) -> MooncakeTable {
    let iceberg_table_config = test_iceberg_table_config(context, table_name);
    let mut table_config = test_mooncake_table_config(context);
    table_config.batch_size = 2;
    table_config.row_identity = identity;
    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path());
    let wal_manager = WalManager::new(&wal_config);
    MooncakeTable::new(
        (*create_test_arrow_schema()).clone(),
        table_name.to_string(),
        1,
        context.path(),
        iceberg_table_config.clone(),
        table_config,
        wal_manager,
        create_test_object_storage_cache(&context.temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap()
}

pub async fn test_append_only_table(context: &TestContext, table_name: &str) -> MooncakeTable {
    // Append-only table requires IdentityProp::None and append_only=true
    let iceberg_table_config = test_iceberg_table_config(context, table_name);
    let mut table_config = test_mooncake_table_config(context);
    table_config.batch_size = 2;
    table_config.append_only = true;
    table_config.row_identity = IdentityProp::None;
    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path());
    let wal_manager = WalManager::new(&wal_config);
    MooncakeTable::new(
        (*create_test_arrow_schema()).clone(),
        table_name.to_string(),
        1,
        context.path(),
        iceberg_table_config.clone(),
        table_config,
        wal_manager,
        create_test_object_storage_cache(&context.temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap()
}

// TODO(hjiang): Support object storage.
pub fn read_ids_from_parquet(file_path: &String) -> Vec<Option<i32>> {
    let file = File::open(file_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let mut ids = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        ids.extend((0..col.len()).map(|i| {
            if col.is_null(i) {
                None
            } else {
                Some(col.value(i))
            }
        }));
    }

    ids
}

pub fn verify_file_contents(
    file_path: &String,
    expected_ids: &[i32],
    expected_row_count: Option<usize>,
) {
    let actual = read_ids_from_parquet(file_path)
        .into_iter()
        .flatten()
        .collect::<HashSet<_>>();
    let expected: HashSet<_> = expected_ids.iter().copied().collect();

    if let Some(count) = expected_row_count {
        assert_eq!(actual.len(), count, "Unexpected number of rows in file");
    }

    assert_eq!(actual, expected, "File contents don't match expected IDs");
}

pub fn append_rows(table: &mut MooncakeTable, rows: Vec<MoonlinkRow>) -> Result<()> {
    for row in rows {
        table.append(row)?;
    }
    Ok(())
}

pub fn batch_rows(start_id: i32, count: i32) -> Vec<MoonlinkRow> {
    (start_id..start_id + count)
        .map(|id| test_row(id, &format!("Row {id}"), 30 + id))
        .collect()
}

pub async fn append_commit_flush_create_mooncake_snapshot_for_test(
    table: &mut MooncakeTable,
    completion_rx: &mut Receiver<TableEvent>,
    rows: Vec<MoonlinkRow>,
    lsn: u64,
) -> Result<()> {
    append_rows(table, rows)?;
    table.commit(lsn);
    flush_table_and_sync(table, completion_rx, lsn).await?;
    create_mooncake_snapshot_for_test(table, completion_rx).await;
    Ok(())
}

fn verify_files_and_deletions_impl(
    files: &[String],
    deletions: &[PositionDelete],
    expected_ids: &[i32],
) {
    let mut res = vec![];
    for (i, path) in files.iter().enumerate() {
        let mut ids = read_ids_from_parquet(path);
        for deletion in deletions {
            if deletion.data_file_number == i as u32 {
                ids[deletion.data_file_row_number as usize] = None;
            }
        }
        res.extend(ids.into_iter().flatten());
    }
    res.sort();
    assert_eq!(res, expected_ids);
}

pub fn get_data_files_for_read(data_file_paths: &[DataFileForRead]) -> Vec<String> {
    data_file_paths
        .iter()
        .map(|data_file| data_file.get_file_path())
        .collect::<Vec<_>>()
}

pub fn get_deletion_puffin_files_for_read(
    deletion_vector_puffins: &[NonEvictableHandle],
) -> Vec<String> {
    deletion_vector_puffins
        .iter()
        .map(|cache_handle| cache_handle.get_cache_filepath().to_string())
        .collect::<Vec<_>>()
}

pub async fn verify_files_and_deletions(
    data_file_paths: &[String],
    puffin_file_paths: &[String],
    position_deletes: Vec<PositionDelete>,
    deletion_vectors: Vec<DeletionVector>,
    expected_ids: &[i32],
) {
    // Read deletion vector blobs and add to position deletes.
    let file_io = FileIOBuilder::new_fs_io().build().unwrap();
    let mut position_deletes = position_deletes;
    let mut load_blob_futures = Vec::with_capacity(deletion_vectors.len());
    for cur_blob in deletion_vectors.iter() {
        let get_blob_future = puffin_utils::load_blob_from_puffin_file(
            file_io.clone(),
            puffin_file_paths
                .get(cur_blob.puffin_file_number as usize)
                .unwrap(),
        );
        load_blob_futures.push(get_blob_future);
    }

    let load_blob_results = join_all(load_blob_futures).await;
    for (idx, cur_load_blob_res) in load_blob_results.into_iter().enumerate() {
        let blob = cur_load_blob_res.unwrap();
        let dv = IcebergDeletionVector::deserialize(blob).unwrap();
        let batch_deletion_vector = dv.take_as_batch_delete_vector();
        let deleted_rows = batch_deletion_vector.collect_deleted_rows();
        assert!(!deleted_rows.is_empty());
        position_deletes.append(
            &mut deleted_rows
                .iter()
                .map(|row_idx| PositionDelete {
                    data_file_number: deletion_vectors[idx].data_file_number,
                    data_file_row_number: *row_idx as u32,
                })
                .collect::<Vec<PositionDelete>>(),
        );
    }

    verify_files_and_deletions_impl(data_file_paths, &position_deletes, expected_ids)
}
