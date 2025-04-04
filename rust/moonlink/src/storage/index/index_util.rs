use crate::row::{MoonlinkRow, RowValue};
use crate::storage::table_utils::{RawDeletionRecord, RecordLocation};
use multimap::MultiMap;
use std::sync::Arc;
pub trait Index: Send + Sync {
    fn find_record(&self, raw_record: &RawDeletionRecord) -> Option<Vec<&RecordLocation>>;
}

/// Type for primary keys
pub type PrimaryKey = i64;

/// Index containing records in memory
pub type MemIndex = MultiMap<PrimaryKey, RecordLocation>; // key -> (batch_id, row_offset)
/// Index containing records in files
pub type FileIndex = MultiMap<PrimaryKey, RecordLocation>; // key -> (batch_id, row_offset)

pub struct ParquetFileIndex {
    pub index: FileIndex,
}

pub struct MooncakeIndex {
    pub in_memory_index: Option<Arc<MemIndex>>,
    pub file_indices: Vec<ParquetFileIndex>,
}

pub fn get_lookup_key(row: &MoonlinkRow) -> i64 {
    // UNDONE(REPLICATION IDENTITY):
    // For now in testing, we assume the primary key is the first column!

    match row.values[0] {
        RowValue::Int32(value) => value as i64,

        RowValue::Int64(value) => value,

        _ => todo!("Handle other types of primary keys"),
    }
}
