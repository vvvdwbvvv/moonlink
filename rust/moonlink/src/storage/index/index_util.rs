use super::test_in_memory_index::MemIndex;
use pg_replicate::conversions::table_row::TableRow;
use pg_replicate::conversions::Cell;
use std::path::PathBuf;
#[derive(Debug, Clone, PartialEq)]
pub enum RecordLocation {
    /// Record is in a memory batch
    /// (batch_id, row_offset)
    MemoryBatch(usize, usize),

    /// Record is in a disk file
    /// (file_path, row_offset)
    DiskFile(PathBuf, usize),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordIdentity {}

#[derive(Clone, Debug)]
pub struct RawDeletionRecord {
    pub(crate) lookup_key: i64,
    pub(crate) row_identity: Option<RecordIdentity>,
    pub(crate) pos: Option<(usize, usize)>,
    pub(crate) lsn: u64,
}

#[derive(Clone, Debug)]
pub struct ProcessedDeletionRecord {
    pub(crate) lookup_key: i64,
    pub(crate) pos: RecordLocation,
    pub(crate) lsn: u64,
}

impl Into<(usize, usize)> for RecordLocation {
    fn into(self) -> (usize, usize) {
        match self {
            RecordLocation::MemoryBatch(batch_id, row_offset) => (batch_id, row_offset),
            _ => panic!("Cannot convert RecordLocation to (usize, usize)"),
        }
    }
}

impl From<(usize, usize)> for RecordLocation {
    fn from(value: (usize, usize)) -> Self {
        RecordLocation::MemoryBatch(value.0, value.1)
    }
}

pub trait Index: Send + Sync {
    fn find_record(&self, raw_record: &RawDeletionRecord) -> Option<RecordLocation>;

    fn insert(&mut self, key: i64, location: RecordLocation);
}

pub fn create_index() -> Box<dyn Index> {
    Box::new(MemIndex::new())
}

pub fn get_lookup_key(row: &TableRow) -> i64 {
    // For now in testing, we assume the primary key is the first column!
    //
    match row.values[0] {
        Cell::I32(value) => value as i64,
        Cell::I64(value) => value,
        _ => todo!("Handle other types of primary keys"),
    }
}
