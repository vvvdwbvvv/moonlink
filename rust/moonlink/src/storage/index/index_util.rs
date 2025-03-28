use super::test_in_memory_index::MemIndex;
use crate::storage::table_utils::{RawDeletionRecord, RecordLocation};
use pg_replicate::conversions::table_row::TableRow;
use pg_replicate::conversions::Cell;
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
