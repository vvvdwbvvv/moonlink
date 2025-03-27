use crate::error::Result;
use crate::storage::data_batches::{BatchEntry, ColumnStoreBuffer, CommitCheckPoint};
use crate::storage::delete_buffer::{DeletionBuffer, DeletionRecord};
use crate::storage::index::test_in_memory_index::MemIndex;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use pg_replicate::conversions::table_row::TableRow;
use std::mem::swap;
use std::sync::Arc;

/// MemSlice is a table slice that is stored in memory.
/// It contains a column store buffer for storing data
/// and a delete buffer for tracking deletions from the stream.
///
/// MemSlice is Copy-On-Read
/// Reader will create a snapshot of the current state of the table,
/// by applying all deletions to column store buffer
///
pub struct MemSlice {
    /// Column store buffer for storing data
    ///
    column_store: ColumnStoreBuffer,

    /// Delete buffer for tracking deletions from the stream
    ///
    delete_buffer: DeletionBuffer,

    /// Mem index for the table
    ///
    mem_index: MemIndex,
}

impl MemSlice {
    pub fn new(schema: Arc<Schema>, max_rows_per_buffer: usize) -> Self {
        Self {
            column_store: ColumnStoreBuffer::new(schema, max_rows_per_buffer),
            delete_buffer: DeletionBuffer::new(),
            mem_index: MemIndex::new(),
        }
    }

    pub fn delete(&mut self, primary_key: i64, lsn: u64) {
        let pos: Option<(usize, usize)> = self.mem_index.remove(&primary_key).map(Into::into);
        self.delete_buffer.delete(primary_key, pos, lsn);
        if let Some(pos) = pos {
            self.column_store.delete(pos);
        }
    }

    pub fn append(
        &mut self,
        primary_key: i64,
        row: &TableRow,
    ) -> Result<Option<(usize, Arc<RecordBatch>)>> {
        let (seg_idx, row_idx, new_batch) = self.column_store.append_row(row)?;
        self.mem_index
            .insert(primary_key, (seg_idx, row_idx).into());
        Ok(new_batch)
    }

    pub fn get_num_rows(&self) -> usize {
        self.column_store.get_num_rows()
    }

    pub fn flush(
        &mut self,
    ) -> Result<(Option<(usize, Arc<RecordBatch>)>, Vec<BatchEntry>, MemIndex)> {
        let batch = self.column_store.finalize_current_batch()?;
        let entries = self.column_store.flush();
        let mut index = MemIndex::new();
        swap(&mut index, &mut self.mem_index);
        Ok((batch, entries, index))
    }

    pub fn get_commit_check_point(&self) -> CommitCheckPoint {
        self.column_store.get_commit_check_point()
    }

    pub fn get_deletions(&self, start_lsn: u64, end_lsn: u64) -> &[DeletionRecord] {
        self.delete_buffer.get_deletions(start_lsn, end_lsn)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use arrow::datatypes::Schema;
    use arrow::datatypes::{DataType, Field};
    use pg_replicate::conversions::Cell;

    #[test]
    fn test_mem_slice() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);
        let mut mem_table = MemSlice::new(Arc::new(schema), 4);

        // Create arrays properly
        mem_table
            .append(
                1,
                &TableRow {
                    values: vec![
                        Cell::I32(1),
                        Cell::String("John".to_string()),
                        Cell::I32(30),
                    ],
                },
            )
            .unwrap();

        mem_table
            .append(
                2,
                &TableRow {
                    values: vec![
                        Cell::I32(2),
                        Cell::String("Jane".to_string()),
                        Cell::I32(25),
                    ],
                },
            )
            .unwrap();

        mem_table
            .append(
                3,
                &TableRow {
                    values: vec![Cell::I32(3), Cell::String("foo".to_string()), Cell::I32(40)],
                },
            )
            .unwrap();
        mem_table.delete(2, 1);
        mem_table.delete(3, 1);
        mem_table.delete(1, 2);

        println!("delete_buffer: {:?}", mem_table.delete_buffer);
        println!("get_deletions: {:?}", mem_table.get_deletions(0, 1));
        assert_eq!(mem_table.get_deletions(0, 0).len(), 0);
        assert_eq!(mem_table.get_deletions(0, 1).len(), 2);
        assert_eq!(mem_table.get_deletions(0, 2).len(), 3);

        assert_eq!(mem_table.mem_index.len(), 0);
    }
}
