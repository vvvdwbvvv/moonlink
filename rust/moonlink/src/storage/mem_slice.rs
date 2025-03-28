use crate::error::Result;
use crate::storage::data_batches::{BatchEntry, ColumnStoreBuffer};
use crate::storage::index::index_util::{create_index, Index};
use crate::storage::table_utils::{RawDeletionRecord, RecordLocation};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use pg_replicate::conversions::table_row::TableRow;
use std::mem::swap;
use std::sync::Arc;

/// MemSlice is a table slice that is stored in memory.
/// It contains a column store buffer for storing data
///
/// MemSlice is Copy-On-Read
/// Reader will create a snapshot of the current state of the table,
/// by applying all deletions to column store buffer
///
pub struct MemSlice {
    /// Column store buffer for storing data
    ///
    column_store: ColumnStoreBuffer,

    /// Mem index for the table
    ///
    mem_index: Box<dyn Index>,
}

impl MemSlice {
    pub fn new(schema: Arc<Schema>, max_rows_per_buffer: usize) -> Self {
        Self {
            column_store: ColumnStoreBuffer::new(schema, max_rows_per_buffer),
            mem_index: create_index(),
        }
    }

    pub fn delete(&mut self, record: &RawDeletionRecord) -> Option<(u64, usize)> {
        let res: Option<(u64, usize)> = self.mem_index.find_record(&record).map(Into::into);
        if let Some(pos) = res {
            self.column_store.delete(pos);
        }
        res
    }

    pub fn append(
        &mut self,
        primary_key: i64,
        row: &TableRow,
    ) -> Result<Option<(u64, Arc<RecordBatch>)>> {
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
    ) -> Result<(
        Option<(u64, Arc<RecordBatch>)>,
        Vec<BatchEntry>,
        Box<dyn Index>,
    )> {
        let batch = self.column_store.finalize_current_batch()?;
        let entries = self.column_store.flush();
        let mut index = create_index();
        swap(&mut index, &mut self.mem_index);
        Ok((batch, entries, index))
    }

    pub fn get_commit_check_point(&self) -> RecordLocation {
        self.column_store.get_commit_check_point()
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
        assert_eq!(
            mem_table.delete(&RawDeletionRecord {
                lookup_key: 2,
                lsn: 0,
                pos: None,
                row_identity: None
            }),
            Some((0, 1))
        );
        assert_eq!(
            mem_table.delete(&RawDeletionRecord {
                lookup_key: 3,
                lsn: 0,
                pos: None,
                row_identity: None
            }),
            Some((0, 2))
        );
        assert_eq!(
            mem_table.delete(&RawDeletionRecord {
                lookup_key: 1,
                lsn: 0,
                pos: None,
                row_identity: None
            }),
            Some((0, 0))
        );
    }
}
