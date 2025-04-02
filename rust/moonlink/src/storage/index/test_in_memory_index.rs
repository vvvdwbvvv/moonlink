use crate::storage::index::index_util::{Index, MemIndex, MooncakeIndex, ParquetFileIndex};
use crate::storage::table_utils::{RawDeletionRecord, RecordLocation};
use std::sync::Arc;
impl Index for MemIndex {
    fn find_record(&self, raw_record: &RawDeletionRecord) -> Option<Vec<&RecordLocation>> {
        self.get_vec(&raw_record.lookup_key)
            .map(|v| v.iter().collect())
    }
}

// UNDONE(The index is just a placeholder, it is all in memory and not persisted)
impl MooncakeIndex {
    /// Create a new, empty in-memory index
    pub fn new() -> Self {
        Self {
            in_memory_index: None,
            file_indices: Vec::new(),
        }
    }

    /// Insert a memory index (batch of in-memory records)
    ///
    pub fn insert_memory_index(&mut self, mem_index: Arc<MemIndex>) {
        assert!(
            self.in_memory_index.is_none(),
            "Currently we don't allow multiple concurrent flushes,
            so there should be only one memory index at a time"
        );
        self.in_memory_index = Some(mem_index);
    }

    pub fn delete_memory_index(&mut self) {
        self.in_memory_index = None;
    }

    /// Insert a file index (batch of on-disk records)
    ///
    /// This adds a new file index to the collection of file indices
    pub fn insert_file_index(&mut self, file_index: ParquetFileIndex) {
        self.file_indices.push(file_index);
    }
}

impl Index for MooncakeIndex {
    fn find_record(&self, raw_record: &RawDeletionRecord) -> Option<Vec<&RecordLocation>> {
        let mut res = Vec::new();

        // Check in-memory index
        if self.in_memory_index.is_some() {
            if let Some(locations) = self
                .in_memory_index
                .as_ref()
                .unwrap()
                .get_vec(&raw_record.lookup_key)
            {
                res.extend(locations.iter());
            }
        }

        // Check file indices
        for file_index_meta in &self.file_indices {
            if let Some(locations) = file_index_meta.index.get_vec(&raw_record.lookup_key) {
                res.extend(locations.iter());
            }
        }

        if res.is_empty() {
            None
        } else {
            Some(res)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_in_memory_index_basic() {
        let mut index = MooncakeIndex::new();

        // Insert memory records as a batch
        let mut mem_index = MemIndex::new();
        mem_index.insert(1, RecordLocation::MemoryBatch(0, 5));
        mem_index.insert(2, RecordLocation::MemoryBatch(0, 10));
        mem_index.insert(3, RecordLocation::MemoryBatch(1, 3));
        index.insert_memory_index(Arc::new(mem_index));

        let record = RawDeletionRecord {
            lookup_key: 1,
            _row_identity: None,
            pos: None,
            lsn: 1,
        };

        // Test direct field access
        let direct_locations = index
            .in_memory_index
            .as_ref()
            .unwrap()
            .get_vec(&record.lookup_key);
        assert!(direct_locations.is_some());
        assert_eq!(direct_locations.unwrap().len(), 1);

        // Test the Index trait implementation
        let trait_locations = index.find_record(&record);
        assert!(trait_locations.is_some());
        assert_eq!(trait_locations.unwrap().len(), 1);
    }
}
