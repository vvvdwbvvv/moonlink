use crate::storage::index::index_util::{Index, RawDeletionRecord, RecordLocation};
use std::collections::HashMap;
/// Type for primary keys
pub type PrimaryKey = i64;

/// Index containing records in memory
pub type MemIndex = HashMap<PrimaryKey, RecordLocation>; // key -> (batch_id, row_offset)
/// Index containing records in files
pub type FileIndex = HashMap<PrimaryKey, RecordLocation>; // key -> (batch_id, row_offset)

impl Index for MemIndex {
    fn find_record(&self, raw_record: &RawDeletionRecord) -> Option<RecordLocation> {
        self.get(&raw_record.lookup_key).cloned()
    }

    fn insert(&mut self, key: PrimaryKey, location: RecordLocation) {
        self.insert(key, location);
    }
}
/// A simple in-memory index that maps primary keys to record locations
pub struct InMemoryIndex {
    /// Map for records in memory batches
    memory_index: MemIndex,

    /// Map for records in files that have been flushed to disk
    /// Organized by file batch for efficient updates and lookups
    file_indices: Vec<FileIndex>,
}

impl InMemoryIndex {
    /// Create a new, empty in-memory index
    pub fn new() -> Self {
        Self {
            memory_index: HashMap::new(),
            file_indices: Vec::new(),
        }
    }

    /// Insert a memory index (batch of in-memory records)
    ///
    /// This replaces the current memory index
    pub fn insert_memory_index(&mut self, mem_index: MemIndex) {
        assert!(self.memory_index.is_empty());
        self.memory_index = mem_index;
    }

    pub fn delete_memory_index(&mut self) {
        self.memory_index.clear();
    }

    /// Insert a file index (batch of on-disk records)
    ///
    /// This adds a new file index to the collection of file indices
    pub fn insert_file_index(&mut self, file_index: FileIndex) {
        self.file_indices.push(file_index);
    }

    /// Look up a record location by primary key
    /// Returns the location if found, None if the record doesn't exist
    pub fn lookup(&self, key: PrimaryKey) -> Option<RecordLocation> {
        if let Some(location) = self.memory_index.get(&key) {
            return Some(location.clone());
        }
        // Finally check all file indices, from newest to oldest
        for index in self.file_indices.iter().rev() {
            if let Some(location) = index.get(&key) {
                return Some(location.clone());
            }
        }

        // Record not found
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    #[test]
    fn test_in_memory_index_basic() {
        let mut index = InMemoryIndex::new();

        // Insert memory records as a batch
        let mut mem_index = MemIndex::new();
        mem_index.insert(1, RecordLocation::MemoryBatch(0, 5));
        mem_index.insert(2, RecordLocation::MemoryBatch(0, 10));
        mem_index.insert(3, RecordLocation::MemoryBatch(1, 3));
        index.insert_memory_index(mem_index);

        // Look up records
        assert_eq!(index.lookup(1), Some(RecordLocation::MemoryBatch(0, 5)));
        assert_eq!(index.lookup(2), Some(RecordLocation::MemoryBatch(0, 10)));
        assert_eq!(index.lookup(3), Some(RecordLocation::MemoryBatch(1, 3)));
        assert_eq!(index.lookup(4), None);

        // Insert file records as a batch
        let mut file_index = FileIndex::new();
        file_index.insert(
            4,
            RecordLocation::DiskFile(PathBuf::from("file1.parquet"), 0),
        );
        file_index.insert(
            5,
            RecordLocation::DiskFile(PathBuf::from("file1.parquet"), 1),
        );
        index.insert_file_index(file_index);

        // Look up file records
        assert_eq!(
            index.lookup(4),
            Some(RecordLocation::DiskFile(PathBuf::from("file1.parquet"), 0))
        );
        assert_eq!(
            index.lookup(5),
            Some(RecordLocation::DiskFile(PathBuf::from("file1.parquet"), 1))
        );
    }
}
