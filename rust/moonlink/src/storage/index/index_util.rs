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
