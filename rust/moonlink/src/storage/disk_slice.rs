use crate::error::{Error, Result};
use crate::storage::data_batches::BatchEntry;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

pub struct DiskSliceWriter {
    /// The schema of the DiskSlice.
    ///
    schema: Arc<Schema>,

    dir_path: PathBuf,

    batches: Vec<BatchEntry>,

    writer_lsn: u64,
    /// The list of parquet files. These are immutable once the slice is created.
    ///
    files: Vec<PathBuf>,
}

impl DiskSliceWriter {
    #[cfg(debug_assertions)]
    const PARQUET_FILE_SIZE: usize = 1024 * 1024 * 2; // 2MB

    #[cfg(not(debug_assertions))]
    const PARQUET_FILE_SIZE: usize = 1024 * 1024 * 128; // 128MB

    pub fn new(
        schema: Arc<Schema>,
        dir_path: PathBuf,
        batches: Vec<BatchEntry>,
        writer_lsn: u64,
    ) -> Self {
        Self {
            schema,
            dir_path,
            batches,
            files: vec![],
            writer_lsn,
        }
    }

    pub fn write(&mut self) -> Result<()> {
        let mut filtered_batches = Vec::new();
        for entry in self.batches.iter() {
            let filtered_batch = entry.batch.get_filtered_batch()?;
            if let Some(batch) = filtered_batch {
                filtered_batches.push(batch);
            }
        }
        self.files = self.write_batch_to_parquet(&filtered_batches, &self.dir_path)?;
        Ok(())
    }

    pub fn input_batches(&self) -> &Vec<BatchEntry> {
        &self.batches
    }
    /// Get the list of files in the DiskSlice
    pub fn output_files(&self) -> &[PathBuf] {
        self.files.as_slice()
    }
    /// Write record batches to parquet files
    fn write_batch_to_parquet(
        &self,
        record_batches: &Vec<RecordBatch>,
        dir_path: &PathBuf,
    ) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        let mut writer = None;
        for batch in record_batches {
            if writer.is_none() {
                // Generate a unique file name
                let file_name: String = format!("{}.parquet", Uuid::new_v4());
                let file_path = dir_path.join(file_name);

                // Create the file
                let file = File::create(&file_path).map_err(|e| Error::Io(e))?;
                files.push(file_path);
                writer = Some(ArrowWriter::try_new(file, self.schema.clone(), None)?);
            }
            // Write the batch
            writer.as_mut().unwrap().write(batch)?;

            if writer.as_ref().unwrap().memory_size() > Self::PARQUET_FILE_SIZE {
                // Finalize the writer
                writer.unwrap().close()?;
                writer = None;
            }
        }
        if let Some(writer) = writer {
            writer.close()?;
        }
        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mem_slice::MemSlice;
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use pg_replicate::conversions::{table_row::TableRow, Cell};
    use tempfile::tempdir;
    #[test]
    fn test_disk_slice_builder() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir().map_err(|e| Error::Io(e))?;
        // Create a schema for testing
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create a MemSlice with test data
        let mut mem_slice = MemSlice::new(schema.clone(), 100);

        // Add some test rows
        let row1 = TableRow {
            values: vec![Cell::I32(1), Cell::String("Alice".to_string())],
        };
        let row2 = TableRow {
            values: vec![Cell::I32(2), Cell::String("Bob".to_string())],
        };

        mem_slice.append(1, &row1)?;
        mem_slice.append(2, &row2)?;
        let (_new_batch, entries, _index) = mem_slice.flush().unwrap();

        let mut disk_slice =
            DiskSliceWriter::new(schema, temp_dir.path().to_path_buf(), entries, 1);
        disk_slice.write()?;
        // Verify files were created
        assert!(!disk_slice.output_files().is_empty());
        println!("Files: {:?}", disk_slice.output_files());

        // Read the files and verify the data
        for file in disk_slice.output_files() {
            let file = File::open(file).map_err(|e| Error::Io(e))?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            println!("Converted arrow schema is: {}", builder.schema());

            let mut reader = builder.build().unwrap();
            let record_batch = reader.next().unwrap().unwrap();
            println!("{:?}", record_batch);
        }
        // Clean up temporary directory
        temp_dir.close().map_err(|e| Error::Io(e))?;

        Ok(())
    }
}
