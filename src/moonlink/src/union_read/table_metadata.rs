use bincode::enc::{write::Writer, Encode, Encoder};
use bincode::error::EncodeError;

#[derive(Debug)]
pub(super) struct TableMetadata {
    pub(super) data_files: Vec<String>,
    pub(super) position_deletes: Vec<(u32, u32)>,
}

impl Encode for TableMetadata {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let writer = encoder.writer();
        write_usize(writer, self.data_files.len())?;
        let mut offset = 0;
        for data_file in &self.data_files {
            write_usize(writer, offset)?;
            offset = offset.saturating_add(data_file.len());
        }
        write_usize(writer, offset)?;
        write_usize(writer, self.position_deletes.len())?;
        for position_delete in &self.position_deletes {
            write_u32(writer, position_delete.0)?;
            write_u32(writer, position_delete.1)?;
        }
        for data_file in &self.data_files {
            writer.write(data_file.as_bytes())?;
        }
        Ok(())
    }
}

fn write_u32<W: Writer>(writer: &mut W, value: u32) -> Result<(), EncodeError> {
    writer.write(&value.to_ne_bytes())
}

fn write_usize<W: Writer>(writer: &mut W, value: usize) -> Result<(), EncodeError> {
    let value = u32::try_from(value).map_err(|_| EncodeError::Other("out of range"))?;
    write_u32(writer, value)
}
