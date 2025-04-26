// A read state is a collection of objects that are shared between moonlink and readers
//
// Meant to be sent using either shared memory or network connection.
//

use super::table_metadata::TableMetadata;
use bincode::config;

const BINCODE_CONFIG: config::Configuration = config::standard();

#[derive(Debug)]
pub struct ReadState {
    pub data: Vec<u8>,
}

impl Drop for ReadState {
    fn drop(&mut self) {
        println!("Dropping read state");
    }
}

impl ReadState {
    pub(super) fn new(input: (Vec<String>, Vec<(u32, u32)>)) -> Self {
        let metadata = TableMetadata {
            data_files: input.0,
            position_deletes: input.1,
        };
        let data = bincode::encode_to_vec(metadata, BINCODE_CONFIG).unwrap(); // TODO
        Self { data }
    }
}
