use async_trait::async_trait;
/// A trait which defines deletion vector write related interfaces.
use iceberg::puffin::PuffinWriter;
use iceberg::spec::{Schema as IcebergSchema, TableMetadata};
use iceberg::table::Table;
use iceberg::{Catalog, Result as IcebergResult, TableIdent};

use std::collections::HashSet;

use crate::storage::iceberg::puffin_writer_proxy::PuffinBlobMetadataProxy;

pub enum PuffinBlobType {
    DeletionVector,
    FileIndex,
}

/// TODO(hjiang): iceberg-rust currently doesn't support puffin write, to workaround and reduce code change,
/// we record puffin metadata ourselves and rewrite manifest file before transaction commits.
#[async_trait]
pub trait PuffinWrite {
    /// Record persisted puffin metadata.
    fn record_puffin_metadata(
        &mut self,
        puffin_filepath: String,
        puffin_metadata: Vec<PuffinBlobMetadataProxy>,
        puffin_blob_type: PuffinBlobType,
    );
    /// Add puffin metadata from the writer, and close it.
    async fn record_puffin_metadata_and_close(
        &mut self,
        puffin_filepath: String,
        puffin_writer: PuffinWriter,
        puffin_blob_type: PuffinBlobType,
    ) -> IcebergResult<()>;

    /// Set data files to remove, their corresponding deletion vectors will be removed alongside.
    fn set_data_files_to_remove(&mut self, data_files: HashSet<String>);

    /// Set puffin file to remove.
    fn set_index_puffin_files_to_remove(&mut self, puffin_filepaths: HashSet<String>);

    /// After transaction commits, puffin metadata should be cleared for next puffin write.
    fn clear_puffin_metadata(&mut self);
}

/// TODO(hjiang): iceberg-rust currently doesn't support schema evolution, to workaround and reduce code change,
/// we do schema evolution by directly setting table commits.
#[async_trait]
pub trait SchemaUpdate {
    /// Update table schema, and return the updated iceberg table.
    async fn update_table_schema(
        &mut self,
        new_schema: IcebergSchema,
        table_ident: TableIdent,
    ) -> IcebergResult<Table>;
}

#[async_trait]
pub trait CatalogAccess {
    /// Get warehouse location.
    #[allow(unused)]
    fn get_warehouse_location(&self) -> &str;

    /// Load metadata and its location foe the given table.
    async fn load_metadata(
        &self,
        table_ident: &TableIdent,
    ) -> IcebergResult<(String /*metadata_filepath*/, TableMetadata)>;
}

pub trait MoonlinkCatalog: PuffinWrite + SchemaUpdate + CatalogAccess + Catalog {}
impl<T: PuffinWrite + SchemaUpdate + CatalogAccess + Catalog> MoonlinkCatalog for T {}
