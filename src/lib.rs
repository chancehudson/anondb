mod bytes;
mod collection;
mod index;
mod kv;
mod kv_redb;
mod query;

use anyhow::Context;
pub use bytes::Bytes;
pub use collection::*;
pub use index::*;
pub(crate) use kv::*;
pub use kv_redb::*;

#[cfg(test)]
mod test;

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;

pub trait AAnonDB: Sized {
    fn at_path(path: &Path) -> Result<Self>;
    fn in_memory(bytes_maybe: Option<&[u8]>) -> Result<Self>;
    fn prepare(self) -> Result<Arc<Self>>;
}

/// A generic document oriented database. Supports accelerated queries over arbitrarily encoded
/// documents.
pub struct AnonDB<D: KV> {
    /// The key value store backing this AnonDB instance.
    kv: D,
}

impl<D: KV> AnonDB<D> {
    pub fn at_path(path: &Path) -> Result<Self> {
        Ok(Self {
            kv: D::at_path(path)?,
        })
    }

    pub fn in_memory(bytes_maybe: Option<&[u8]>) -> Result<Self> {
        Ok(Self {
            kv: D::in_memory(bytes_maybe)?,
        })
    }

    /// Determine all the indices that should exist and create them if necessary.
    pub fn prepare(self) -> Result<Arc<Self>> {
        // check that all our collections and indices have distinct table names in the KV
        let mut all_table_names = Vec::default();

        for collection in self.collections.values() {
            // make sure that each collection has a primary_key defined
            if collection.primary_key().is_none() {
                anyhow::bail!(
                    "Collection \"{}\" does not have a primary key set, refusing to start.",
                    collection.name()
                );
            }
            all_table_names.push(collection.name().into());
            for (name, index) in collection.indices() {
                let id = index.id();
                all_table_names.push(id);
                if self.collections.contains_key(&id) {
                    anyhow::bail!("Collection name collides with index name: \"{}\"", id);
                }
            }
        }
        all_table_names
            .iter()
            .fold(Ok(HashSet::<String>::default()), |acc, ele| {
                let acc = acc?;
                if acc.contains(ele) {
                    anyhow::bail!(
                        "Invalid database configuration, the table \"{ele}\" is referenced twice."
                    );
                }
                acc.insert(ele);
                Ok(acc)
            });
        self.rebuild_indices()?;
        Ok(Arc::new(self))
    }

    /// Fully rebuild all indices. This operation is `O(N)` over the number of documents in the
    /// database.
    pub fn rebuild_indices(&self) -> Result<()> {
        // first empty all index collections
        let tx = self.kv.write_tx()?;
        for collection in self.collections.values() {
            for (name, index) in collection.indices() {
                let index_collection_name = collection.index_id(name, index);
                tx.clear(&index_collection_name)?;
            }
        }
        tx.commit()?;

        // then iterate over all documents and construct indices
        for collection in self.collections.values() {
            self.kv.scan(collection.name(), |key, val| {
                let data = parse_bytes(val)?;
                for (name, index) in collection.indices() {
                    let mut index_entry = Vec::default();
                    for (field_name, sort_dir) in index {
                        if let Some(field) = data.get(field_name.as_str()) {
                            index_entry.extend(encode_index_key(field, sort_dir)?);
                        }
                    }
                    let index_collection_name = collection.index_id(name, index);
                    // we insert the document primary key as the value and the indexed bytes as the
                    // key to get lexicographic iteration
                    self.kv.insert(&index_collection_name, &index_entry, key)?;
                }
                return Ok(true);
            })?;
        }
        Ok(())
    }
}
