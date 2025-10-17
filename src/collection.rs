use std::collections::HashMap;

use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;

use super::*;

pub struct Collection<T, K: KV>
where
    T: 'static + Serialize + for<'de> Deserialize<'de>,
{
    kv: Option<Arc<K>>,
    name: String,
    /// KV table name keyed to the index data
    indices: HashMap<String, Index<T>>,
    /// Extractor function to get a primary key from an instance of T
    primary_key: Option<fn(&T) -> Vec<u8>>,
}

impl<T, K: KV> Collection<T, K>
where
    T: 'static + Serialize + for<'de> Deserialize<'de>,
{
    /// Initialize a new collection
    pub fn new(name: &str) -> Self {
        Self {
            kv: None,
            name: name.into(),
            primary_key: None,
            indices: HashMap::default(),
        }
    }

    /// Set a primary key extractor for the collection.
    /// Failing to set a primary key extractor will cause a runtime error.
    pub fn set_primary_key(mut self, extractor: fn(&T) -> Vec<u8>) -> Self {
        self.primary_key = Some(extractor);
        self
    }

    fn kv(&self) -> &Arc<K> {
        self.kv
            .as_ref()
            .expect(&format!("Collection \"{}\" has no kv set!", self.name()))
    }

    fn primary_key(&self) -> &fn(&T) -> Vec<u8> {
        self.primary_key.as_ref().expect(&format!(
            "Collection \"{}\" has no primary key extractor is set!",
            self.name()
        ))
    }

    /// Define an index with a `name`, and a set of fields and their sort direction. This will
    /// create an index over 1 or more fields. See kv.rs for information on how indices are sorted.
    /// Indices are automatically used during operation and can be lazily initialized/removed.
    pub fn add_index(mut self, index: Index<T>) -> Result<Self> {
        let index_id = index.id();
        if self.indices.contains_key(&index_id) {
            panic!(
                "In collection \"{}\" an index named \"{index_id}\" already exists. Refusing to continue.",
                self.name
            );
        }
        if index.field_names.is_empty() {
            log::warn!(
                "In collection \"{}\", index \"{index_id}\" contains no fields",
                self.name
            );
            #[cfg(not(debug_assertions))]
            panic!("Refusing to start in production mode with an empty index");
        }
        self.indices
            .insert(format!("{}_{}", self.name(), index_id), index);
        Ok(self)
    }

    /// Return all the table names that this collection uses in the underlying KV.
    pub fn table_names(&self) -> Vec<String> {
        vec![
            vec![self.name().to_string()],
            self.indices.keys().cloned().collect::<Vec<_>>(),
        ]
        .concat()
    }

    /// The name of the collection. This is the name of the table that will be used in the
    /// underlying KV.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the number of documents in the collection.
    pub fn count(&self) -> Result<u64> {
        self.kv().count(self.name())
    }

    /// Insert a document into a collection. All relevant indices will be updated.
    pub fn insert(&self, document: &T) -> Result<()> {
        // Serialize our document
        let data = rmp_serde::to_vec_named(document)?;

        let tx = self.kv().write_tx()?;
        // Serialize our document primary key
        let primary_key = (self.primary_key())(document);
        // Check if the primary key exists, if so reject the insertion
        // TODO: use a "contains_key" type function to avoid loading the data unnecessarily
        if tx.get(self.name(), primary_key.as_slice())?.is_some() {
            anyhow::bail!(
                "Attempting to insert document will duplicate primary key in collection \"{}\": primary key: \"{:?}\"",
                self.name(),
                primary_key
            );
        }
        tx.insert(self.name(), primary_key.as_slice(), data.as_slice())?;
        for (table_name, index) in &self.indices {
            // For each index serialize a key for the index
            let key = (index.serialize)(document);
            if index.options.unique {
                // If the index is unique we need to reject duplicate keys
                if tx.get(&table_name, key.as_slice())?.is_some() {
                    anyhow::bail!(
                        "Collection \"{}\" index \"{}\" cannot insert document, uniqueness constraint violated",
                        self.name(),
                        index.name
                    );
                }
                tx.insert(&table_name, key.as_slice(), primary_key.as_slice())?;
            } else {
                // non-unique index, we're inserting into a multimap table
                tx.insert_multimap(&table_name, key.as_slice(), primary_key.as_slice())?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}
