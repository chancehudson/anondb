use std::collections::HashMap;

use anyhow::Context;
use anyhow::Result;
use byteorder::BigEndian;
use byteorder::LittleEndian;
use byteorder::WriteBytesExt;
use rmpv::ValueRef;

#[derive(Debug, Clone, Default, PartialEq)]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

impl ToString for SortDirection {
    fn to_string(&self) -> String {
        match self {
            Self::Asc => "asc".into(),
            Self::Desc => "desc".into(),
        }
    }
}

pub fn parse_bytes(mut bytes: &[u8]) -> Result<HashMap<&str, ValueRef>> {
    match rmpv::decode::read_value_ref(&mut bytes)? {
        rmpv::ValueRef::Map(decoded) => {
            let mut out = HashMap::new();
            for (key, val) in decoded {
                match key {
                    rmpv::ValueRef::String(key) => {
                        out.insert(
                            key.into_str()
                                .ok_or(anyhow::anyhow!("Document has a non-utf8 key!",))?,
                            val,
                        );
                    }
                    _ => {
                        anyhow::bail!(
                            "Document has a non-string key! key: {:?} value: {:?}",
                            key,
                            val
                        );
                    }
                }
            }
            Ok(out)
        }
        _ => unreachable!("decoded data should be a map"),
    }
}

/// A generic key-value store. Assumed to be capable of transactional mutation of key-value collections.
pub trait KV: Sized + Operations {
    /// Initialize a kv persisted to a path. What path is (directory, file, etc) is determined by
    /// the underlying implementation.
    fn at_path(path: &std::path::Path) -> Result<Self>;
    /// Initialize a kv with a byte representation of the initial state. This byte
    /// representation is arbitrary to the concrete implementation.
    fn in_memory(bytes_maybe: Option<&[u8]>) -> Result<Self>;
    fn write_tx(&self) -> Result<impl Transaction>;
    fn read_tx(&self) -> Result<impl Transaction>;

    /// Iterate over the contents of a collection, in ascending lexicographic order. Must be
    /// `O(N)`.
    fn scan<S>(&self, table: &str, predicate: S) -> Result<()>
    where
        S: Fn(&[u8], &[u8]) -> Result<bool>;
}

pub trait Operations {
    /// Insert a key for a multimap table. Must be `O(1)`.
    fn insert_multimap(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()>;
    /// Remove a key and value from a multimap table. Returns `true` if the key/value pair was
    /// present in the table. Must be `O(1)`.
    fn remove_multimap(&self, table: &str, key: &[u8], value: &[u8]) -> Result<bool>;
    /// Remove all values associated with a key in a multimap table. Must be `O(1)`.
    fn remove_all_multimap(&self, table: &str, key: &[u8]) -> Result<()>;
    /// Get an iterator over all the values associated with a key in a multimap table.
    fn get_multimap(&self, table: &str, key: &[u8]) -> Result<impl Iterator<Item = &[u8]>>;

    /// Insert a key for a table and return the old value if it exists. Must be `O(1)`.
    fn insert(&self, table: &str, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Remove a key from a table. Must be `O(1)`.
    fn remove(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Retrieve the value associated to a key for a table. Must be `O(1)`.
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Determine the number of keys present in a table.
    fn count(&self, table: &str) -> Result<u64>;
    /// Remove all entries in the table.
    fn clear(&self, table: &str) -> Result<()>;
}

pub trait Transaction: Operations {
    fn commit(self) -> Result<()>;
}

/// Types of fields that can be used for indexing
pub enum FieldType {
    Nil = 0x00,
    Bool = 0x01,
    Int = 0x02,    // 64 bit integer
    Float = 0x04,  // 64 bit float
    String = 0x05, // UTF8 string
    Bytes = 0x06,  // raw bytes, max length 2**32
}

/// Encode a value in a lexicographically comparable way. Leading byte indicates type of data.
pub fn encode_index_key(value: &ValueRef, sort_dir: &SortDirection) -> Result<Vec<u8>> {
    let mut buf = Vec::new();

    match value {
        ValueRef::Nil => {
            buf.write_u8(0x00)?;
        }
        ValueRef::Boolean(b) => {
            buf.write_u8(0x01)?;
            buf.write_u8(if *b { 1 } else { 0 })?;
        }
        ValueRef::Integer(i) => {
            buf.write_u8(0x02)?;
            let val = i.as_i64().ok_or(anyhow::anyhow!(
                "Failed to serialize integer, is it out of bounds (i64)?"
            ))?;
            // XOR with sign bit to make negatives sort before positives
            let sortable = (val as u64) ^ 0x8000_0000_0000_0000;
            buf.write_u64::<BigEndian>(sortable)?;
        }
        ValueRef::F64(_) => {
            anyhow::bail!("Floating point numbers are not allowed in indices");
        }
        ValueRef::String(s) => {
            buf.write_u8(0x04)?;
            buf.extend_from_slice(s.as_bytes());
            buf.write_u8(0x00)?; // Null terminator (instead of length prefix)
        }
        ValueRef::Binary(b) => {
            buf.write_u8(0x05)?;
            buf.write_u32::<BigEndian>(
                u32::try_from(b.len())
                    .context("Cannot store more than 2**32 bytes in a single entry.")?,
            )?;
            buf.extend_from_slice(b);
        }
        _ => anyhow::bail!("Unsupported value type in encode_index_key"),
    }

    if matches!(sort_dir, SortDirection::Desc) {
        for byte in buf.iter_mut().skip(1) {
            *byte = !*byte;
        }
    }

    Ok(buf)
}
