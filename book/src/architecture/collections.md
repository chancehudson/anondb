# Collections

A collection is a set of documents and indices. Indices are automatically created/deleted/synchronized during operation.

The `#[anondb(index = field0, field1)]` macro generates a list of fields that exist in the index, as well as a key extractor function for a document. This function is statically safe and looks roughly like this:

```
fn index_key_extractor(document: &T) -> Vec<u8> {
    vec![
        document.field0.serialize_lex(),
        document.field1.serialize_lex()
    ].concat()
}
```

This ensures that field indices exist on the document and implement the required serialization traits. It also avoids the overhead of parsing a document into a hashmap of key/value pairs and accessing using string comparisons.

## Primary index

The primary index operates the same as a regular index, with two exceptions:
1. The kv table name of a primary index is the name of the collection
2. Each entry in the primary index stores the full document data

The primary index must additionally be `unique`.

## Indices

Non-primary indices are simply called an "index". An index is formed over some fields of a document. Each field in an index must implement `SerializeLexicographic`.

