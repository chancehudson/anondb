# Architecture

AnonDB is a set of macros and structs to form a fully featured database implementation. This implementation supports indexing and complex queries in an automated way.

Each database is implemented on top of a generic key-value store (the `KV` trait). The underlying key-value store is responsible for providing persistence to disk/memory, lexicographic key sorting, and atomic transactions. The key-value store provides access to tables identified by a `String`.

The key-value store is _not_ responsible for indices, collections, or database metadata. These concepts exist at the database level.

The key-value implementation and database implementation can be optimized/customized separately for different use cases.
