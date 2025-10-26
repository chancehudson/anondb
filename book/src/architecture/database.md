# Database

AnonDB is designed to easily produce an embedded document store with support for accelerated queries:

```rs
#[derive(AnonDB)]
pub struct DB<K: KV> {
    #[anondb(primary_key = id)]
    pub users: Collection<User, K>,
    #[anondb(primary_key = id)]
    pub posts: Collection<Post, K>,
}
```


