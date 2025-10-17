use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;

use crate::*;

#[derive(Serialize, Deserialize)]
pub struct TestDocument {
    id: u128,
    id2: u128,
}

pub struct DB {
    pub test_collection: Collection<TestDocument>,
}

#[test]
fn create_collections() -> Result<()> {
    let db = DB {
        test_collection: Collection::<TestDocument>::new("test_collection")
            .set_primary_key(primary_key!(TestDocument, id))
            .add_index(index!(TestDocument, id, id2; IndexOptions { unique: true }))?,
    };
    let db = AnonDB::<RedbDB>::in_memory(None)?
        .collection(
            Collection::<TestDocument>::new("test_collection")
                .set_primary_key(primary_key!(TestDocument, id))
                .add_index(index!(TestDocument, id, id2; IndexOptions { unique: true }))?,
        )?
        .prepare()?;

    db.insert("test_collection", &TestDocument { id: 0, id2: 1 })?;
    assert_eq!(db.count("test_collection")?, 1);

    Ok(())
}

// #[test]
// fn open_and_find_one() -> Result<()> {
//     let db = Journal::in_memory(None)?;
//     db.insert("table", &"key1".to_string(), &"test_value_1".to_string())?;
//     db.insert("table", &"key2".to_string(), &"test_value_2".to_string())?;
//
//     let record = db
//         .find_one::<String, String, _>("table", |key, value| {
//             if value.ends_with("2") {
//                 Some((key, value))
//             } else {
//                 None
//             }
//         })?
//         .expect("did not find record");
//
//     assert_eq!(record.0, "key2");
//     assert_eq!(record.1, "test_value_2");
//
//     Ok(())
// }
//
// #[test]
// fn open_and_find_many() -> Result<()> {
//     let db = Journal::in_memory(None)?;
//     db.insert("table", &"key1".to_string(), &"test_value_1".to_string())?;
//     db.insert("table", &"key2".to_string(), &"test_value_2".to_string())?;
//
//     let records = db.find_many::<String, String, _>("table", |_key, _value| true)?;
//
//     assert_eq!(records.len(), 2);
//
//     Ok(())
// }
//
// #[test]
// fn open_and_insert() -> Result<()> {
//     let db = Journal::in_memory(None)?;
//     db.insert("test2", &"test_key".to_string(), &"test_value".to_string())?;
//     assert_eq!(db.get_state()?.next_tx_index, 1);
//     db.insert("test3", &"test_key".to_string(), &"test_value".to_string())?;
//     assert_eq!(db.get_state()?.next_tx_index, 2);
//     Ok(())
// }
//
// #[test]
// fn open_and_tx() -> Result<()> {
//     let db = Journal::in_memory(None)?;
//     // first transaction
//     db.insert("test2", &"test_key".to_string(), &"test_value".to_string())?;
//     // second transaction
//     let mut tx = db.begin_write()?;
//     {
//         let mut table = tx.open_table("test")?;
//         table.insert(&"ahfskasjf".to_string(), &"AShaskfasfaf".to_string())?;
//     }
//     tx.commit()?;
//
//     let transactions = db.drain_transactions()?;
//     assert_eq!(transactions.len(), 2);
//     assert_eq!(transactions.get(0).unwrap().operations.len(), 3);
//     assert_eq!(transactions.get(1).unwrap().operations.len(), 3);
//     Ok(())
// }
//
// #[test]
// fn open_and_get() -> Result<()> {
//     let db = Journal::in_memory(None)?;
//     let mut tx = db.begin_write()?;
//     let test_val = "tsdfadasdgasgag".to_string();
//     {
//         let mut table = tx.open_table("test")?;
//         table.insert(&"test_key".to_string(), &test_val)?;
//     }
//     tx.commit()?;
//
//     assert_eq!(
//         db.get::<String, String>("test", &"test_key".to_string())?
//             .expect("get operation errored in open_and_get"),
//         test_val
//     );
//
//     Ok(())
// }
//
// #[test]
// fn open_and_remove() -> Result<()> {
//     let db = Journal::in_memory(None)?;
//     let mut tx = db.begin_write()?;
//     {
//         let mut table = tx.open_table("test")?;
//         table.remove(&"ahfskasjf".to_string())?;
//     }
//     tx.commit()?;
//
//     let transactions = db.drain_transactions()?;
//     assert_eq!(transactions.len(), 1);
//     assert_eq!(transactions.first().unwrap().operations.len(), 3);
//     Ok(())
// }
