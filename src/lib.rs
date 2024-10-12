mod error;
pub use error::Error;

mod traits;
pub use traits::{
    KeyValueStore,
    Indexable,
    KeyValue,
};

#[cfg(feature = "leveldb")]
mod level_store;
#[cfg(feature = "leveldb")]
pub use level_store::LevelStore;

#[cfg(feature = "rusqlite")]
mod sqlite_store;
#[cfg(feature = "rusqlite")]
pub use sqlite_store::SqliteStore;


mod memory_store;
pub use memory_store::MemoryStore;

pub mod database;
pub use database::Database;
