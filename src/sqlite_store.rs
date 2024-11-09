use super::Error;
use super::traits::KeyValueStore;

use std::sync::Arc;
use std::path::{PathBuf, Path};

use rusqlite::Connection;
use tokio::sync::Mutex;

use serde::{Serialize, Deserialize};

const PARTITION_KEY: &[u8; 14] = b"__PARTITIONS__";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Partitions {
    paths: Vec<PathBuf>
}

#[derive(Clone)]
pub struct SqliteStore {
    location: PathBuf,
    store: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    async fn get_partitions(&self) -> Result<Vec<PathBuf>, Error> {
        Ok(self.get(PARTITION_KEY).await?.as_ref().map(|b|
            serde_json::from_slice::<Vec<PathBuf>>(b)
        ).transpose()?.unwrap_or_default())
    }
}

#[async_trait::async_trait]
impl KeyValueStore for SqliteStore {
    async fn new(location: PathBuf) -> Result<Self, Error> {
        std::fs::create_dir_all(location.clone())?;
        let db = Connection::open(location.join("kvs.db"))?;
        db.execute("CREATE TABLE if not exists kvs(key TEXT NOT NULL UNIQUE, value TEXT);", [])?;
        Ok(SqliteStore{location, store: Arc::new(Mutex::new(db))})
    }

    async fn partition(&self, location: PathBuf) -> Result<Box<dyn KeyValueStore>, Error> {
        let mut partitions = self.get_partitions().await?;
        if !partitions.contains(&location) {
            partitions.push(location.clone());
            self.set(PARTITION_KEY, &serde_json::to_vec(&partitions)?).await?;
        }
        Ok(Box::new(Self::new(self.location.join(location)).await?))
    }

    async fn get_partition(&self, location: &Path) -> Option<Box<dyn KeyValueStore>> {
        let partitions = self.get_partitions().await.unwrap();
        if partitions.contains(&location.to_path_buf()) {
            Some(Box::new(Self::new(self.location.join(location)).await.unwrap()))
        } else {None}
    }

    async fn clear(&self) -> Result<(), Error> {
        let partitions = self.get_partitions().await?;
        for path in &partitions {
            self.get_partition(path).await.unwrap().clear().await?;
        }
        self.set(PARTITION_KEY, &serde_json::to_vec::<Vec<PathBuf>>(&vec![])?).await?;
        let keys: Vec<Vec<u8>> = self.keys().await?;
        for key in keys {
            self.delete(&key).await?;
        }
        Ok(())
    }
    async fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.store.lock().await.execute("DELETE FROM kvs WHERE key = ?;", [hex::encode(key)])?;
        Ok(())
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let store = self.store.lock().await;
        let mut stmt = store.prepare(&format!("SELECT value FROM kvs where key = \'{}\'", hex::encode(key)))?;
        let result = stmt.query_and_then([], |row| {
            let item: String = row.get(0)?;
            Ok(hex::decode(item)?)
        })?.collect::<Result<Vec<Vec<u8>>, Error>>()?;
        Ok(result.first().cloned())
    }

    async fn set(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.store.lock().await.execute("
            INSERT INTO kvs(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value;
        ", [hex::encode(key), hex::encode(value)])?;
        Ok(())
    }

    async fn get_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let store = self.store.lock().await;
        let mut stmt = store.prepare("SELECT key, value FROM kvs")?;
        let result = stmt.query_and_then([], |row| {
            let key: String = row.get(0)?;
            let value: String = row.get(1)?;
            Ok((hex::decode(key)?, hex::decode(value)?))
        })?.collect::<Result<Vec<(Vec<u8>, Vec<u8>)>, Error>>()?
        .into_iter().filter(|(k, _)| k != PARTITION_KEY).collect();
        Ok(result)
    }

    async fn keys(&self) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self.get_all().await?.into_iter().map(|(k, _)| k).collect())
    }

    async fn values(&self) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self.get_all().await?.into_iter().map(|(_, v)| v).collect())
    }

    fn location(&self) -> PathBuf { self.location.clone() }
}

impl std::fmt::Debug for SqliteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut fmt = f.debug_struct("SqliteStore");
        fmt
        .field("location", &self.location)
        .finish()
    }
}
