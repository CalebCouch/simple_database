use super::Error;
use super::traits::KeyValueStore;

use std::collections::HashMap;
use std::path::{PathBuf, Path};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::sync::{LazyLock};

use serde::{Serialize, Deserialize};

type Store = Arc<Mutex<(Vec<PathBuf>, HashMap<Vec<u8>, Vec<u8>>)>>;

static MEMORY: LazyLock<Mutex<HashMap<PathBuf, Store>>> = LazyLock::new(|| Mutex::new(HashMap::new()));


#[derive(Serialize, Deserialize, Debug)]
pub struct Partitions {
    names: Vec<String>
}

#[derive(Clone)]
pub struct MemoryStore {
    location: PathBuf,
    store: Store
}

#[async_trait::async_trait]
impl KeyValueStore for MemoryStore {
    async fn new(location: PathBuf) -> Result<Self, Error> {
        let mut memory = MEMORY.lock().await;
        let store = if let Some(store) = memory.get(&location) {
            store.clone()
        } else {
            let store = Arc::new(Mutex::new((vec![], HashMap::new())));
            memory.insert(location.clone(), store.clone());
            store
        };
        Ok(MemoryStore{location, store})
    }

    async fn partition(&self, location: PathBuf) -> Result<Box<dyn KeyValueStore>, Error> {
        if let Some(ns) = self.get_partition(&location).await {Ok(ns)} else {
            self.store.lock().await.0.push(location.clone());
            Ok(Box::new(Self::new(self.location.join(location)).await?))
        }
    }

    async fn get_partition(&self, location: &Path) -> Option<Box<dyn KeyValueStore>> {
        if self.store.lock().await.0.contains(&location.to_path_buf()) {
            Some(Box::new(Self::new(self.location.join(location)).await.unwrap()))
        } else {None}
    }

    async fn clear(&self) -> Result<(), Error> {
        let mut store = self.store.lock().await;
        for loc in &store.0 {
            Box::pin(self.get_partition(loc)).await.unwrap().clear().await?;
        }
        store.0.clear();
        store.1.clear();
        Ok(())
    }
    async fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.store.lock().await.1.remove(key);
        Ok(())
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.store.lock().await.1.get(key).cloned())
    }
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.store.lock().await.1.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn get_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        Ok(self.store.lock().await.1.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }
    async fn keys(&self) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self.store.lock().await.1.keys().cloned().collect())
    }
    async fn values(&self) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self.store.lock().await.1.values().cloned().collect())
    }

    fn location(&self) -> PathBuf { self.location.clone() }
}

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut fmt = f.debug_struct("MemoryStore");
        fmt
        .field("location", &self.location)
        .finish()
    }
}
