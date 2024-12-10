use super::Error;

use super::database::Index;

use std::path::{PathBuf, Path};

use dyn_clone::{clone_trait_object, DynClone};

pub type KeyValue = Vec<(Vec<u8>, Vec<u8>)>;

#[async_trait::async_trait]
pub trait KeyValueStore: std::fmt::Debug + Send + Sync + DynClone {
    async fn new(location: PathBuf) -> Result<Self, Error> where Self: Sized;
    async fn partition(&self, location: PathBuf) -> Result<Box<dyn KeyValueStore>, Error>;
    async fn get_partition(&self, location: &Path) -> Option<Box<dyn KeyValueStore>>;
    async fn clear(&self) -> Result<(), Error>;
    async fn delete(&self, key: &[u8]) -> Result<(), Error>;
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<(), Error>;

    async fn get_all(&self) -> Result<KeyValue, Error>;
    async fn keys(&self) -> Result<Vec<Vec<u8>>, Error>;
    async fn values(&self) -> Result<Vec<Vec<u8>>, Error>;

    fn location(&self) -> PathBuf;
}
clone_trait_object!(KeyValueStore);

pub trait Indexable {
    const PRIMARY_KEY: &'static str = "primary_key";
    const DEFAULT_SORT: &'static str = Self::PRIMARY_KEY;
    fn primary_key(&self) -> Vec<u8>;
    fn secondary_keys(&self) -> Index {Index::default()}
    fn index(&self) -> Index {
        let mut index = self.secondary_keys();
        index.insert(Self::PRIMARY_KEY.to_string(), hex::encode(self.primary_key()).into());
        index
    }
}
