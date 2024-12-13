use super::Error;

use super::traits::{KeyValueStore, Field};

use std::path::PathBuf;

use serde::Deserialize;

#[derive(Clone)]
pub struct State {
    store: Box<dyn KeyValueStore>,
}

impl State {
    pub async fn new<KVS: KeyValueStore + 'static>(
        path: PathBuf,
    ) -> Result<Self, Error> {
        Ok(State{
            store: Box::new(KVS::new(path).await?)
        })
    }

    pub async fn set<F: Field>(&self, field: F) -> Result<(), Error> {
        self.store.set(&field.as_bytes(), &serde_json::to_vec(&field)?).await?;
        Ok(())
    }

    pub async fn get_raw<F: Field>(&self, field: &F) -> Result<Option<Vec<u8>>, Error> {
        self.store.get(&field.as_bytes()).await
    }

    pub async fn get<F: Field, T: for <'a> Deserialize<'a>>(&self, field: &F) -> Result<Option<T>, Error> {
        Ok(self.get_raw(field).await?.map(|b|
            serde_json::from_slice::<Option<T>>(&b)
        ).transpose()?.flatten())
    }

    pub async fn get_or_default<F: Field, T: for <'a> Deserialize<'a> + Default>(&self, field: &F) -> Result<T, Error> {
        Ok(self.get(field).await?.unwrap_or_default())
    }

    pub async fn get_or_err<F: Field, T: for <'a> Deserialize<'a>>(&self, field: &F) -> Result<T, Error> {
        self.get(field).await?.ok_or(Error::err("State.get_or_err", &format!("Value not found for field: {:?}", field)))
    }
}
