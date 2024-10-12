#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[cfg(feature = "leveldb")]
    #[error(transparent)]
    LevelDB(#[from] leveldb::error::Error),

    #[error("Generic Error {0}: {1}")]
    Generic(String, String),
}

impl Error {
    pub fn err(ctx: &str, err: &str) -> Self {Error::Generic(ctx.to_string(), err.to_string())}
}

