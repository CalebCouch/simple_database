#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[cfg(feature = "rusqlite")]
    #[error(transparent)]
    HexError(#[from] hex::FromHexError),

    #[cfg(feature = "rusqlite")]
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),

    #[cfg(feature = "rusqlite")]
    #[error(transparent)]
    IoError(#[from] std::io::Error),


    #[error("Generic Error {0}: {1}")]
    Generic(String, String),
}

impl Error {
    pub fn err(ctx: &str, err: &str) -> Self {Error::Generic(ctx.to_string(), err.to_string())}
}

