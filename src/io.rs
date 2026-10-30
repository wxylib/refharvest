//! Opening sources: a URL or path becomes an [`AsyncFileReader`] that
//! counts what it fetches and retries a request that fails part way.

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_hdf5::reader::ObjectReader;
use async_hdf5::{AsyncFileReader, HDF5Error};
use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;

use crate::{Error, Result};

/// The store and path for `source`: `http://` or `https://` URLs,
/// `file://` URLs or plain paths, and `s3://bucket/key` with the `s3`
/// feature (credentials and endpoint from the usual `AWS_*` variables).
pub fn store_for(source: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
    if source.starts_with("http://") || source.starts_with("https://") {
        let u = url::Url::parse(source).map_err(|e| Error::Io(format!("{source}: {e}")))?;
        let host = u
            .host_str()
            .ok_or_else(|| Error::Io(format!("{source}: no host")))?;
        let base = match u.port() {
            Some(p) => format!("{}://{host}:{p}", u.scheme()),
            None => format!("{}://{host}", u.scheme()),
        };
        let store = object_store::http::HttpBuilder::new()
            .with_url(base)
            .with_client_options(object_store::ClientOptions::new().with_allow_http(true))
            .build()
            .map_err(|e| Error::Io(format!("{source}: {e}")))?;
        let path =
            Path::from_url_path(u.path()).map_err(|e| Error::Io(format!("{source}: {e}")))?;
        return Ok((Arc::new(store), path));
    }
    #[cfg(feature = "s3")]
    if let Some(rest) = source.strip_prefix("s3://") {
        let (bucket, key) = rest
            .split_once('/')
            .ok_or_else(|| Error::Io(format!("{source}: no key")))?;
        let store = object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| Error::Io(format!("{source}: {e}")))?;
        return Ok((Arc::new(store), Path::from(key)));
    }
    let local = match source.strip_prefix("file://") {
        Some(p) => std::path::PathBuf::from(p),
        None => std::path::PathBuf::from(source),
    };
    let local = std::fs::canonicalize(&local).map_err(|e| Error::Io(format!("{source}: {e}")))?;
    let path =
        Path::from_filesystem_path(&local).map_err(|e| Error::Io(format!("{source}: {e}")))?;
    Ok((Arc::new(object_store::local::LocalFileSystem::new()), path))
}

/// An [`AsyncFileReader`] for `source` (see [`store_for`]).
pub fn reader_for(source: &str) -> Result<Arc<dyn AsyncFileReader>> {
    let (store, path) = store_for(source)?;
    Ok(Arc::new(ObjectReader::new(store, path)))
}

#[derive(Default)]
struct Stats {
    requests: AtomicU64,
    bytes: AtomicU64,
}

/// A reader that counts the requests reaching the store underneath it and
/// retries a request that fails part way.
#[derive(Clone)]
pub(crate) struct Counting {
    inner: Arc<dyn AsyncFileReader>,
    stats: Arc<Stats>,
    retries: u32,
}

impl fmt::Debug for Counting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Counting")
    }
}

impl Counting {
    pub(crate) fn new(inner: Arc<dyn AsyncFileReader>, retries: u32) -> Self {
        Counting {
            inner,
            stats: Arc::default(),
            retries,
        }
    }

    pub(crate) fn requests(&self) -> u64 {
        self.stats.requests.load(Ordering::Relaxed)
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.stats.bytes.load(Ordering::Relaxed)
    }

    fn count(&self, n: u64, bytes: u64) {
        self.stats.requests.fetch_add(n, Ordering::Relaxed);
        self.stats.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl AsyncFileReader for Counting {
    async fn get_bytes(&self, range: Range<u64>) -> std::result::Result<Bytes, HDF5Error> {
        let mut tries = 0;
        loop {
            match self.inner.get_bytes(range.clone()).await {
                Ok(b) => {
                    self.count(1, b.len() as u64);
                    return Ok(b);
                }
                Err(_) if tries < self.retries => tries += 1,
                Err(e) => return Err(e),
            }
        }
    }

    async fn get_byte_ranges(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> std::result::Result<Vec<Bytes>, HDF5Error> {
        let mut tries = 0;
        loop {
            match self.inner.get_byte_ranges(ranges.clone()).await {
                Ok(v) => {
                    self.count(v.len() as u64, v.iter().map(|b| b.len() as u64).sum());
                    return Ok(v);
                }
                Err(_) if tries < self.retries => tries += 1,
                Err(e) => return Err(e),
            }
        }
    }

    async fn file_size(&self) -> std::result::Result<Option<u64>, HDF5Error> {
        self.inner.file_size().await
    }
}
