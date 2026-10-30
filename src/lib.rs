//! # refharvest
//!
//! Harvest chunk byte references from files, so that their bytes can be
//! read in place as Zarr chunks: for every chunk of every array, the
//! source, the byte offset, the stored size and the codec, written as an
//! mdim-refs/0.2 table (wxylib/design `specs/mdim-refs-0.2.md`).
//!
//! The format parsers are borrowed, not written here. HDF5 (and so
//! netCDF-4) metadata comes from [async-hdf5](https://docs.rs/async-hdf5),
//! which reads by byte range through an `object_store` or any
//! [`AsyncFileReader`]. What this crate adds (design m23, section 3):
//!
//! - a **row contract**: [`FileRefs`] and [`ArrayRefs`], one per file and
//!   array, each with its [`ChunkRef`]s, and [`collate::concat`] to place
//!   files along a shared first axis;
//! - a **fetch plan**: chunk B-tree (v1) indexes are walked a level at a
//!   time with many requests in flight, because a file written one step at
//!   a time spreads its index nodes through the file and a walk that waits
//!   for each node costs one round trip per node;
//! - a **codec map** ([`codec`]) from the HDF5 filter pipeline to Zarr v2
//!   codec ids and the block-info `info` text, or a named [`Refusal`];
//! - the **cost** of each harvest ([`Cost`]: requests, bytes, seconds),
//!   kept with the table so a store says what it took to make.
//!
//! With the `parquet` feature (default) [`table::write`] writes the flat
//! export that wxylib's `wxy-refs` reads: `arrays.json`,
//! `refs_<array>.parquet` and `sources.parquet`.
//!
//! ```no_run
//! # async fn run() -> refharvest::Result<()> {
//! let opts = refharvest::Options::default();
//! let mut files = Vec::new();
//! for day in ["01", "02"] {
//!     let url = format!("https://example.org/oisst-avhrr-v02r01.198109{day}.nc");
//!     files.push(refharvest::hdf5::harvest(&url, &opts).await?);
//! }
//! let table = refharvest::collate::concat(files, None)?;
//! refharvest::table::write("oisst-refs", &table)?;
//! # Ok(())
//! # }
//! ```

use std::fmt;

pub use async_hdf5::AsyncFileReader;

mod btree;
pub mod codec;
pub mod collate;
pub mod hdf5;
pub mod io;
#[cfg(feature = "parquet")]
pub mod table;

/// Errors from harvesting, collating or writing.
#[derive(Debug)]
pub enum Error {
    /// A source could not be opened or read.
    Io(String),
    /// The file's structure could not be parsed.
    Format(String),
    /// Files could not be placed together in one table.
    Collate(String),
    /// The table could not be written.
    Write(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(m) => write!(f, "read: {m}"),
            Error::Format(m) => write!(f, "format: {m}"),
            Error::Collate(m) => write!(f, "collate: {m}"),
            Error::Write(m) => write!(f, "write: {m}"),
        }
    }
}

impl std::error::Error for Error {}

/// Result with this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// How to harvest.
#[derive(Debug, Clone)]
pub struct Options {
    /// Index node requests in flight at once (default 32).
    pub inflight: usize,
    /// Retries for a request that fails part way, such as a connection
    /// reset during a long transfer (default 3).
    pub retries: u32,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            inflight: 32,
            retries: 3,
        }
    }
}

/// One chunk: its position in the array's chunk grid and its bytes in
/// the source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRef {
    /// Chunk grid position, slowest axis first.
    pub index: Vec<u64>,
    /// Absolute byte offset in the source.
    pub offset: u64,
    /// Stored (filtered) size in bytes.
    pub size: u64,
    /// HDF5 filter mask: bit `i` set means filter `i` was skipped for this
    /// chunk. 0 elsewhere.
    pub filter_mask: u32,
}

/// Whether an array holds data or a coordinate (an HDF5 dimension scale).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A data variable.
    Data,
    /// A coordinate variable.
    Coord,
}

impl Kind {
    /// The mdim-refs `kind` value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::Data => "data",
            Kind::Coord => "coord",
        }
    }
}

/// The codec of every chunk of one array.
#[derive(Debug, Clone, PartialEq)]
pub struct Codec {
    /// Zarr v2 form, `{"compressor": ..., "filters": [...]}`.
    pub v2: serde_json::Value,
    /// Block-info text form, `KEY=VALUE` pairs joined by `"; "`.
    pub info: String,
}

/// One array of one source and its chunk references.
#[derive(Debug, Clone)]
pub struct ArrayRefs {
    /// Array name (the HDF5 dataset name).
    pub name: String,
    /// Data or coordinate.
    pub kind: Kind,
    /// Dimension names, slowest first (`dim_0`, ... when the file names none).
    pub dims: Vec<String>,
    /// Array shape, slowest first.
    pub shape: Vec<u64>,
    /// Chunk shape (the shape itself for contiguous storage).
    pub chunks: Vec<u64>,
    /// numpy dtype string, such as `<i2`.
    pub dtype: String,
    /// `_FillValue` as text, when the file states one.
    pub fill: Option<String>,
    /// Codec shared by every chunk.
    pub codec: Codec,
    /// Attributes as JSON (HDF5 and netCDF bookkeeping attributes left out).
    pub attrs: serde_json::Map<String, serde_json::Value>,
    /// Allocated chunks. Positions of the grid that are missing here were
    /// never written and read as `fill`.
    pub refs: Vec<ChunkRef>,
}

/// An array that was not referenced, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// Array name.
    pub array: String,
    /// Why its chunks cannot be read in place as Zarr chunks.
    pub reason: String,
}

/// What a harvest cost: requests that reached the store, bytes they
/// returned, and wall time.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Cost {
    /// Requests that succeeded (retries not counted).
    pub requests: u64,
    /// Bytes returned.
    pub bytes: u64,
    /// Seconds from open to the last chunk index.
    pub seconds: f64,
}

/// Everything harvested from one source.
#[derive(Debug, Clone)]
pub struct FileRefs {
    /// The source as given (URL or path), written verbatim into the table.
    pub source: String,
    /// Source size in bytes, when the store reports it.
    pub size: Option<u64>,
    /// Referenced arrays, in file order.
    pub arrays: Vec<ArrayRefs>,
    /// Arrays left out, with reasons.
    pub refused: Vec<Refusal>,
    /// What the harvest cost.
    pub cost: Cost,
}
