//! Level-parallel walk of an HDF5 version 1 B-tree of raw data chunks
//! (node type 1), the chunk index libhdf5 writes by default and so the
//! one in most netCDF-4 files.
//!
//! Every node of one level is fetched concurrently, then the next level.
//! The cost is one round trip per level instead of one per node (design
//! m23, section 2.2). Layout from the HDF5 File Format Specification,
//! "Version 1 B-trees": signature `TREE`, node type, level, entries used,
//! two sibling addresses, then key, child, key, ..., key; a type 1 key is
//! the stored chunk size (4 bytes), the filter mask (4) and one 8-byte
//! element offset per dimension plus a trailing 0.

use std::sync::Arc;

use async_hdf5::AsyncFileReader;
use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};

use crate::{ChunkRef, Error, Result};

fn le(b: &[u8], n: usize) -> u64 {
    b[..n].iter().rev().fold(0u64, |a, x| (a << 8) | *x as u64)
}

/// All chunks below the node at `root`, a file address relative to
/// `base`. `chunk_shape` has one entry per dataset dimension.
pub(crate) async fn chunks_v1(
    reader: &Arc<dyn AsyncFileReader>,
    root: u64,
    chunk_shape: &[u64],
    size_of_offsets: u8,
    base: u64,
    inflight: usize,
) -> Result<Vec<ChunkRef>> {
    let o = size_of_offsets as usize;
    let rank = chunk_shape.len();
    let key = 8 + 8 * (rank + 1);
    let header = 8 + 2 * o;
    // A full node with libhdf5's default K = 32 (64 children); a node that
    // says it holds more is read again at its own size.
    let guess = (header + 65 * key + 64 * o) as u64;
    if chunk_shape.contains(&0) {
        return Err(Error::Format("chunk shape has a zero extent".into()));
    }

    let mut level = vec![root];
    let mut rows = Vec::new();
    let mut depth = 0u32;
    while !level.is_empty() {
        if depth > 64 {
            return Err(Error::Format("chunk B-tree deeper than 64 levels".into()));
        }
        let nodes: Vec<Bytes> = stream::iter(level.iter().copied())
            .map(|addr| async move {
                let start = base + addr;
                let mut b = reader.get_bytes(start..start + guess).await?;
                if b.len() >= header {
                    let n = le(&b[6..], 2) as usize;
                    let need = (header + (n + 1) * key + n * o) as u64;
                    if need > b.len() as u64 {
                        b = reader.get_bytes(start..start + need).await?;
                    }
                }
                Ok::<_, async_hdf5::HDF5Error>(b)
            })
            .buffer_unordered(inflight.max(1))
            .try_collect()
            .await
            .map_err(|e| Error::Io(e.to_string()))?;

        let mut next = Vec::new();
        for b in nodes {
            if b.len() < header || &b[0..4] != b"TREE" {
                return Err(Error::Format(
                    "chunk B-tree node without TREE signature".into(),
                ));
            }
            if b[4] != 1 {
                return Err(Error::Format(format!(
                    "B-tree node type {} is not a chunk index",
                    b[4]
                )));
            }
            let node_level = b[5];
            let n = le(&b[6..], 2) as usize;
            if b.len() < header + (n + 1) * key + n * o {
                return Err(Error::Format(
                    "chunk B-tree node shorter than its entries".into(),
                ));
            }
            let mut p = header;
            for _ in 0..n {
                let k = &b[p..p + key];
                let child = le(&b[p + key..], o);
                if node_level == 0 {
                    let index = (0..rank)
                        .map(|d| le(&k[8 + 8 * d..], 8) / chunk_shape[d])
                        .collect();
                    rows.push(ChunkRef {
                        index,
                        offset: base + child,
                        size: le(k, 4),
                        filter_mask: le(&k[4..], 4) as u32,
                    });
                } else {
                    next.push(child);
                }
                p += key + o;
            }
        }
        level = next;
        depth += 1;
    }
    rows.sort_by(|a, b| a.index.cmp(&b.index));
    Ok(rows)
}
