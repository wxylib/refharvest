//! HDF5 filter pipelines as Zarr v2 codecs and block-info `info` text.
//!
//! The pipeline is listed in write order. Zarr v2 applies `filters` and
//! then one `compressor`, so a pipeline maps when it is any number of
//! shuffles followed by at most one compressor. Everything else is
//! refused by name rather than referenced wrongly.

use async_hdf5::messages::datatype::ByteOrder;
use async_hdf5::FilterPipeline;
use serde_json::{json, Value};

use crate::Codec;

/// HDF5 filter ids (H5Zpublic.h and the HDF Group's registered filters).
pub mod ids {
    /// gzip / zlib.
    pub const DEFLATE: u16 = 1;
    /// Byte shuffle.
    pub const SHUFFLE: u16 = 2;
    /// Fletcher-32 checksum.
    pub const FLETCHER32: u16 = 3;
    /// SZIP.
    pub const SZIP: u16 = 4;
    /// N-bit packing.
    pub const NBIT: u16 = 5;
    /// Scale-offset.
    pub const SCALEOFFSET: u16 = 6;
    /// bzip2.
    pub const BZIP2: u16 = 307;
    /// LZF (h5py).
    pub const LZF: u16 = 32000;
    /// Blosc.
    pub const BLOSC: u16 = 32001;
    /// LZ4.
    pub const LZ4: u16 = 32004;
    /// Bitshuffle.
    pub const BITSHUFFLE: u16 = 32008;
    /// Zstandard.
    pub const ZSTD: u16 = 32015;
}

fn name(id: u16) -> String {
    match id {
        ids::DEFLATE => "deflate".into(),
        ids::SHUFFLE => "shuffle".into(),
        ids::FLETCHER32 => "fletcher32".into(),
        ids::SZIP => "szip".into(),
        ids::NBIT => "nbit".into(),
        ids::SCALEOFFSET => "scaleoffset".into(),
        ids::BZIP2 => "bzip2".into(),
        ids::LZF => "lzf".into(),
        ids::BLOSC => "blosc".into(),
        ids::LZ4 => "lz4".into(),
        ids::BITSHUFFLE => "bitshuffle".into(),
        ids::ZSTD => "zstd".into(),
        other => format!("filter {other}"),
    }
}

/// The codec for `pipeline` over elements of `element_size` bytes in
/// `order`, or the reason there is none.
pub fn map(
    pipeline: &FilterPipeline,
    element_size: u32,
    order: ByteOrder,
) -> Result<Codec, String> {
    let mut filters = Vec::new();
    let mut compressor = Value::Null;
    let mut info = Vec::new();
    let mut shuffled = false;
    for f in &pipeline.filters {
        if !compressor.is_null() {
            return Err(format!(
                "{} after the compressor has no Zarr v2 form",
                name(f.id)
            ));
        }
        match f.id {
            ids::SHUFFLE => {
                filters.push(json!({"id": "shuffle", "elementsize": element_size}));
                shuffled = true;
            }
            ids::DEFLATE => {
                let level = f.client_data.first().copied().unwrap_or(6);
                compressor = json!({"id": "zlib", "level": level});
                info.push("COMPRESSION=DEFLATE".to_string());
            }
            ids::ZSTD => {
                let level = f.client_data.first().copied().unwrap_or(3);
                compressor = json!({"id": "zstd", "level": level});
                info.push("COMPRESSION=ZSTD".to_string());
            }
            ids::FLETCHER32 => {
                return Err("fletcher32 checksums each stored chunk; not mapped yet".into())
            }
            other => return Err(format!("{} is not mapped to a Zarr codec", name(other))),
        }
    }
    if shuffled {
        info.push("FILTER=SHUFFLE".to_string());
    }
    match order {
        ByteOrder::LittleEndian => info.push("ENDIANNESS=LITTLE".to_string()),
        ByteOrder::BigEndian => info.push("ENDIANNESS=BIG".to_string()),
        ByteOrder::Vax => return Err("VAX byte order".into()),
        ByteOrder::NotApplicable => {}
    }
    Ok(Codec {
        v2: json!({"compressor": compressor, "filters": if filters.is_empty() { Value::Null } else { Value::Array(filters) }}),
        info: info.join("; "),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_hdf5::messages::filter_pipeline::Filter;

    fn pipeline(ids: &[(u16, &[u32])]) -> FilterPipeline {
        FilterPipeline {
            filters: ids
                .iter()
                .map(|(id, cd)| Filter {
                    id: *id,
                    name: None,
                    flags: 0,
                    client_data: cd.to_vec(),
                })
                .collect(),
        }
    }

    #[test]
    fn shuffle_then_deflate() {
        let c = map(
            &pipeline(&[(ids::SHUFFLE, &[2]), (ids::DEFLATE, &[1])]),
            2,
            ByteOrder::LittleEndian,
        )
        .unwrap();
        assert_eq!(
            c.info,
            "COMPRESSION=DEFLATE; FILTER=SHUFFLE; ENDIANNESS=LITTLE"
        );
        assert_eq!(
            c.v2,
            json!({"compressor": {"id": "zlib", "level": 1}, "filters": [{"id": "shuffle", "elementsize": 2}]})
        );
    }

    #[test]
    fn unfiltered_big_endian() {
        let c = map(&pipeline(&[]), 4, ByteOrder::BigEndian).unwrap();
        assert_eq!(c.info, "ENDIANNESS=BIG");
        assert_eq!(c.v2, json!({"compressor": null, "filters": null}));
    }

    #[test]
    fn refusals() {
        assert!(map(
            &pipeline(&[(ids::DEFLATE, &[4]), (ids::SHUFFLE, &[2])]),
            2,
            ByteOrder::LittleEndian
        )
        .unwrap_err()
        .contains("after the compressor"));
        assert!(
            map(&pipeline(&[(ids::SZIP, &[])]), 2, ByteOrder::LittleEndian)
                .unwrap_err()
                .contains("szip")
        );
        assert!(map(
            &pipeline(&[(ids::DEFLATE, &[4]), (ids::FLETCHER32, &[])]),
            2,
            ByteOrder::LittleEndian
        )
        .is_err());
    }
}
