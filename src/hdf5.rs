//! HDF5 and netCDF-4 sources, through async-hdf5.
//!
//! async-hdf5 reads the superblock, object headers, layouts, filters and
//! attributes. Version 1 chunk B-trees are walked here a level at a time
//! (see the crate docs); every other index kind (fixed and extensible
//! arrays, version 2 B-trees, single chunk) and contiguous storage use
//! async-hdf5's own `chunk_index()`.
//!
//! netCDF-4 conventions read here: dimension names come from each
//! dataset's `DIMENSION_LIST` (object references to its dimension
//! scales, resolved through the global heap), a dataset whose `CLASS` is
//! `DIMENSION_SCALE` is a coordinate, and a dimension scale that netCDF
//! wrote only to name a dimension (no variable) is left out. Only the root
//! group is read.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_hdf5::messages::data_layout::ChunkIndexType;
use async_hdf5::messages::datatype::ByteOrder;
use async_hdf5::{AsyncFileReader, AttributeValue, DataType, HDF5Dataset, HDF5File, StorageLayout};
use serde_json::{json, Map, Value};

use crate::io::Counting;
use crate::{
    btree, codec, ArrayRefs, ChunkRef, Cost, Error, FileRefs, Kind, Options, Refusal, Result,
};

/// Attributes that are HDF5 or netCDF bookkeeping, not CF metadata.
const INTERNAL: &[&str] = &[
    "DIMENSION_LIST",
    "REFERENCE_LIST",
    "CLASS",
    "NAME",
    "_Netcdf4Dimid",
    "_Netcdf4Coordinates",
    "_nc3_strict",
    "_NCProperties",
];

/// The text netCDF-4 puts in `NAME` of a dimension scale that is not a
/// variable.
const NOT_A_VARIABLE: &str = "This is a netCDF dimension but not a netCDF variable";

/// Harvest every array in the root group of the HDF5 file at `source`
/// (see [`crate::io::store_for`] for the forms a source can take).
pub async fn harvest(source: &str, opts: &Options) -> Result<FileRefs> {
    harvest_reader(source, crate::io::reader_for(source)?, opts).await
}

/// Harvest from any reader; `source` is the name written into the table.
pub async fn harvest_reader(
    source: &str,
    reader: Arc<dyn AsyncFileReader>,
    opts: &Options,
) -> Result<FileRefs> {
    let t0 = Instant::now();
    let counting = Counting::new(reader, opts.retries);
    let size = counting.file_size().await.ok().flatten();
    let file = HDF5File::open(counting.clone())
        .await
        .map_err(|e| Error::Format(format!("{source}: {e}")))?;
    let sb = file.superblock().clone();
    let root = file
        .root_group()
        .await
        .map_err(|e| Error::Format(format!("{source}: {e}")))?;
    let children = root
        .children()
        .await
        .map_err(|e| Error::Format(format!("{source}: {e}")))?;
    let names_by_address: HashMap<u64, String> = children
        .iter()
        .map(|c| (c.address, c.name.clone()))
        .collect();
    let datasets = root
        .dataset_names()
        .await
        .map_err(|e| Error::Format(format!("{source}: {e}")))?;

    let raw: Arc<dyn AsyncFileReader> = Arc::new(counting.clone());
    let mut arrays = Vec::new();
    let mut refused = Vec::new();
    let mut dim_lengths: HashMap<String, u64> = HashMap::new();
    for name in datasets {
        let ds = root
            .dataset(&name)
            .await
            .map_err(|e| Error::Format(format!("{source}: {name}: {e}")))?;
        let attrs = attributes(&ds, file.reader(), sb.size_of_offsets, sb.size_of_lengths).await;
        let class = attrs
            .iter()
            .find(|a| a.name == "CLASS")
            .and_then(|a| a.value.as_str().map(str::to_string));
        let nc_name = attrs
            .iter()
            .find(|a| a.name == "NAME")
            .and_then(|a| a.value.as_str().map(str::to_string));
        if class.as_deref() == Some("DIMENSION_SCALE") && ds.shape().len() == 1 {
            dim_lengths.insert(name.clone(), ds.shape()[0]);
        }
        if nc_name
            .as_deref()
            .is_some_and(|n| n.starts_with(NOT_A_VARIABLE))
        {
            continue;
        }
        let kind = if class.as_deref() == Some("DIMENSION_SCALE") {
            Kind::Coord
        } else {
            Kind::Data
        };
        match array(
            &ds,
            &name,
            kind,
            &attrs,
            &file,
            &names_by_address,
            &raw,
            sb.size_of_offsets,
            sb.size_of_lengths,
            sb.base_address,
            opts,
        )
        .await
        {
            Ok(a) => arrays.push(a),
            Err(reason) => refused.push(Refusal {
                array: name,
                reason,
            }),
        }
    }
    // A variable that was never written along an unlimited dimension has
    // a dataspace of 0 there, while netCDF gives every variable the
    // dimension's length (the longest variable's): its chunks are absent,
    // not missing.
    for a in &mut arrays {
        for (d, n) in a.dims.iter().enumerate() {
            if let Some(len) = dim_lengths.get(n) {
                a.shape[d] = a.shape[d].max(*len);
            }
        }
    }

    Ok(FileRefs {
        source: source.to_string(),
        size,
        arrays,
        refused,
        cost: Cost {
            requests: counting.requests(),
            bytes: counting.bytes(),
            seconds: t0.elapsed().as_secs_f64(),
        },
    })
}

#[allow(clippy::too_many_arguments)]
async fn array(
    ds: &HDF5Dataset,
    name: &str,
    kind: Kind,
    attrs: &[async_hdf5::Attribute],
    file: &HDF5File,
    names_by_address: &HashMap<u64, String>,
    raw: &Arc<dyn AsyncFileReader>,
    size_of_offsets: u8,
    size_of_lengths: u8,
    base: u64,
    opts: &Options,
) -> std::result::Result<ArrayRefs, String> {
    if ds.has_external_storage() {
        return Err("external storage (bytes in another file)".into());
    }
    if ds.is_null_dataspace() {
        return Err("null dataspace".into());
    }
    let (dtype, order) = numpy_dtype(ds.dtype())?;
    let shape = ds.shape().to_vec();

    let (chunks, refs) = match ds.layout() {
        StorageLayout::Compact { .. } => {
            return Err("compact storage (bytes in the object header)".into())
        }
        StorageLayout::Chunked {
            chunk_shape,
            index_address,
            indexing_type: ChunkIndexType::BTreeV1,
            ..
        } if !undefined(*index_address, size_of_offsets) => {
            let refs = btree::chunks_v1(
                raw,
                *index_address,
                chunk_shape,
                size_of_offsets,
                base,
                opts.inflight,
            )
            .await
            .map_err(|e| e.to_string())?;
            (chunk_shape.clone(), refs)
        }
        _ => {
            let index = ds
                .chunk_index()
                .await
                .map_err(|e| format!("chunk index: {e}"))?;
            let chunks = match ds.chunk_shape() {
                Some(c) => c.to_vec(),
                None => shape.clone(),
            };
            let refs = index
                .iter()
                .map(|c| ChunkRef {
                    index: c.indices.clone(),
                    offset: c.byte_offset,
                    size: c.byte_length,
                    filter_mask: c.filter_mask,
                })
                .collect();
            (chunks, refs)
        }
    };
    if let Some(c) = refs.iter().find(|c: &&ChunkRef| c.filter_mask != 0) {
        return Err(format!(
            "chunk {:?} skips filters (mask {:#x}), so the array has no single codec",
            c.index, c.filter_mask
        ));
    }
    let codec = codec::map(ds.filters(), ds.element_size(), order)?;

    let dims = match attrs.iter().find(|a| a.name == "DIMENSION_LIST") {
        Some(a) => match dimension_names(
            &a.value,
            shape.len(),
            file,
            names_by_address,
            size_of_offsets,
            size_of_lengths,
        )
        .await
        {
            Some(d) => d,
            None => default_dims(shape.len()),
        },
        None if kind == Kind::Coord && shape.len() == 1 => vec![name.to_string()],
        None => default_dims(shape.len()),
    };

    let fill = attrs
        .iter()
        .find(|a| a.name == "_FillValue")
        .and_then(|a| scalar_text(&a.value));
    let mut json_attrs = Map::new();
    for a in attrs {
        if INTERNAL.contains(&a.name.as_str()) {
            continue;
        }
        if let Some(v) = attr_json(&a.value) {
            json_attrs.insert(a.name.clone(), v);
        }
    }

    Ok(ArrayRefs {
        name: name.to_string(),
        kind,
        dims,
        shape,
        chunks,
        dtype,
        fill,
        codec,
        attrs: json_attrs,
        refs,
    })
}

/// The dataset's attributes, compact and dense, with variable-length
/// strings resolved.
///
/// Two gaps in async-hdf5 0.1.0 are filled here until they are fixed
/// upstream:
///
/// - It reads the name character set byte of a version 3 attribute
///   message only when flag bit 0x10 is set, but the HDF5 specification
///   puts that byte in every version 3 message, so each name starts one
///   byte early and the parse fails (every attribute of a netCDF-4 file
///   written by libhdf5 1.8 or later). The flag is set on a copy of the
///   message before parsing.
/// - It reads compact attributes only. Dense attributes (an attribute
///   info message pointing at a fractal heap and a name-index version 2
///   B-tree, which libhdf5 switches to at 8 or more attributes) are read
///   here with async-hdf5's own heap and B-tree readers.
async fn attributes(
    ds: &HDF5Dataset,
    reader: &Arc<dyn AsyncFileReader>,
    size_of_offsets: u8,
    size_of_lengths: u8,
) -> Vec<async_hdf5::Attribute> {
    use async_hdf5::messages::attribute::AttributeMessage;
    use async_hdf5::object_header::msg_types;
    let mut messages: Vec<bytes::Bytes> = ds
        .header()
        .find_messages(msg_types::ATTRIBUTE)
        .into_iter()
        .map(|m| m.data.clone())
        .collect();
    if let Some(info) = ds.header().find_message(msg_types::ATTRIBUTE_INFO) {
        messages.extend(
            dense_attribute_messages(&info.data, reader, size_of_offsets, size_of_lengths).await,
        );
    }
    let mut out = Vec::new();
    for data in messages {
        let data = if data.first() == Some(&3) && data.len() > 1 {
            let mut v = data.to_vec();
            v[1] |= 0x10;
            bytes::Bytes::from(v)
        } else {
            data
        };
        let Ok(am) = AttributeMessage::parse(&data, size_of_offsets, size_of_lengths) else {
            continue;
        };
        match am
            .to_attribute_resolved(reader, size_of_offsets, size_of_lengths)
            .await
        {
            Ok(a) => out.push(a),
            Err(_) => out.push(am.to_attribute()),
        }
    }
    out
}

/// The attribute messages held in dense storage: each record of the
/// name-index B-tree (type 8) starts with the 8-byte fractal heap id of
/// one attribute message. Empty when the storage cannot be read.
async fn dense_attribute_messages(
    info: &bytes::Bytes,
    reader: &Arc<dyn AsyncFileReader>,
    size_of_offsets: u8,
    size_of_lengths: u8,
) -> Vec<bytes::Bytes> {
    use async_hdf5::btree::v2::{collect_all_records, BTreeV2Header};
    use async_hdf5::heap::fractal::FractalHeap;
    use async_hdf5::messages::attribute_info::AttributeInfoMessage;
    let Ok(info) = AttributeInfoMessage::parse(info, size_of_offsets, size_of_lengths) else {
        return Vec::new();
    };
    if undefined(info.fractal_heap_address, size_of_offsets)
        || undefined(info.name_btree_address, size_of_offsets)
    {
        return Vec::new();
    }
    let Ok(heap) = FractalHeap::read(
        reader,
        info.fractal_heap_address,
        size_of_offsets,
        size_of_lengths,
    )
    .await
    else {
        return Vec::new();
    };
    let Ok(header) = BTreeV2Header::read(
        reader,
        info.name_btree_address,
        size_of_offsets,
        size_of_lengths,
    )
    .await
    else {
        return Vec::new();
    };
    let Ok(records) = collect_all_records(reader, &header, size_of_offsets, size_of_lengths).await
    else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(records.len());
    for r in records {
        if r.len() < 8 {
            continue;
        }
        if let Ok(m) = heap.get_object(&r[..8]).await {
            out.push(m);
        }
    }
    out
}

fn undefined(addr: u64, size_of_offsets: u8) -> bool {
    match size_of_offsets {
        8 => addr == u64::MAX,
        n => addr == (1u64 << (8 * n as u32)) - 1,
    }
}

fn default_dims(rank: usize) -> Vec<String> {
    (0..rank).map(|i| format!("dim_{i}")).collect()
}

/// numpy dtype string and byte order, or why the type is not referenced.
fn numpy_dtype(t: &DataType) -> std::result::Result<(String, ByteOrder), String> {
    let (code, size, order) = match t {
        DataType::FixedPoint {
            size,
            signed,
            byte_order,
            ..
        } => (if *signed { 'i' } else { 'u' }, *size, *byte_order),
        DataType::FloatingPoint {
            size, byte_order, ..
        } => ('f', *size, *byte_order),
        other => return Err(format!("data type {other:?} is not a fixed-size number")),
    };
    let prefix = match (size, order) {
        (1, _) => '|',
        (_, ByteOrder::LittleEndian) => '<',
        (_, ByteOrder::BigEndian) => '>',
        (_, o) => return Err(format!("byte order {o:?}")),
    };
    let order = if size == 1 {
        ByteOrder::NotApplicable
    } else {
        order
    };
    Ok((format!("{prefix}{code}{size}"), order))
}

/// Dimension names from a `DIMENSION_LIST` value: one variable-length
/// sequence of object references per dimension, each held in the global
/// heap. `None` when any dimension cannot be resolved.
async fn dimension_names(
    value: &AttributeValue,
    rank: usize,
    file: &HDF5File,
    names_by_address: &HashMap<u64, String>,
    size_of_offsets: u8,
    size_of_lengths: u8,
) -> Option<Vec<String>> {
    let AttributeValue::Raw(raw) = value else {
        return None;
    };
    let o = size_of_offsets as usize;
    let elem = 4 + o + 4;
    if raw.len() < rank * elem {
        return None;
    }
    let le = |b: &[u8]| b.iter().rev().fold(0u64, |a, x| (a << 8) | *x as u64);
    let mut out = Vec::with_capacity(rank);
    for i in 0..rank {
        let e = &raw[i * elem..(i + 1) * elem];
        let collection = le(&e[4..4 + o]);
        let index = le(&e[4 + o..]) as u32;
        let obj = async_hdf5::heap::global::read_global_heap_object(
            file.reader(),
            collection,
            index,
            size_of_offsets,
            size_of_lengths,
        )
        .await
        .ok()?;
        if obj.len() < o {
            return None;
        }
        out.push(names_by_address.get(&le(&obj[..o]))?.clone());
    }
    Some(out)
}

fn f32_json(v: f32) -> Value {
    // Widen as the value is, so 0.01f32 reads 0.009999999776482582 and
    // decodes to the same f32 (rounding it to fewer digits shifts packed
    // data; design m17).
    json!(v as f64)
}

fn attr_json(v: &AttributeValue) -> Option<Value> {
    fn one_or_many<T: Clone>(v: &[T], f: impl Fn(&T) -> Value) -> Value {
        if v.len() == 1 {
            f(&v[0])
        } else {
            Value::Array(v.iter().map(f).collect())
        }
    }
    Some(match v {
        AttributeValue::I8(x) => one_or_many(x, |a| json!(a)),
        AttributeValue::I16(x) => one_or_many(x, |a| json!(a)),
        AttributeValue::I32(x) => one_or_many(x, |a| json!(a)),
        AttributeValue::I64(x) => one_or_many(x, |a| json!(a)),
        AttributeValue::U8(x) => one_or_many(x, |a| json!(a)),
        AttributeValue::U16(x) => one_or_many(x, |a| json!(a)),
        AttributeValue::U32(x) => one_or_many(x, |a| json!(a)),
        AttributeValue::U64(x) => one_or_many(x, |a| json!(a)),
        AttributeValue::F32(x) => one_or_many(x, |a| f32_json(*a)),
        AttributeValue::F64(x) => one_or_many(x, |a| json!(a)),
        AttributeValue::String(s) => json!(s),
        AttributeValue::Raw(_) => return None,
    })
}

/// A scalar attribute as text, for `fill`.
fn scalar_text(v: &AttributeValue) -> Option<String> {
    if !v.is_scalar() {
        return None;
    }
    Some(match v {
        AttributeValue::I8(x) => x[0].to_string(),
        AttributeValue::I16(x) => x[0].to_string(),
        AttributeValue::I32(x) => x[0].to_string(),
        AttributeValue::I64(x) => x[0].to_string(),
        AttributeValue::U8(x) => x[0].to_string(),
        AttributeValue::U16(x) => x[0].to_string(),
        AttributeValue::U32(x) => x[0].to_string(),
        AttributeValue::U64(x) => x[0].to_string(),
        AttributeValue::F32(x) => float_text(x[0] as f64),
        AttributeValue::F64(x) => float_text(x[0]),
        _ => return None,
    })
}

fn float_text(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        }
    } else {
        v.to_string()
    }
}
