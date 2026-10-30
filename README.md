# refharvest

Harvest chunk byte references from files: for every chunk of every array,
the source, the byte offset, the stored size and the codec, written as an
[mdim-refs/0.2](https://github.com/wxylib/design/blob/main/specs/mdim-refs-0.2.md)
table. A reader such as wxylib's `wxy-refs` can then read those bytes in
place as Zarr chunks, or render the table as kerchunk or Icechunk.

Status: stage 1 of [design m23](https://github.com/wxylib/design/blob/main/docs/m23-byte-ref-harvesting.md),
HDF5 and netCDF-4. Not on crates.io.

## What it is

The format parsers are borrowed. HDF5 metadata comes from
[async-hdf5](https://crates.io/crates/async-hdf5), which reads by byte
range through `object_store`. refharvest adds what sits around a parser:

- **Rows.** `FileRefs` and `ArrayRefs` per file and array, with their
  `ChunkRef`s; `collate::concat` stacks files along a shared first
  dimension (a daily series becomes one time axis); `table::write`
  writes `arrays.json`, `refs_<array>.parquet` and `sources.parquet`.
  Every chunk grid position gets a row, and chunks never written are
  `present = 0`.
- **A fetch plan.** Version 1 chunk B-trees, libhdf5's default and so
  most netCDF-4 files, are walked a level at a time with many requests in
  flight. A file written one time step at a time spreads its index nodes
  through the whole file, so a walk that waits for each node costs a
  round trip per node.
- **A codec map.** Shuffle, deflate and zstd map to Zarr v2 codec ids and
  the block-info `info` text. Anything else (szip, fletcher32, scale-offset,
  blosc, a filter after the compressor, a chunk that skips a filter) is a
  named refusal in the table, not a wrong reference.
- **Cost.** Each source's requests, bytes and seconds go into
  `sources.parquet`, so a table says what it took to make.

## Use

```
cargo run --release -- hdf5 \
  https://noaa-cdr-sea-surface-temp-optimum-interpolation-pds.s3.amazonaws.com/data/v2.1/avhrr/198109/oisst-avhrr-v02r01.19810901.nc \
  https://noaa-cdr-sea-surface-temp-optimum-interpolation-pds.s3.amazonaws.com/data/v2.1/avhrr/198109/oisst-avhrr-v02r01.19810902.nc \
  --out oisst-refs
```

Sources can be paths, `file://`, `http(s)://` or (with the `s3` feature)
`s3://` URLs, or `@list.txt` with one per line. `--concat-dim` names the
stacking dimension (default: the first dimension of the first data
array), `--inflight` the index requests in flight per file (32) and
`--files` the files harvested at once (8).

From Rust:

```rust
let opts = refharvest::Options::default();
let a = refharvest::hdf5::harvest("a.nc", &opts).await?;
let b = refharvest::hdf5::harvest("b.nc", &opts).await?;
let table = refharvest::collate::concat(vec![a, b], None)?;
refharvest::table::write("refs", &table)?;
```

Any `async_hdf5::AsyncFileReader` can be harvested with
`hdf5::harvest_reader`. async-hdf5 0.1 depends on object_store 0.13.

## Checked against

| Source | Oracle | Result |
|---|---|---|
| `tests/data/btree_v1.h5`, 1,400 chunks in a two-level B-tree, written by libhdf5 | libhdf5 `H5Dchunk_iter` (rhdf5 `h5getAllChunkInfo`) | identical, at 1, 3 and 32 in flight |
| a file written by hdf5-pure with a fixed array index | hdf5-pure `Dataset::chunks()` | identical |
| wxylib/fixtures `mdimrefs/` (3 OISST files, one chunk never written) | GDAL `GetRawBlockInfo` table | identical rows, absent chunk included |
| 30 OISST v2.1 files over HTTPS (NOAA bucket) | fixtures `mdimrefs/oisst-198109` (GDAL) | all 120 rows identical, 4.9 s |
| BRAN2023 `ocean_temp_2010_01.nc` and `_02.nc` over HTTPS (NCI THREDDS, 9.9 GB) | fixtures `mdimrefs/bran2023-temp-201001` (VirtualiZarr, libhdf5) | all 97,920 fixture rows identical; 180,540 refs in 69 s, 39.6 MB read |

The first three run in `cargo test` (the fixtures one needs a
wxylib/fixtures checkout at `$WXY_FIXTURES` or `../fixtures`).

Differences from the fixture tables, both in the fixtures: the OISST
table records deflate level 1 where the files' filter pipelines say 4
(the level does not change decoding), and the BRAN table's
`scale_factor` is 0.0078 where the file says 0.007782219909131527
(design m17).

## Gaps in async-hdf5 0.1.0, filled here

- Version 3 attribute messages fail to parse: the name character set
  byte is read only when flag bit 0x10 is set, but every version 3
  message has it. That loses every attribute of a netCDF-4 file written
  by libhdf5 1.8 or later. refharvest sets the flag on a copy before
  parsing.
- Dense attributes (an attribute info message, a fractal heap and a
  name-index B-tree, used from 8 attributes on) are not read. refharvest
  reads them with async-hdf5's own heap and B-tree readers.
- The chunk B-tree walk waits for each node. refharvest walks version 1
  trees itself.

These are meant to go upstream; each workaround is removed when a release
carries the fix.

## Not yet

Groups below the root; coordinates as inline values or affine (a
coordinate whose chunks cannot follow from file to file, such as a time
axis chunked at 1024 with one step per file, is refused); compact
storage; filters beyond shuffle, deflate and zstd; TIFF (stage 3 moves
wxylib's `scan_tiff` here); netCDF classic and GRIB.

## Licence

MIT or Apache-2.0, at your option.
