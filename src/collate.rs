//! Placing harvested files in one table.
//!
//! [`concat`] stacks files along one dimension, the way a daily or
//! monthly series of netCDF files makes one long time axis: an array that
//! has the dimension first gets every file's chunks, offset along that
//! axis; an array without it (a coordinate such as `lat`, or a constant
//! field) is taken from the first file and must have the same shape in
//! the others. Every position of the chunk grid gets a row; positions no
//! file wrote are absent (`present = 0`) and read as `fill`.

use crate::{ArrayRefs, Cost, Error, FileRefs, Kind, Refusal, Result};

/// One row of a table: a chunk position and, when present, where its
/// bytes are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// Global chunk grid position, slowest axis first.
    pub index: Vec<u64>,
    /// Index into [`Table::sources`], or `None` when the chunk is absent.
    pub source: Option<usize>,
    /// Byte offset in the source.
    pub offset: u64,
    /// Stored size in bytes.
    pub size: u64,
}

/// An array of the table: its description (with the global shape) and its
/// rows in chunk grid order.
#[derive(Debug, Clone)]
pub struct TableArray {
    /// Description; `refs` is empty, the rows are in `rows`.
    pub meta: ArrayRefs,
    /// One row per chunk grid position.
    pub rows: Vec<Row>,
}

/// A source and what harvesting it cost.
#[derive(Debug, Clone)]
pub struct Source {
    /// URL or path, verbatim.
    pub source: String,
    /// Size in bytes, when known.
    pub size: Option<u64>,
    /// Harvest cost.
    pub cost: Cost,
}

/// Arrays, sources and refusals of one table.
#[derive(Debug, Clone)]
pub struct Table {
    /// The dimension files were stacked along, if any array has it.
    pub concat_dim: Option<String>,
    /// Arrays in first-seen order.
    pub arrays: Vec<TableArray>,
    /// Sources in the order given.
    pub sources: Vec<Source>,
    /// Arrays left out, with the source they were refused in.
    pub refused: Vec<(String, Refusal)>,
}

/// Stack `files` along `dim` (default: the first dimension of the first
/// data array of the first file).
pub fn concat(files: Vec<FileRefs>, dim: Option<&str>) -> Result<Table> {
    let Some(first) = files.first() else {
        return Err(Error::Collate("no files".into()));
    };
    let concat_dim = dim.map(str::to_string).or_else(|| {
        first
            .arrays
            .iter()
            .find(|a| a.kind == Kind::Data && !a.dims.is_empty())
            .map(|a| a.dims[0].clone())
    });

    let mut names: Vec<String> = Vec::new();
    for f in &files {
        for a in &f.arrays {
            if !names.contains(&a.name) {
                names.push(a.name.clone());
            }
        }
    }

    let mut arrays = Vec::new();
    let mut refused: Vec<(String, Refusal)> = files
        .iter()
        .flat_map(|f| f.refused.iter().map(|r| (f.source.clone(), r.clone())))
        .collect();
    for name in &names {
        match stack(&files, name, concat_dim.as_deref()) {
            Ok(a) => arrays.push(a),
            Err((source, reason)) => refused.push((
                source,
                Refusal {
                    array: name.clone(),
                    reason,
                },
            )),
        }
    }

    let sources = files
        .iter()
        .map(|f| Source {
            source: f.source.clone(),
            size: f.size,
            cost: f.cost,
        })
        .collect();
    Ok(Table {
        concat_dim,
        arrays,
        sources,
        refused,
    })
}

/// One array across `files`, or the source and reason it cannot be
/// placed in one regular chunk grid.
fn stack(
    files: &[FileRefs],
    name: &str,
    concat_dim: Option<&str>,
) -> std::result::Result<TableArray, (String, String)> {
    let parts: Vec<(usize, &ArrayRefs)> = files
        .iter()
        .enumerate()
        .filter_map(|(i, f)| f.arrays.iter().find(|a| a.name == name).map(|a| (i, a)))
        .collect();
    let (_, a0) = parts[0];
    let stacked = concat_dim.is_some_and(|d| a0.dims.first().map(String::as_str) == Some(d));
    for (i, a) in &parts[1..] {
        let what = |m: &str| {
            (
                files[*i].source.clone(),
                format!("{m} differs from {}", files[parts[0].0].source),
            )
        };
        if a.dims != a0.dims {
            return Err(what("dims"));
        }
        if a.chunks != a0.chunks {
            return Err(what("chunks"));
        }
        if a.dtype != a0.dtype {
            return Err(what("dtype"));
        }
        if a.codec != a0.codec {
            return Err(what("codec"));
        }
        if stacked && a.shape[1..] != a0.shape[1..] || !stacked && a.shape != a0.shape {
            return Err(what("shape"));
        }
    }
    if !stacked {
        return Ok(place(a0, &[(parts[0].0, a0, 0)]));
    }
    if parts.len() != files.len() {
        let missing: Vec<&str> = files
            .iter()
            .filter(|f| !f.arrays.iter().any(|a| a.name == name))
            .map(|f| f.source.as_str())
            .collect();
        return Err((
            missing[0].to_string(),
            format!("missing from {} of {} files", missing.len(), files.len()),
        ));
    }
    let c0 = a0.chunks[0];
    let mut start = 0u64;
    let mut placed = Vec::new();
    for (k, (i, a)) in parts.iter().enumerate() {
        if a.shape[0] % c0 != 0 && k + 1 != parts.len() {
            return Err((
                    files[*i].source.clone(),
                    format!(
                        "{} steps along {} is not a whole number of {c0}-step chunks, so the next file cannot follow it in one chunk grid",
                        a.shape[0], a0.dims[0]
                    ),
                ));
        }
        placed.push((*i, *a, start / c0));
        start += a.shape[0];
    }
    let mut meta = a0.clone();
    meta.shape[0] = start;
    Ok(place(&meta, &placed))
}

/// Rows for `meta`'s full chunk grid from `parts` (source index, array,
/// first chunk along axis 0).
fn place(meta: &ArrayRefs, parts: &[(usize, &ArrayRefs, u64)]) -> TableArray {
    let grid: Vec<u64> = meta
        .shape
        .iter()
        .zip(&meta.chunks)
        .map(|(s, c)| s.div_ceil(*c))
        .collect();
    let total: u64 = grid.iter().product();
    let mut rows: Vec<Row> = (0..total)
        .map(|mut k| {
            let mut index = vec![0; grid.len()];
            for d in (0..grid.len()).rev() {
                index[d] = k % grid[d];
                k /= grid[d];
            }
            Row {
                index,
                source: None,
                offset: 0,
                size: 0,
            }
        })
        .collect();
    let flat = |index: &[u64]| index.iter().zip(&grid).fold(0u64, |a, (i, g)| a * g + i);
    for (src, a, first) in parts {
        for c in &a.refs {
            let mut index = c.index.clone();
            if !index.is_empty() {
                index[0] += first;
            }
            if index.iter().zip(&grid).any(|(i, g)| i >= g) {
                continue;
            }
            let r = &mut rows[flat(&index) as usize];
            r.source = Some(*src);
            r.offset = c.offset;
            r.size = c.size;
        }
    }
    let mut meta = meta.clone();
    meta.refs.clear();
    TableArray { meta, rows }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChunkRef, Codec};

    fn array(
        name: &str,
        dims: &[&str],
        shape: &[u64],
        chunks: &[u64],
        refs: &[(&[u64], u64)],
    ) -> ArrayRefs {
        ArrayRefs {
            name: name.into(),
            kind: if dims.len() == 1 && dims[0] == name {
                Kind::Coord
            } else {
                Kind::Data
            },
            dims: dims.iter().map(|d| d.to_string()).collect(),
            shape: shape.to_vec(),
            chunks: chunks.to_vec(),
            dtype: "<f4".into(),
            fill: None,
            codec: Codec {
                v2: serde_json::json!({"compressor": null, "filters": null}),
                info: "ENDIANNESS=LITTLE".into(),
            },
            attrs: Default::default(),
            refs: refs
                .iter()
                .map(|(i, o)| ChunkRef {
                    index: i.to_vec(),
                    offset: *o,
                    size: 10,
                    filter_mask: 0,
                })
                .collect(),
        }
    }

    fn file(source: &str, arrays: Vec<ArrayRefs>) -> FileRefs {
        FileRefs {
            source: source.into(),
            size: None,
            arrays,
            refused: vec![],
            cost: Cost::default(),
        }
    }

    #[test]
    fn stacks_along_time_and_keeps_absent_chunks() {
        let a = file(
            "a.nc",
            vec![
                array(
                    "v",
                    &["time", "y"],
                    &[2, 4],
                    &[1, 2],
                    &[(&[0, 0], 100), (&[0, 1], 110), (&[1, 0], 120)],
                ),
                array("y", &["y"], &[4], &[4], &[(&[0], 5)]),
            ],
        );
        let b = file(
            "b.nc",
            vec![
                array("v", &["time", "y"], &[1, 4], &[1, 2], &[(&[0, 1], 200)]),
                array("y", &["y"], &[4], &[4], &[(&[0], 5)]),
            ],
        );
        let t = concat(vec![a, b], None).unwrap();
        assert_eq!(t.concat_dim.as_deref(), Some("time"));
        let v = &t.arrays[0];
        assert_eq!(v.meta.shape, vec![3, 4]);
        let rows: Vec<(Vec<u64>, Option<usize>, u64)> = v
            .rows
            .iter()
            .map(|r| (r.index.clone(), r.source, r.offset))
            .collect();
        assert_eq!(
            rows,
            vec![
                (vec![0, 0], Some(0), 100),
                (vec![0, 1], Some(0), 110),
                (vec![1, 0], Some(0), 120),
                (vec![1, 1], None, 0),
                (vec![2, 0], None, 0),
                (vec![2, 1], Some(1), 200),
            ]
        );
        let y = &t.arrays[1];
        assert_eq!(y.meta.shape, vec![4]);
        assert_eq!(y.rows.len(), 1);
    }

    #[test]
    fn refuses_a_partial_chunk_before_the_last_file() {
        let a = file(
            "a.nc",
            vec![
                array("v", &["time", "y"], &[1, 4], &[1, 4], &[(&[0, 0], 1)]),
                array("time", &["time"], &[1], &[1024], &[(&[0], 9)]),
            ],
        );
        let b = file(
            "b.nc",
            vec![
                array("v", &["time", "y"], &[1, 4], &[1, 4], &[(&[0, 0], 2)]),
                array("time", &["time"], &[1], &[1024], &[(&[0], 9)]),
            ],
        );
        let t = concat(vec![a, b], Some("time")).unwrap();
        assert_eq!(t.arrays.len(), 1);
        assert_eq!(t.refused.len(), 1);
        assert_eq!(t.refused[0].1.array, "time");
    }
}
