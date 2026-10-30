//! The wxylib/fixtures OISST table (GDAL `GetRawBlockInfo`, libhdf5):
//! three netCDF-4 files, one never-written `ice` chunk. refharvest's
//! table must have the same rows. Needs a fixtures checkout at
//! `$WXY_FIXTURES` (or `../fixtures`); skipped without one.

#![cfg(feature = "parquet")]

use std::path::PathBuf;

use arrow_array::{Array, Int64Array, Int8Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

type Row = (
    Vec<i64>,
    i8,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<String>,
);

fn fixtures() -> Option<PathBuf> {
    let p = std::env::var_os("WXY_FIXTURES")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../fixtures")));
    p.join("mdimrefs/nc/198109").is_dir().then_some(p)
}

/// Rows of a refs table, paths cut to the file name.
fn rows(path: &std::path::Path, rank: usize) -> Vec<Row> {
    let file = std::fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::new();
    for b in reader {
        let b = b.unwrap();
        let col = |n: &str| b.column_by_name(n).unwrap().clone();
        let dims: Vec<Int64Array> = (0..rank)
            .map(|d| {
                col(&format!("dim_{d}"))
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone()
            })
            .collect();
        let present = col("present");
        let present = present.as_any().downcast_ref::<Int8Array>().unwrap();
        let p = col("path");
        let p = p.as_any().downcast_ref::<StringArray>().unwrap();
        let off = col("offset");
        let off = off.as_any().downcast_ref::<Int64Array>().unwrap();
        let size = col("size");
        let size = size.as_any().downcast_ref::<Int64Array>().unwrap();
        let info = col("info");
        let info = info.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            let opt_s = |a: &StringArray| (!a.is_null(i)).then(|| a.value(i).to_string());
            let opt_i = |a: &Int64Array| (!a.is_null(i)).then(|| a.value(i));
            out.push((
                dims.iter().map(|d| d.value(i)).collect(),
                present.value(i),
                opt_s(p).map(|s| s.rsplit('/').next().unwrap().to_string()),
                opt_i(off),
                opt_i(size),
                opt_s(info),
            ));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn oisst_fixture_rows() {
    let Some(fx) = fixtures() else {
        eprintln!("skipped: no wxylib/fixtures checkout (set WXY_FIXTURES)");
        return;
    };
    let dir = fx.join("mdimrefs/nc/198109");
    let mut names: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    names.sort();
    let mut files = Vec::new();
    for n in &names {
        files.push(
            refharvest::hdf5::harvest(n.to_str().unwrap(), &Default::default())
                .await
                .unwrap(),
        );
    }
    let table = refharvest::collate::concat(files, None).unwrap();
    assert_eq!(table.concat_dim.as_deref(), Some("time"));
    let out = std::env::temp_dir().join(format!("refharvest-fixture-{}", std::process::id()));
    refharvest::table::write(&out, &table).unwrap();

    for array in ["sst", "ice"] {
        let want = rows(&fx.join(format!("mdimrefs/refs_{array}.parquet")), 4);
        let got = rows(&out.join(format!("refs_{array}.parquet")), 4);
        assert_eq!(got, want, "{array}");
    }
    let ice = table.arrays.iter().find(|a| a.meta.name == "ice").unwrap();
    assert_eq!(ice.rows.iter().filter(|r| r.source.is_none()).count(), 1);
    let sst = &table
        .arrays
        .iter()
        .find(|a| a.meta.name == "sst")
        .unwrap()
        .meta;
    assert_eq!(sst.dims, ["time", "zlev", "lat", "lon"]);
    assert_eq!(sst.fill.as_deref(), Some("-999"));
    assert_eq!(sst.attrs["scale_factor"], serde_json::json!(0.01f32 as f64));
    std::fs::remove_dir_all(&out).ok();
}
