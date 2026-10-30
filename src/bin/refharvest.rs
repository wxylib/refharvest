//! `refharvest hdf5 <source>... --out <dir>`: harvest HDF5 / netCDF-4
//! files, stack them along their first dimension and write an mdim-refs
//! table.

use std::process::ExitCode;

const USAGE: &str =
    "usage: refharvest hdf5 <source>... --out <dir> [--concat-dim <name>] [--inflight <n>]

  <source>  a path, file:// URL, http(s):// URL (or s3:// with the s3 feature),
            or @list.txt with one source per line
  --out     directory for arrays.json, refs_<array>.parquet, sources.parquet
  --concat-dim  dimension to stack files along (default: the first dimension
            of the first data array)
  --inflight    index requests in flight per file (default 32)
  --files       files harvested at once (default 8)";

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) != Some("hdf5") {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }
    let mut sources = Vec::new();
    let mut out = None;
    let mut concat_dim = None;
    let mut opts = refharvest::Options::default();
    let mut at_once = 8usize;
    let mut it = args[1..].iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--out" => out = it.next().cloned(),
            "--concat-dim" => concat_dim = it.next().cloned(),
            "--inflight" => match it.next().and_then(|v| v.parse().ok()) {
                Some(n) => opts.inflight = n,
                None => {
                    eprintln!("--inflight takes a number\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--files" => match it.next().and_then(|v| v.parse().ok()) {
                Some(n) => at_once = n,
                None => {
                    eprintln!("--files takes a number\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            s if s.starts_with('@') => match std::fs::read_to_string(&s[1..]) {
                Ok(text) => sources.extend(
                    text.lines()
                        .map(str::trim)
                        .filter(|l| !l.is_empty())
                        .map(String::from),
                ),
                Err(e) => {
                    eprintln!("{}: {e}", &s[1..]);
                    return ExitCode::FAILURE;
                }
            },
            s => sources.push(s.to_string()),
        }
    }
    let (Some(out), false) = (out, sources.is_empty()) else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };

    use futures::StreamExt;
    let harvested: Vec<(String, refharvest::Result<refharvest::FileRefs>)> =
        futures::stream::iter(sources.iter().cloned())
            .map(|s| {
                let opts = opts.clone();
                async move {
                    let r = refharvest::hdf5::harvest(&s, &opts).await;
                    (s, r)
                }
            })
            .buffered(at_once.max(1))
            .collect()
            .await;
    let mut files = Vec::new();
    for (s, r) in harvested {
        match r {
            Ok(f) => {
                let chunks: usize = f.arrays.iter().map(|a| a.refs.len()).sum();
                eprintln!(
                    "{s}: {} arrays, {chunks} chunks, {} refused; {} requests, {} bytes, {:.1} s",
                    f.arrays.len(),
                    f.refused.len(),
                    f.cost.requests,
                    f.cost.bytes,
                    f.cost.seconds
                );
                for r in &f.refused {
                    eprintln!("  refused {}: {}", r.array, r.reason);
                }
                files.push(f);
            }
            Err(e) => {
                eprintln!("{s}: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    let table = match refharvest::collate::concat(files, concat_dim.as_deref()) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = refharvest::table::write(&out, &table) {
        eprintln!("{e}");
        return ExitCode::FAILURE;
    }
    eprintln!("wrote {} arrays to {out}", table.arrays.len());
    ExitCode::SUCCESS
}
