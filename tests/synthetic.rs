//! Files written by hdf5-pure (a second, independent HDF5 implementation)
//! are harvested and checked against hdf5-pure's own chunk list. Its
//! writer emits HDF5 1.10 layouts (fixed and extensible array indexes);
//! the version 1 B-tree is covered by `libhdf5.rs`.

use hdf5_pure::{ChunkIndex, File, FileBuilder, LibVer};

fn write(path: &std::path::Path, ver: LibVer) {
    let (t, y, x) = (6u64, 40u64, 50u64);
    let data: Vec<f32> = (0..t * y * x).map(|i| (i as f32 * 0.37).sin()).collect();
    let mut b = FileBuilder::new();
    b.with_libver_bounds(ver, ver);
    b.create_dataset("v")
        .with_f32_data(&data)
        .with_shape(&[t, y, x])
        .with_chunks(&[1, 16, 16])
        .with_shuffle()
        .with_deflate(4);
    b.write(path).unwrap();
}

fn oracle(path: &std::path::Path, index: ChunkIndex) -> Vec<(Vec<u64>, u64, u64)> {
    let f = File::open(path).unwrap();
    let ds = f.dataset("v").unwrap();
    assert_eq!(ds.chunk_index().unwrap(), Some(index));
    let cs: Vec<u64> = ds.chunk_shape().unwrap().unwrap().to_vec();
    let mut v: Vec<(Vec<u64>, u64, u64)> = ds
        .chunks()
        .unwrap()
        .into_iter()
        .map(|c| {
            (
                c.offset.iter().zip(&cs).map(|(o, s)| o / s).collect(),
                c.address,
                c.storage_size,
            )
        })
        .collect();
    v.sort();
    v
}

async fn check(low: LibVer, index: ChunkIndex, tag: &str) {
    let dir =
        std::env::temp_dir().join(format!("refharvest-synthetic-{}-{tag}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("v.h5");
    write(&path, low);
    let want = oracle(&path, index);
    assert_eq!(want.len(), 6 * 3 * 4);

    for inflight in [1, 4, 32] {
        let opts = refharvest::Options {
            inflight,
            ..Default::default()
        };
        let f = refharvest::hdf5::harvest(path.to_str().unwrap(), &opts)
            .await
            .unwrap();
        assert!(f.refused.is_empty(), "{:?}", f.refused);
        let a = f.arrays.iter().find(|a| a.name == "v").unwrap();
        assert_eq!(a.shape, vec![6, 40, 50]);
        assert_eq!(a.chunks, vec![1, 16, 16]);
        assert_eq!(a.dtype, "<f4");
        assert_eq!(
            a.codec.info,
            "COMPRESSION=DEFLATE; FILTER=SHUFFLE; ENDIANNESS=LITTLE"
        );
        assert_eq!(a.codec.v2["compressor"]["level"], 4);
        let mut got: Vec<(Vec<u64>, u64, u64)> = a
            .refs
            .iter()
            .map(|c| (c.index.clone(), c.offset, c.size))
            .collect();
        got.sort();
        assert_eq!(got, want, "{tag}, {inflight} in flight");
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn hdf5_1_10_layout() {
    // A fixed array, read by async-hdf5's chunk_index().
    check(LibVer::V110, ChunkIndex::FixedArray, "v110").await;
}
