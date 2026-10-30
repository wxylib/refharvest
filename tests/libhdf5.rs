//! A file written by libhdf5, with libhdf5's own chunk list as the
//! oracle (`tests/data/make_btree_v1.R`: rhdf5, `H5Dchunk_iter`). Its
//! 1,400 chunks sit in a two-level version 1 B-tree, which refharvest
//! walks a level at a time.

fn oracle() -> Vec<(Vec<u64>, u64, u64)> {
    let text = include_str!("data/btree_v1.chunks.csv");
    let mut v: Vec<(Vec<u64>, u64, u64)> = text
        .lines()
        .skip(1)
        .map(|l| {
            let f: Vec<u64> = l.split(',').map(|x| x.parse().unwrap()).collect();
            assert_eq!(f[5], 0, "filter mask");
            (f[0..3].to_vec(), f[3], f[4])
        })
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn btree_v1_matches_libhdf5() {
    let want = oracle();
    assert_eq!(want.len(), 1400);
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/btree_v1.h5");
    for inflight in [1, 3, 32] {
        let opts = refharvest::Options {
            inflight,
            ..Default::default()
        };
        let f = refharvest::hdf5::harvest(path, &opts).await.unwrap();
        let a = f.arrays.iter().find(|a| a.name == "v").unwrap();
        assert_eq!(a.shape, vec![7, 90, 200]);
        assert_eq!(a.chunks, vec![1, 9, 10]);
        assert_eq!(a.dtype, "<i4");
        assert_eq!(a.fill, None);
        let mut got: Vec<(Vec<u64>, u64, u64)> = a
            .refs
            .iter()
            .map(|c| (c.index.clone(), c.offset, c.size))
            .collect();
        got.sort();
        assert_eq!(got, want, "{inflight} in flight");
        // One request per tree node plus the metadata blocks, however many
        // are in flight.
        assert!(f.cost.requests < 40, "{} requests", f.cost.requests);
    }
}
