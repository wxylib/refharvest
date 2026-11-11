"""Harvest a byte-refs table (one row per tile) for days of MUR analysed_sst COGs.

    python harvest.py      # the days in items.csv (the demo's 10 days)
    python harvest.py N    # the latest N days; rewrites items.csv
"""
import asyncio, csv, sys, time
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc, pyarrow.fs as pafs
from async_tiff import TIFF
from obstore.store import HTTPStore

N = int(sys.argv[1]) if len(sys.argv) > 1 else None
base = "https://data.source.coop/ausantarctic/ghrsst-mur-v2"
store = HTTPStore.from_url(base)

# the collection's STAC GeoParquet index, read anonymously from source.coop's
# S3 bucket (only the columns needed are fetched)
s3 = pafs.S3FileSystem(anonymous=True, region="us-west-2")
items = pq.read_table("us-west-2.opendata.source.coop/ausantarctic/ghrsst-mur-v2/ghrsst-mur-v2.parquet",
                      filesystem=s3, columns=["id", "datetime", "assets", "proj:transform", "proj:shape"])
items = items.sort_by("datetime")
allhref = [a["analysed_sst"]["href"] for a in items.column("assets").to_pylist()]
alldt = items.column("datetime").to_pylist()
if N is None:
    want = [r["href"] for r in csv.DictReader(open("items.csv"))]
    pos = {h: i for i, h in enumerate(allhref)}
    missing = [h for h in want if h not in pos]
    if missing:
        raise SystemExit(f"{len(missing)} items.csv hrefs not in the index, e.g. {missing[0]}")
    keep = [pos[h] for h in want]
else:
    keep = list(range(len(allhref)))[-N:]
    with open("items.csv", "w", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL); w.writerow(["href", "datetime"])
        for i in keep:
            w.writerow([allhref[i], alldt[i].isoformat()])
hrefs = [allhref[i] for i in keep]
dts = [alldt[i] for i in keep]

async def one(href, dt):
    path = href[len(base) + 1:]
    t = await TIFF.open(path, store=store)
    ifd = t.ifds[0]                              # full resolution
    tw, th = ifd.tile_width, ifd.tile_height
    ntx = -(-ifd.image_width // tw)
    off, cnt = list(ifd.tile_offsets), list(ifd.tile_byte_counts)
    rows = []
    for i, (o, c) in enumerate(zip(off, cnt)):
        rows.append((href, dt, i % ntx, i // ntx, o, c))
    meta = dict(width=ifd.image_width, height=ifd.image_height, tw=tw, th=th,
                compression=str(ifd.compression), predictor=str(ifd.predictor), noverviews=len(t.ifds) - 1)
    return rows, meta

async def main():
    t0 = time.time()
    res = await asyncio.gather(*[one(h, d) for h, d in zip(hrefs, dts)])
    rows = [r for rr, _ in res for r in rr]
    print("harvest seconds:", round(time.time() - t0, 2), "files:", len(res), "tiles:", len(rows))
    print("ifd0:", res[0][1])
    cols = list(zip(*rows))
    tab = pa.table({"href": cols[0], "datetime": pa.array(cols[1], pa.timestamp("ms", tz="UTC")),
                    "tx": pa.array(cols[2], pa.int32()), "ty": pa.array(cols[3], pa.int32()),
                    "offset": pa.array(cols[4], pa.int64()), "length": pa.array(cols[5], pa.int64())})
    pq.write_table(tab, "mur_refs.parquet")
    print(tab.slice(0, 3).to_pandas() if False else tab.slice(0, 3))
    print("total bytes referenced:", pc.sum(tab["length"]).as_py(), "empty tiles:", pc.sum(pc.equal(tab["length"], 0)).as_py())

asyncio.run(main())
