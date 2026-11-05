"""Harvest a byte-refs table (one row per tile) for N days of MUR analysed_sst COGs."""
import asyncio, sys, time
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
from async_tiff import TIFF
from obstore.store import HTTPStore

N = int(sys.argv[1]) if len(sys.argv) > 1 else 10
base = "https://data.source.coop/ausantarctic/ghrsst-mur-v2"
store = HTTPStore.from_url(base)

# the collection's STAC GeoParquet index (about 570 KB), read from source.coop
index = bytes(store.get("ghrsst-mur-v2.parquet").bytes())
items = pq.read_table(pa.py_buffer(index), columns=["id", "datetime", "assets", "proj:transform", "proj:shape"])
items = items.sort_by([("datetime", "descending")]).slice(0, N)
hrefs = [a["analysed_sst"]["href"] for a in items.column("assets").to_pylist()]
dts = items.column("datetime").to_pylist()

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
