"""Planner windows x refs rows -> coalesced range GETs -> decoded local window GeoTIFFs.

Uses only the refs table plus per-collection constants (tile size, dtype,
codec, predictor, geotransform) -- no TIFF header is opened here.
"""
import asyncio, csv, json, sys, time, os
import numpy as np, pyarrow.parquet as pq, pyarrow.compute as pc, pyarrow as pa
import zstandard
from obstore.store import HTTPStore

BASE = "https://data.source.coop/ausantarctic/ghrsst-mur-v2"
TW = TH = 512
GAP = int(sys.argv[1]) if len(sys.argv) > 1 else 65536   # coalesce ranges closer than this
store = HTTPStore.from_url(BASE)
refs = pq.read_table("mur_refs.parquet")
wins = list(csv.DictReader(open("plan_windows.csv")))
os.makedirs("local", exist_ok=True)

def coalesce(off, ln):
    o = np.argsort(off); off, ln = off[o], ln[o]
    starts, ends = [off[0]], [off[0] + ln[0]]
    for a, l in zip(off[1:], ln[1:]):
        if a - ends[-1] <= GAP:
            ends[-1] = max(ends[-1], a + l)
        else:
            starts.append(a); ends.append(a + l)
    return starts, ends

def decode(buf):
    raw = zstandard.ZstdDecompressor().decompress(buf, max_output_size=TW * TH * 2)
    a = np.frombuffer(raw, "<i2").reshape(TH, TW)
    return np.cumsum(a, axis=1, dtype=np.int16)          # undo predictor 2 (wraps like int16)

async def one(w):
    href = w["href"]; tx0, tx1, ty0, ty1 = (int(w[k]) for k in ("tx0", "tx1", "ty0", "ty1"))
    m = pc.and_(pc.equal(refs["href"], href),
                pc.and_(pc.and_(pc.greater_equal(refs["tx"], tx0), pc.less_equal(refs["tx"], tx1)),
                        pc.and_(pc.greater_equal(refs["ty"], ty0), pc.less_equal(refs["ty"], ty1))))
    rows = refs.filter(m)
    off = rows["offset"].to_numpy(); ln = rows["length"].to_numpy()
    starts, ends = coalesce(off, ln)
    path = href[len(BASE) + 1:]
    bufs = await store.get_ranges_async(path, starts=starts, ends=ends)
    out = np.full(((ty1 - ty0 + 1) * TH, (tx1 - tx0 + 1) * TW), -32768, np.int16)
    for tx, ty, o, l in zip(rows["tx"].to_pylist(), rows["ty"].to_pylist(), off, ln):
        k = np.searchsorted(starts, o, side="right") - 1
        b = memoryview(bufs[k])[o - starts[k]: o - starts[k] + l]
        out[(ty - ty0) * TH:(ty - ty0 + 1) * TH, (tx - tx0) * TW:(tx - tx0 + 1) * TW] = decode(bytes(b))
    return dict(href=href, tiles=len(off), requests=len(starts),
                bytes=int(sum(e - s for s, e in zip(starts, ends))), tile_bytes=int(ln.sum()),
                x0=tx0 * TW, y0=ty0 * TH, arr=out)

async def main():
    t0 = time.time()
    res = await asyncio.gather(*[one(w) for w in wins])
    t1 = time.time()
    summ = []
    for r in res:
        fn = "local/" + os.path.basename(r["href"]).replace(".tif", ".i16")
        r["arr"].tofile(fn)
        summ.append({k: v for k, v in r.items() if k != "arr"} | {"file": fn, "shape": list(r["arr"].shape)})
    json.dump(summ, open("fetch_summary.json", "w"), indent=1)
    print(f"refs fetch+decode: {t1 - t0:.2f} s for {len(res)} windows")
    print("tiles:", sum(r["tiles"] for r in summ), " range requests:", sum(r["requests"] for r in summ),
          " MB fetched:", round(sum(r["bytes"] for r in summ) / 1e6, 1),
          " MB of tiles:", round(sum(r["tile_bytes"] for r in summ) / 1e6, 1))

asyncio.run(main())
