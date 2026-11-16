# Draft issue for belian-earth/garry (not posted)

Drafted 2026-11-05 to 11-11 and parked on 2026-11-11. Post it at
<https://github.com/belian-earth/garry/issues/new> when ready. The
numbers are from the first run (see the README's results); the rerun
on 2026-11-11 hit rewritten files, as the staleness note describes.

---

Title: One read stage across the time axis of an N-D source

Hi Hugh, a suggestion from some experiments with garry on MUR SST (10 days of COGs on source.coop, warped to a 2 km EPSG:3031 grid, mean over time).

**What I saw.** garry takes one source with a time axis happily: a classic VRT with `DIM_time_INDEX` / `DIM_time_VALUE` band metadata through 
`lazy_dataset()` gives exactly the same composite as ten separate `lazy_source()`s. But the plan still expands to one read and one warp per 
time step, 21 stages either way, so it isn't faster.

**The suggestion.** When a source's time slices share a grid (the usual case for a daily series), plan one read stage per spatial chunk covering 
every time step, then warp and reduce. Each read stage reads the same source window from every slice.

**Why it pays off.** I joined garry's plan windows to a byte-refs table (one row per COG tile: href, datetime, tx, ty, offset, length). With that 
table, "this window, every day" is one filter on (tx, ty) across all files. For 10 days it came to 700 tiles in 40 coalesced range requests, fetched 
and decoded in 1.2–2.3 s against 3.9 s for tuned GDAL `-srcwin` reads and 24 s for garry's direct warped reads. The result was byte-identical, and so 
was the composite. Scripts and the timing table: https://github.com/wxylib/refharvest/tree/main/demo/garry-mur (PR wxylib/refharvest#6).

Separately, rwarp's `warp_plan()` reproduced garry's read windows (to within the 2-pixel margin) in 10 ms, which might interest you for the planning step.

Three side notes:
- A refs table goes stale when a file is replaced, and the rows alone can't show it. MUR rewrites recent days: a week after the first harvest, 8 of the 10 files had new tile offsets and lengths. So a refs route needs each file's ETag or size recorded and checked before fetching. That's on the refs side, not garry's, but it matters if garry ever reads from refs.
- GDAL 3.13 has no classic view of a multidimensional VRT (unlike Zarr), which is why I used a classic VRT with `DIM_*` metadata. That one's for GDAL, not garry.
- These are single runs on 2 cores, so the ratios are indicative. A full-year run on Setonix or Gadi is next.

Happy to help prototype it if it fits garry's design.
