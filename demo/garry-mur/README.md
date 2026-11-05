# Demo: a planner's windows joined to tile refs (MUR SST, garry)

A prototype of what a TIFF/COG stage of refharvest would give a planner.
One row per COG tile (`href, datetime, tx, ty, offset, length`) is harvested
from 10 days of MUR `analysed_sst` COGs on source.coop. The read windows of a
[garry](https://github.com/belian-earth/garry) plan are then joined to those
rows, and the tiles fetched as coalesced range requests and decoded from the
rows alone, with no TIFF opened.

The harvest here is ad hoc Python (async-tiff + obstore). In refharvest
terms, it is the COG analogue of stage 1's HDF5 harvest: the tile grid is
the chunk grid, and a daily series collated on time is one array. See the
refharvest issue for the stage this motivates.

## Run order

1. `harvest.py N`: read the full-resolution IFD of the latest N MUR
   `analysed_sst` COGs and write `mur_refs.parquet`, one row per tile:
   href, datetime, tx, ty, offset, length. The latest N dates come from
   the collection's STAC GeoParquet index, read from
   `s3://us-west-2.opendata.source.coop/ausantarctic/ghrsst-mur-v2/ghrsst-mur-v2.parquet`
   (anonymous; also at <https://data.source.coop/ausantarctic/ghrsst-mur-v2/ghrsst-mur-v2.parquet>).
2. `plan_join.R`: build a garry plan (10 days, warped to a 2 km EPSG:3031
   grid off East Antarctica, mean over time), walk every warp stage's read
   chunks, transform each to source pixel windows and tile ranges
   (`plan_windows.csv`), and time garry's own direct collect.
3. `refs_fetch.py [GAP]`: join the windows to refs rows, coalesce ranges
   (default gap 64 KiB), fetch with obstore `get_ranges`, decode ZSTD +
   predictor 2 from the rows alone.
4. `verify.R`: write the decoded windows as local GeoTIFFs, rerun the same
   garry pipeline on them, and compare with the direct result. It also
   compares each window with GDAL's own read of it in `gdalfetch/` (made
   by parallel `gdal_translate -srcwin`; that script isn't included).
5. `one_source.R` + `mur10_classic.vrt`: the same 10 days as one classic
   VRT with `DIM_time_INDEX` / `DIM_time_VALUE` band metadata, read with
   garry's `lazy_dataset()`. `mur10.vrt` is the equivalent multidimensional
   VRT; GDAL 3.13 opens it in multidim mode but gives no classic view of
   it (unlike Zarr), hence the classic stand-in.

`items.csv` (the 10 COGs) and `plan_windows.csv` (the plan's windows) are
included so steps 3 and 4 can run without re-planning. `mur_refs.parquet`
(322 KB) is not: step 1 regenerates it in about a second.

## Results

2 cores, a sandbox network link, 10 days, 700 tiles of 512 x 512. Single
runs, so treat the ratios as indicative.

| route | wall |
|---|---|
| harvest refs (10 headers) | 0.9 s |
| garry direct warped remote reads (2 readers) | 24.1 s |
| parallel `gdal_translate -srcwin`, untuned | 11.2 s |
| parallel `gdal_translate -srcwin`, tuned | 3.9 s |
| refs rows, 40 coalesced GETs, decode | 1.2-2.3 s |
| garry collect over the refs-decoded windows | 2.9 s |

Tuned GDAL: `GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR`,
`GDAL_HTTP_MULTIPLEX=YES`, `GDAL_HTTP_VERSION=2`,
`GDAL_NUM_THREADS=ALL_CPUS`. That is the fair comparison.

- 700 tiles coalesce into 40 range requests: 19.2 MB fetched for 18.8 MB
  of tiles.
- The refs-decoded windows are byte-identical to GDAL's `-srcwin` reads.
- The final composite is identical by both routes (max difference 0, same
  NaN pattern).
- One N-D source gives the identical result, but garry's plan is still 21
  stages (one read and one warp per time step). A read stage covering every
  time step would be one refs filter on (tx, ty) across all files.

## Related

- rwarp reproduced the same composite with its own planner (`warp_plan()`,
  10 ms, windows within garry's 2-pixel margin) and kernel; follow-ups are
  on the hypertidy roadmap board
  (<https://github.com/orgs/hypertidy/projects/1>).
- The missing classic view of multidim VRTs is on rwarp's list of GDAL
  findings (hypertidy/rwarp#34).
