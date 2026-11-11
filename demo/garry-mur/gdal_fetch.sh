#!/bin/sh
# GDAL's own read of each refs_fetch.py window, for verify.R: one
# gdal_translate -srcwin per day, in parallel, into gdalfetch/.
# Needs fetch_summary.json from refs_fetch.py. Tuned as in the README.
set -e
export GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR
export GDAL_HTTP_MULTIPLEX=YES GDAL_HTTP_VERSION=2
export GDAL_NUM_THREADS=ALL_CPUS
mkdir -p gdalfetch
python3 -c '
import json
for s in json.load(open("fetch_summary.json")):
    ny, nx = s["shape"]
    print(s["href"], s["x0"], s["y0"], nx, ny)
' | xargs -P 10 -n 5 sh -c '
  gdal_translate -q -srcwin "$2" "$3" "$4" "$5" \
    -co TILED=YES -co COMPRESS=ZSTD \
    "/vsicurl/$1" "gdalfetch/$(basename "$1")"
' sh
