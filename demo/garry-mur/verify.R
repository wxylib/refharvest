suppressMessages({library(garry); library(gdalraster); library(jsonlite)})
summ <- fromJSON("fetch_summary.json", simplifyVector = FALSE)
gt0 <- c(-179.995, 0.01, 0, 89.995, 0, -0.01)   # MUR COG geotransform (checked below)
chk <- new(GDALRaster, paste0("/vsicurl/", summ[[1]]$href)); print(chk$getGeoTransform()); gt0 <- chk$getGeoTransform()
wkt <- chk$getProjection(); chk$close()

dir.create("localtif", showWarnings = FALSE)
files <- character(length(summ)); maxdiff <- 0
for (k in seq_along(summ)) {
  s <- summ[[k]]; ny <- s$shape[[1]]; nx <- s$shape[[2]]
  v <- readBin(s$file, "integer", n = nx * ny, size = 2, signed = TRUE)
  ## byte-for-byte against GDAL's own srcwin read of the same window
  g <- new(GDALRaster, file.path("gdalfetch", basename(s$href)))
  gx <- g$getRasterXSize(); gy <- g$getRasterYSize()
  gv <- g$read(1, 0, 0, gx, gy, gx, gy); g$close()
  m <- matrix(v, nx, ny)[1:gx, 1:gy]               # x fastest; GDAL clips at raster edge
  maxdiff <- max(maxdiff, max(abs(as.numeric(m) - gv)))
  f <- file.path("localtif", basename(s$href)); files[k] <- f
  create("GTiff", f, nx, ny, 1, "Int16", options = c("TILED=YES", "COMPRESS=ZSTD"))
  ds <- new(GDALRaster, f, read_only = FALSE)
  ds$setGeoTransform(c(gt0[1] + s$x0 * gt0[2], gt0[2], 0, gt0[4] + s$y0 * gt0[6], 0, gt0[6]))
  ds$setProjection(wkt); ds$setNoDataValue(1, -32768)
  ds$setScale(1, 0.001); ds$setOffset(1, 25)
  ds$write(1, 0, 0, nx, ny, v); ds$close()
}
cat("max |refs-decoded - GDAL srcwin| over 10 windows:", maxdiff, "\n")

## same garry pipeline over the refs-materialised local windows
g <- graph_new()
srcs <- lapply(files, lazy_source, graph = g, scale = TRUE)
to <- GridSpec(crs = srs_to_wkt("EPSG:3031"), transform = c(1.5e6, 2000, 0, 1e6, 0, -2000),
               extent = c(1.5e6, -1e6, 3.5e6, 1e6), dims = c(x = 1000L, y = 1000L), dtype = "f32")
out <- lapply(srcs, align_to, to = to, resampling = "bilinear") |> lazy_stack() |> reduce_over("mean", over = "t")
t0 <- Sys.time(); r <- collect(out); cat(sprintf("garry collect over local windows: %.2f s\n", as.numeric(Sys.time() - t0, units = "secs")))
d <- readRDS("garry_direct.rds")
cat("composite max |refs-route - direct-route|:", max(abs(r - d), na.rm = TRUE),
    " NaN patterns identical:", identical(is.nan(r), is.nan(d)), "\n")
cat("composite range (degC):", range(r, na.rm = TRUE), "\n")
