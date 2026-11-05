suppressMessages({library(garry); library(gdalraster)})
Sys.setenv(GDAL_DISABLE_READDIR_ON_OPEN = "EMPTY_DIR")
ds <- lazy_dataset("mur10_classic.vrt", scale = TRUE)
print(ds)
to <- GridSpec(crs = srs_to_wkt("EPSG:3031"), transform = c(1.5e6, 2000, 0, 1e6, 0, -2000),
               extent = c(1.5e6, -1e6, 3.5e6, 1e6), dims = c(x = 1000L, y = 1000L), dtype = "f32")
## a dataset takes its target grid at construction, so rebuild pinned to `to`
ds2 <- lazy_dataset("mur10_classic.vrt", grid = to, scale = TRUE, resampling = "bilinear")
comp <- reduce_over(ds2, "mean", over = "t")
print(comp)
x <- if (S7::S7_inherits(comp, LazyRaster)) comp else comp[["b1"]]
pl <- plan_lazy(x); print(pl)
garry_daemons(read = 2, compute = 1)
t0 <- Sys.time(); r <- collect(x); el <- as.numeric(Sys.time() - t0, units = "secs")
garry_daemons(0, 0)
cat(sprintf("one-source collect: %.1f s\n", el))
d <- readRDS("garry_direct.rds"); r <- drop(r)
cat("vs 10-source route: max |diff|", max(abs(r - d), na.rm = TRUE),
    " NaN identical:", identical(is.nan(r), is.nan(d)), "\n")
