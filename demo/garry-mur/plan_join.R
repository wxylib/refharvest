## garry plan -> source tile windows -> rows of the refs table
suppressMessages({library(garry); library(gdalraster)})
refs <- arrow_read <- NULL
items <- read.csv("items.csv", stringsAsFactors = FALSE)   # href, datetime (10 days)

## 1. lazy pipeline: 10 MUR days -> 3031 grid off East Antarctica -> mean over t
srcs <- {
  g <- graph_new()
  lapply(paste0("/vsicurl/", items$href), lazy_source, graph = g, scale = TRUE)
}
src_grid <- srcs[[1]]@grid
to <- GridSpec(crs = srs_to_wkt("EPSG:3031"),
               transform = c(1.5e6, 2000, 0, 1e6, 0, -2000),
               extent = c(1.5e6, -1e6, 3.5e6, 1e6),
               dims = c(x = 1000L, y = 1000L), dtype = "f32")
out <- lapply(srcs, align_to, to = to, resampling = "bilinear") |>
  lazy_stack() |> reduce_over("mean", over = "t")
pl <- plan_lazy(out)
print(pl)

## 2. every warp stage's read chunks, as source pixel windows -> tile ranges
gt <- src_grid@transform
src_crs <- src_grid@crs
tw <- 512L
rows <- list()
for (s in pl@stages) {
  if (s@kind != "warp") next
  ck <- chunk_iter(s@chunks)
  for (i in seq_len(nrow(ck))) {
    w <- chunk_window_with_halo(s@chunks, ck$x_off[i], ck$y_off[i], ck$x_size[i], ck$y_size[i])
    tg <- s@grid@transform
    ext <- c(tg[1] + w$x_off * tg[2], tg[4] + (w$y_off + w$y_size) * tg[6],
             tg[1] + (w$x_off + w$x_size) * tg[2], tg[4] + w$y_off * tg[6])
    b <- transform_bounds(ext, s@grid@crs, src_crs, densify_pts = 21L)
    c0 <- floor((b[1] - gt[1]) / gt[2]) - 2; c1 <- ceiling((b[3] - gt[1]) / gt[2]) + 2
    r0 <- floor((b[4] - gt[4]) / gt[6]) - 2; r1 <- ceiling((b[2] - gt[4]) / gt[6]) + 2
    rows[[length(rows) + 1]] <- data.frame(
      stage = s@id, chunk = i, col0 = c0, col1 = c1, row0 = r0, row1 = r1,
      tx0 = c0 %/% tw, tx1 = c1 %/% tw, ty0 = r0 %/% tw, ty1 = r1 %/% tw)
  }
}
win <- do.call(rbind, rows)
win$href <- items$href[win$stage - min(win$stage) + 1]   # warp stage k <-> item k
print(head(win, 3))
cat("warp stages:", length(unique(win$stage)), " read chunks:", nrow(win), "\n")
write.csv(win, "plan_windows.csv", row.names = FALSE)

## 3. GDAL baseline: garry's own collect (direct warped remote reads)
garry_daemons(read = 2, compute = 1)
t0 <- Sys.time(); r <- collect(out); el <- as.numeric(Sys.time() - t0, units = "secs")
cat(sprintf("garry direct collect: %.1f s\n", el))
garry_daemons(0, 0)
saveRDS(r, "garry_direct.rds")
