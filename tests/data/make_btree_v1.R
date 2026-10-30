# Writes tests/data/btree_v1.h5 with libhdf5 (through rhdf5) and its chunk
# list from H5Dchunk_iter (rhdf5 devel h5getAllChunkInfo) as
# btree_v1.chunks.csv. libhdf5's default chunk index is a version 1
# B-tree; 1,400 chunks need two levels. The file is written one time
# step at a time, as model output is.
#
#   Rscript make_btree_v1.R <dir>
suppressPackageStartupMessages(library(rhdf5))
dir <- commandArgs(trailingOnly = TRUE)[1]
f <- file.path(dir, "btree_v1.h5")
unlink(f)
h5createFile(f)
# R dims are fastest first: x = 200, y = 90, t = 7 (HDF5: 7 x 90 x 200).
h5createDataset(f, "v", dims = c(200, 90, 7), chunk = c(10, 9, 1), level = 4,
                storage.mode = "integer", fillValue = -1L)
for (t in 1:7) {
  z <- outer(1:200, 1:90, function(x, y) as.integer(1000 * sin(x / 17 + t) * cos(y / 11)))
  h5write(array(z, c(200, 90, 1)), f, "v", index = list(NULL, NULL, t))
}
fid <- H5Fopen(f, flags = "H5F_ACC_RDONLY")
did <- H5Dopen(fid, "v")
cdim <- rev(H5Dchunk_dims(did))
ci <- h5getAllChunkInfo(did)
idx <- sweep(ci$offset, 2, cdim, "%/%")
out <- data.frame(i = idx[, 1], j = idx[, 2], k = idx[, 3],
                  offset = format(ci$addr, scientific = FALSE, trim = TRUE),
                  size = format(ci$size, scientific = FALSE, trim = TRUE),
                  mask = ci$filter_mask)
write.csv(out, file.path(dir, "btree_v1.chunks.csv"), row.names = FALSE, quote = FALSE)
H5Dclose(did); H5Fclose(fid)
