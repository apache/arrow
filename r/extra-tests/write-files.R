library(arrow)

if (!dir.exists("extra-tests/files")) {
  dir.create("extra-tests/files")
}

source("tests/testthat/helper-data.R")

write_parquet(example_with_metadata, "extra-tests/files/ex_data.parquet")

for (comp in c("lz4", "uncompressed", "zstd")) {
  if(!codec_is_available(comp)) break

  name <- paste0("extra-tests/files/ex_data_", comp, ".feather")
  write_feather(example_with_metadata, name, compression = comp)
}

example_with_metadata_v1 <- example_with_metadata
example_with_metadata_v1$c <- NULL
write_feather(example_with_metadata_v1, "extra-tests/files/ex_data_v1.feather", version = 1)

write_ipc_stream(example_with_metadata, "extra-tests/files/ex_data.stream")
