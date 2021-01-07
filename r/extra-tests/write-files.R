library(arrow)

if (!dir.exists("extra-tests/files")) {
  dir.create("extra-tests/files")
}

source("tests/testthat/helper-data.R")

write_parquet(example_with_metadata, "extra-tests/files/ex_data.parquet")
for (comp in c("lz4", "uncompressed", "zstd")) {
  if(!codec_is_available(comp)) break

  name <- paste0("extra-tests/files/ex_data.", comp, ".feather")
  write_feather(example_with_metadata, name, compression = comp)
}
