library(arrow)

if (!dir.exists("extra-tests/files")) {
  dir.create("extra-tests/files")
}

source("tests/testthat/helper-data.R")

write_parquet(example_with_metadata, "extra-tests/files/ex_data.parquet")
