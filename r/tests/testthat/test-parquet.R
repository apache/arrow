context("Parquet file reading/writing")

pq_file <- system.file("v0.7.1.parquet", package="arrow")

test_that("reading a known Parquet file to tibble", {
  df <- read_parquet(pq_file)
  expect_true(tibble::is_tibble(df))
  expect_identical(dim(df), c(10L, 11L))
  # TODO: assert more about the contents
})

test_that("as_tibble with and without threads", {
  expect_identical(
    read_parquet(pq_file),
    read_parquet(pq_file, use_threads = FALSE)
  )
})
