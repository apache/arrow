context("Parquet file reading/writing")

test_that("reading a known Parquet file to tibble", {
  df <- read_parquet(test_path("files/v0.7.1.parquet"))
  expect_true(is_tibble(df))
  expect_identical(dim(df), c(10L, 11L))
  # TODO: assert more about the contents
})

test_that("as_tibble with and without threads", {
  expect_identical(
    read_parquet(test_path("files/v0.7.1.parquet")),
    read_parquet(test_path("files/v0.7.1.parquet"), use_threads = FALSE)
  )
})
