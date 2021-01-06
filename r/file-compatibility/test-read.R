library(arrow)
library(testthat)

skip_if_version <- function(version, msg, op = `<=`) {
  if (op(numeric_version(Sys.getenv("OLD_ARROW_VERSION", "0.0.0")), version)) {
    skip(msg)
  }
}

pq_file <- "files/ex_data.parquet"

test_that("Can sread the file", {
  df <- read_parquet(pq_file)
  print(str(df))
  expect_true(TRUE)
})

test_that("Can see the metadata", {
  skip_if_version("1.0.1", "Version 1.0.1 can't read new version metadata.")

  df <- read_parquet(pq_file)
  expect_s3_class(df, "tbl")
  expect_equal(attributes(df$a), list(class = "special_string"))
  expect_equal(
    attributes(df$c),
    list(
      row.names = 1L,
      names = c("c1", "c2", "c3"),
      class = c("tbl_df", "tbl", "data.frame")
    )
  )
})

