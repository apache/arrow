library(arrow)
library(testthat)

pq_file <- "files/ex_data.parquet"

test_that("Can see the metadata", {
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

