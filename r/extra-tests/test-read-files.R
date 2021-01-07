library(arrow)
library(testthat)

if_version <- function(version, op = `==`) {
  op(numeric_version(Sys.getenv("OLD_ARROW_VERSION", "0.0.0")), version)
}

skip_if_version_less_than <- function(version, msg) {
  if(if_version(version, `<`)) {
    skip(msg)
  }
}

skip_if_version_equals <- function(version, msg) {
  if(if_version(version, `==`)) {
    skip(msg)
  }
}

pq_file <- "files/ex_data.parquet"

test_that("Can read the file (parquet)", {
  # We can read with no error, we assert metadata below
  expect_error(
    df <- read_parquet(pq_file),
    NA
  )
})

test_that("Can see the metadata (feather)", {
  skip_if_version_less_than("2.0.0", "Version 1.0.1 can't read new version metadata.")

  df <- read_parquet(pq_file)
  expect_s3_class(df, "tbl")

  expect_equal(
    attributes(df),
    list(
      names = letters[1:4],
      row.names = 1L,
      top_level = list(
        field_one = 12,
        field_two = "more stuff"
      ),
      class = c("tbl_df", "tbl", "data.frame")
    )
  )

  # column-level attributes
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



for (comp in c("lz4", "uncompressed", "zstd")) {
  feather_file <- paste0("files/ex_data.", comp, ".feather")

  test_that(paste0("Can see the metadata (feather", comp, ")"), {
    df <- read_feather(feather_file)
    expect_s3_class(df, "tbl")

    expect_equal(
      attributes(df),
      list(
        names = letters[1:4],
        row.names = 1L,
        top_level = list(
          field_one = 12,
          field_two = "more stuff"
        ),
        class = c("tbl_df", "tbl", "data.frame")
      )
    )

    # column-level attributes
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
}


