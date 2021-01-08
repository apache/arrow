library(arrow)
library(testthat)

pq_file <- "files/ex_data.parquet"

test_that("Can read the file (parquet)", {
  # We can read with no error, we assert metadata below
  expect_error(
    df <- read_parquet(pq_file),
    NA
  )
})

### Parquet
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

### Feather
for (comp in c("lz4", "uncompressed", "zstd")) {
  feather_file <- paste0("files/ex_data_", comp, ".feather")

  test_that(paste0("Can read the file (feather ", comp, ")"), {
    # We can read with no error, we assert metadata below
    expect_error(
      df <- read_feather(feather_file),
      NA
    )
  })

  test_that(paste0("Can see the metadata (feather ", comp, ")"), {
    skip_if_version_less_than("2.0.0", "Version 1.0.1 can't read new version metadata.")

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

test_that(paste0("Can read feather version 1"), {
  feather_v1_file <- "files/ex_data_v1.feather"

  df <- read_feather(feather_v1_file)
  expect_s3_class(df, "tbl")

  expect_equal(
    attributes(df),
    list(
      names = c("a", "b", "d"),
      class = c("tbl_df", "tbl", "data.frame"),
      row.names = 1L
    )
  )
})

### IPC Stream
stream_file <- "files/ex_data.stream"

test_that("Can read the file (parquet)", {
  # We can read with no error, we assert metadata below
  expect_error(
    df <- read_ipc_stream(stream_file),
    NA
  )
})

test_that("Can see the metadata (stream)", {
  skip_if_version_less_than("2.0.0", "Version 1.0.1 can't read new version metadata.")
  df <- read_ipc_stream(stream_file)

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


