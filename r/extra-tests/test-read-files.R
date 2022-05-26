# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
test_that("Can see the metadata (parquet)", {
  skip_if_version_less_than("2.0.0", "Version 1.0.1 can't read new version metadata.")

  df <- read_parquet(pq_file)
  expect_s3_class(df, "tbl")

  # expect_mapequal() instead of expect_equal() because there was an order change where
  # `class` is located in version 3.0.0 and above.
  expect_mapequal(
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

    expect_mapequal(
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

test_that("Can read feather version 1", {
  feather_v1_file <- "files/ex_data_v1.feather"

  df <- read_feather(feather_v1_file)
  expect_s3_class(df, "tbl")

  expect_equal(
    attributes(df),
    list(
      names = c("b", "d"),
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

  expect_mapequal(
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
  expect_equal(
    attributes(df$c),
    list(
      row.names = 1L,
      names = c("c1", "c2", "c3"),
      class = c("tbl_df", "tbl", "data.frame")
    )
  )
})

test_that("Can see the extra metadata (parquet)", {
  pq_file <- "files/ex_data_extra_metadata.parquet"

  if (if_version_less_than("3.0.0")) {
    expect_warning(
      df <- read_parquet(pq_file),
      "Invalid metadata$r",
      fixed = TRUE
    )
    expect_s3_class(df, "tbl")
  } else {
    # version 3.0.0 and greater
    df <- read_parquet(pq_file)
    expect_s3_class(df, "tbl")

    expect_equal(
      attributes(df),
      list(
        names = letters[1:4],
        row.names = 1L,
        class = c("tbl_df", "tbl", "data.frame"),
        top_level = list(
          field_one = 12,
          field_two = "more stuff"
        )
      )
    )

    # column-level attributes for the large column.
    expect_named(attributes(df$b), "lots")
    expect_length(attributes(df$b)$lots, 100)
  }
})
