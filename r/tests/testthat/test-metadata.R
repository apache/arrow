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

test_that("Schema metadata", {
  s <- schema(b = double())
  expect_equal(s$metadata, empty_named_list())
  expect_false(s$HasMetadata)
  s$metadata <- list(test = TRUE)
  expect_identical(s$metadata, list(test = "TRUE"))
  expect_true(s$HasMetadata)
  s$metadata$foo <- 42
  expect_identical(s$metadata, list(test = "TRUE", foo = "42"))
  expect_true(s$HasMetadata)
  s$metadata$foo <- NULL
  expect_identical(s$metadata, list(test = "TRUE"))
  expect_true(s$HasMetadata)
  s$metadata <- NULL
  expect_equal(s$metadata, empty_named_list())
  expect_false(s$HasMetadata)
  expect_error(
    s$metadata <- 4,
    "Key-value metadata must be a named list or character vector"
  )
})

test_that("Table metadata", {
  tab <- Table$create(x = 1:2, y = c("a", "b"))
  expect_equal(tab$metadata, empty_named_list())
  tab$metadata <- list(test = TRUE)
  expect_identical(tab$metadata, list(test = "TRUE"))
  tab$metadata$foo <- 42
  expect_identical(tab$metadata, list(test = "TRUE", foo = "42"))
  tab$metadata$foo <- NULL
  expect_identical(tab$metadata, list(test = "TRUE"))
  tab$metadata <- NULL
  expect_equal(tab$metadata, empty_named_list())
})

test_that("Table R metadata", {
  tab <- Table$create(example_with_metadata)
  expect_output(print(tab$metadata), "arrow_r_metadata")
  expect_identical(as.data.frame(tab), example_with_metadata)
})

test_that("R metadata is not stored for types that map to Arrow types (factor, Date, etc.)", {
  tab <- Table$create(example_data[1:6])
  expect_null(tab$metadata$r)

  expect_null(Table$create(example_with_times[1:3])$metadata$r)
})

test_that("classes are not stored for arrow_binary/arrow_large_binary/arrow_fixed_size_binary (ARROW-14140)", {
  raws <- charToRaw("bonjour")

  binary <- Array$create(list(raws), binary())
  large_binary <- Array$create(list(raws), large_binary())
  fixed_size_binary <- Array$create(list(raws), fixed_size_binary(7L))

  expect_null(RecordBatch$create(b = binary)$metadata$r)
  expect_null(RecordBatch$create(b = large_binary)$metadata$r)
  expect_null(RecordBatch$create(b = fixed_size_binary)$metadata$r)

  expect_null(Table$create(b = binary)$metadata$r)
  expect_null(Table$create(b = large_binary)$metadata$r)
  expect_null(Table$create(b = fixed_size_binary)$metadata$r)
})

test_that("Garbage R metadata doesn't break things", {
  tab <- Table$create(example_data[1:6])
  tab$metadata$r <- "garbage"
  expect_warning(
    expect_identical(as.data.frame(tab), example_data[1:6]),
    "Invalid metadata$r",
    fixed = TRUE
  )
  # serialize data like .serialize_arrow_r_metadata does, but don't call that
  # directly since it checks to ensure that the data is a list
  tab$metadata$r <- rawToChar(serialize("garbage", NULL, ascii = TRUE))
  expect_warning(
    expect_identical(as.data.frame(tab), example_data[1:6]),
    "Invalid metadata$r",
    fixed = TRUE
  )
})

test_that("Metadata serialization compression", {
  # attributes that (when serialized) are just under 100kb are not compressed,
  # and simply serialized
  strings <- as.list(rep(make_string_of_size(1), 98))
  small <- .serialize_arrow_r_metadata(strings)
  expect_equal(
    object.size(small),
    object.size(rawToChar(serialize(strings, NULL, ascii = TRUE)))
  )

  # Large strings will be compressed
  large_strings <- as.list(rep(make_string_of_size(1), 100))
  large <- .serialize_arrow_r_metadata(large_strings)
  expect_lt(
    object.size(large),
    object.size(rawToChar(serialize(large_strings, NULL, ascii = TRUE)))
  )
  # and this compression ends up being smaller than even the "small" strings
  expect_lt(object.size(large), object.size(small))

  # However strings where compression + serialization is not effective are no
  # worse than only serialization alone
  large_few_strings <- as.list(rep(make_random_string_of_size(50), 2))
  large_few <- .serialize_arrow_r_metadata(large_few_strings)
  expect_equal(
    object.size(large_few),
    object.size(rawToChar(serialize(large_few_strings, NULL, ascii = TRUE)))
  )

  # But we can disable compression
  op <- options(arrow.compress_metadata = FALSE)
  on.exit(options(op))

  large_strings <- as.list(rep(make_string_of_size(1), 100))
  large <- .serialize_arrow_r_metadata(large_strings)
  expect_equal(
    object.size(large),
    object.size(rawToChar(serialize(large_strings, NULL, ascii = TRUE)))
  )
})

test_that("RecordBatch metadata", {
  rb <- RecordBatch$create(x = 1:2, y = c("a", "b"))
  expect_equal(rb$metadata, empty_named_list())
  rb$metadata <- list(test = TRUE)
  expect_identical(rb$metadata, list(test = "TRUE"))
  rb$metadata$foo <- 42
  expect_identical(rb$metadata, list(test = "TRUE", foo = "42"))
  rb$metadata$foo <- NULL
  expect_identical(rb$metadata, list(test = "TRUE"))
  rb$metadata <- NULL
  expect_equal(rb$metadata, empty_named_list())
})

test_that("RecordBatch R metadata", {
  expect_identical(as.data.frame(record_batch(example_with_metadata)), example_with_metadata)
})

test_that("R metadata roundtrip via parquet", {
  skip_if_not_available("parquet")
  tf <- tempfile()
  on.exit(unlink(tf))

  write_parquet(example_with_metadata, tf)
  expect_identical(read_parquet(tf), example_with_metadata)
})

test_that("R metadata roundtrip via feather", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write_feather(example_with_metadata, tf)
  expect_identical(read_feather(tf), example_with_metadata)
})

test_that("haven types roundtrip via feather", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write_feather(haven_data, tf)
  expect_identical(read_feather(tf), haven_data)
})

test_that("Date/time type roundtrip", {
  rb <- record_batch(example_with_times)
  expect_r6_class(rb$schema$posixlt$type, "StructType")
  expect_identical(as.data.frame(rb), example_with_times)
})

test_that("metadata keeps attribute of top level data frame", {
  df <- structure(data.frame(x = 1, y = 2), foo = "bar")
  tab <- Table$create(df)
  expect_identical(attr(as.data.frame(tab), "foo"), "bar")
  expect_identical(as.data.frame(tab), df)
})


test_that("metadata drops readr's problems attribute", {
  readr_like <- tibble::tibble(
    dbl = 1.1,
    not_here = NA_character_
  )
  attributes(readr_like) <- append(
    attributes(readr_like),
    list(problems = tibble::tibble(
      row = 1L,
      col = NA_character_,
      expected = "2 columns",
      actual = "1 columns",
      file = "'test'"
    ))
  )

  tab <- Table$create(readr_like)
  expect_null(attr(as.data.frame(tab), "problems"))
})

test_that("metadata of list elements (ARROW-10386)", {
  df <- data.frame(x = I(list(structure(1, foo = "bar"), structure(2, baz = "qux"))))
  tab <- Table$create(df)
  expect_identical(attr(as.data.frame(tab)$x[[1]], "foo"), "bar")
  expect_identical(attr(as.data.frame(tab)$x[[2]], "baz"), "qux")
})


test_that("metadata of list elements (ARROW-10386)", {
  skip_if_not_available("dataset")
  skip_if_not_available("parquet")

  local_edition(3)
  library(dplyr)

  df <- tibble::tibble(
    metadata = list(
      structure(1, my_value_as_attr = 1),
      structure(2, my_value_as_attr = 2),
      structure(3, my_value_as_attr = 3),
      structure(4, my_value_as_attr = 3)
    ),
    int = 1L:4L,
    part = c(1, 3, 2, 1)
  )

  dst_dir <- make_temp_dir()
  expect_warning(
    write_dataset(df, dst_dir, partitioning = "part"),
    "Row-level metadata is not compatible with datasets and will be discarded"
  )

  # Reset directory as previous write will have created some files and the default
  # behavior is to error on existing
  dst_dir <- make_temp_dir()
  # but we need to write a dataset with row-level metadata to make sure when
  # reading ones that have been written with them we warn appropriately
  fake_func_name <- write_dataset
  fake_func_name(df, dst_dir, partitioning = "part")

  ds <- open_dataset(dst_dir)
  expect_warning(
    df_from_ds <- collect(ds),
    "Row-level metadata is not compatible with this operation and has been ignored"
  )
  expect_equal(
    arrange(df_from_ds, int),
    arrange(df, int),
    ignore_attr = TRUE
  )

  # however there is *no* warning if we don't select the metadata column
  expect_warning(
    df_from_ds <- ds %>% select(int) %>% collect(),
    NA
  )
})

test_that("dplyr with metadata", {
  skip_if_not_available("dataset")

  expect_dplyr_equal(
    input %>%
      collect(),
    example_with_metadata
  )
  expect_dplyr_equal(
    input %>%
      select(a) %>%
      collect(),
    example_with_metadata
  )
  expect_dplyr_equal(
    input %>%
      mutate(z = b * 4) %>%
      select(z, a) %>%
      collect(),
    example_with_metadata
  )
  expect_dplyr_equal(
    input %>%
      mutate(z = nchar(a)) %>%
      select(z, a) %>%
      collect(),
    example_with_metadata
  )
  # dplyr drops top-level attributes if you do summarize, though attributes
  # of grouping columns appear to come through
  expect_dplyr_equal(
    input %>%
      group_by(a) %>%
      summarize(n()) %>%
      collect(),
    example_with_metadata
  )
  # Same name in output but different data, so the column metadata shouldn't
  # carry through
  expect_dplyr_equal(
    input %>%
      mutate(a = nchar(a)) %>%
      select(a) %>%
      collect(),
    example_with_metadata
  )
})
