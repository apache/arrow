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

skip_if_not_available("json")

test_that("Can read json file with scalars columns (ARROW-5503)", {
  tf <- tempfile()
  on.exit(unlink(tf))
  writeLines('
    { "hello": 3.5, "world": false, "yo": "thing" }
    { "hello": 3.25, "world": null }
    { "hello": 3.125, "world": null, "yo": "\u5fcd" }
    { "hello": 0.0, "world": true, "yo": null }
  ', tf, useBytes = TRUE)

  tab1 <- read_json_arrow(tf, as_data_frame = FALSE)
  tab2 <- read_json_arrow(mmap_open(tf), as_data_frame = FALSE)
  tab3 <- read_json_arrow(ReadableFile$create(tf), as_data_frame = FALSE)

  expect_equal(tab1, tab2)
  expect_equal(tab1, tab3)

  expect_equal(
    tab1$schema,
    schema(hello = float64(), world = boolean(), yo = utf8())
  )
  tib <- as.data.frame(tab1)
  expect_equal(tib$hello, c(3.5, 3.25, 3.125, 0))
  expect_equal(tib$world, c(FALSE, NA, NA, TRUE))
  expect_equal(tib$yo, c("thing", NA, "\u5fcd", NA))
})

test_that("read_json_arrow() converts to tibble", {
  tf <- tempfile()
  on.exit(unlink(tf))
  writeLines('
    { "hello": 3.5, "world": false, "yo": "thing" }
    { "hello": 3.25, "world": null }
    { "hello": 3.125, "world": null, "yo": "\u5fcd" }
    { "hello": 0.0, "world": true, "yo": null }
  ', tf, useBytes = TRUE)

  tab1 <- read_json_arrow(tf)
  tab2 <- read_json_arrow(mmap_open(tf))
  tab3 <- read_json_arrow(ReadableFile$create(tf))

  expect_s3_class(tab1, "tbl_df")
  expect_s3_class(tab2, "tbl_df")
  expect_s3_class(tab3, "tbl_df")

  expect_equal(tab1, tab2)
  expect_equal(tab1, tab3)

  expect_equal(tab1$hello, c(3.5, 3.25, 3.125, 0))
  expect_equal(tab1$world, c(FALSE, NA, NA, TRUE))
  expect_equal(tab1$yo, c("thing", NA, "\u5fcd", NA))
})

test_that("read_json_arrow() supports col_select=", {
  tf <- tempfile()
  writeLines('
    { "hello": 3.5, "world": false, "yo": "thing" }
    { "hello": 3.25, "world": null }
    { "hello": 3.125, "world": null, "yo": "\u5fcd" }
    { "hello": 0.0, "world": true, "yo": null }
  ', tf)

  tab1 <- read_json_arrow(tf, col_select = c(hello, world))
  expect_equal(names(tab1), c("hello", "world"))

  tab2 <- read_json_arrow(tf, col_select = 1:2)
  expect_equal(names(tab2), c("hello", "world"))
})

test_that("read_json_arrow(schema=) with empty schema", {
  tf <- tempfile()
  writeLines('
    { "hello": 3.5, "world": 2, "third_col": 99}
    { "hello": 3.25, "world": 5, "third_col": 98}
    { "hello": 3.125, "world": 8, "third_col": 97 }
    { "hello": 0.0, "world": 10, "third_col": 96}
  ', tf)

  tab1 <- read_json_arrow(tf, schema = schema())

  expect_identical(
    tab1,
    tibble::tibble(
      hello = c(3.5, 3.25, 3.125, 0),
      world = c(2L, 5L, 8L, 10L),
      third_col = c(99L, 98L, 97L, 96L)
    )
  )
})

test_that("read_json_arrow(schema=) with partial schema", {
  tf <- tempfile()
  writeLines('
    { "hello": 3.5, "world": 2, "third_col": 99}
    { "hello": 3.25, "world": 5, "third_col": 98}
    { "hello": 3.125, "world": 8, "third_col": 97 }
    { "hello": 0.0, "world": 10, "third_col": 96}
  ', tf)

  tab1 <- read_json_arrow(tf, schema = schema(third_col = float64(), world = float64()))

  expect_identical(
    tab1,
    tibble::tibble(
      third_col = c(99, 98, 97, 96),
      world = c(2, 5, 8, 10),
      hello = c(3.5, 3.25, 3.125, 0)
    )
  )

  tf2 <- tempfile()
  writeLines('
    { "hello": 3.5, "world": 2, "third_col": "99"}
    { "hello": 3.25, "world": 5, "third_col": "98"}
    { "hello": 3.125, "world": 8, "third_col": "97"}
  ', tf2)

  tab2 <- read_json_arrow(tf2, schema = schema(third_col = string(), world = float64()))

  expect_identical(
    tab2,
    tibble::tibble(
      third_col = c("99", "98", "97"),
      world = c(2, 5, 8),
      hello = c(3.5, 3.25, 3.125)
    )
  )
})

test_that("read_json_arrow(schema=) with full schema", {
  tf <- tempfile()
  writeLines('
    { "hello": 3.5, "world": 2, "third_col": 99}
    { "hello": 3.25, "world": 5, "third_col": 98}
    { "hello": 3.125, "world": 8, "third_col": 97}
    { "hello": 0.0, "world": 10, "third_col": 96}
  ', tf)

  tab1 <- read_json_arrow(
    tf,
    schema = schema(
      hello = float64(),
      third_col = float64(),
      world = float64()
    )
  )

  expect_identical(
    tab1,
    tibble::tibble(
      hello = c(3.5, 3.25, 3.125, 0),
      third_col = c(99, 98, 97, 96),
      world = c(2, 5, 8, 10)
    )
  )
})

test_that("Can read json file with nested columns (ARROW-5503)", {
  tf <- tempfile()
  on.exit(unlink(tf))
  writeLines('
    { "arr": [1.0, 2.0, 3.0], "nuf": {} }
    { "arr": [2.0], "nuf": null }
    { "arr": [], "nuf": { "ps": 78.0, "hello": "hi" } }
    { "arr": null, "nuf": { "ps": 90.0, "hello": "bonjour" } }
    { "arr": [5.0], "nuf": { "hello": "ciao" } }
    { "arr": [5.0, 6.0], "nuf": { "ps": 19 } }
  ', tf)

  tab1 <- read_json_arrow(tf, as_data_frame = FALSE)
  tab2 <- read_json_arrow(mmap_open(tf), as_data_frame = FALSE)
  tab3 <- read_json_arrow(ReadableFile$create(tf), as_data_frame = FALSE)

  expect_equal(tab1, tab2)
  expect_equal(tab1, tab3)

  expect_equal(
    tab1$schema,
    schema(
      arr = list_of(float64()),
      nuf = struct(ps = float64(), hello = utf8())
    )
  )

  struct_array <- tab1$column(1)$chunk(0)
  ps <- Array$create(c(NA, NA, 78, 90, NA, 19))
  hello <- Array$create(c(NA, NA, "hi", "bonjour", "ciao", NA))
  expect_equal(struct_array$field(0L), ps)
  expect_equal(struct_array$GetFieldByName("ps"), ps)
  struct_cols <- struct_array$Flatten()
  expect_identical(length(struct_cols), 2L)
  expect_equal(struct_cols[[1]], ps)
  expect_equal(struct_cols[[2]], hello)
  expect_equal(
    as.vector(struct_array),
    tibble::tibble(ps = ps$as_vector(), hello = hello$as_vector())
  )

  list_array_r <- list(
    c(1, 2, 3),
    c(2),
    numeric(),
    NULL,
    5,
    c(5, 6)
  )
  list_array <- tab1$column(0)
  expect_equal(
    list_array$as_vector(),
    list_array_r,
    ignore_attr = TRUE
  )

  tib <- as.data.frame(tab1)
  expect_equal(
    tib,
    tibble::tibble(
      arr = list_array_r,
      nuf = tibble::tibble(ps = ps$as_vector(), hello = hello$as_vector())
    ),
    ignore_attr = TRUE
  )
})

test_that("Can read json file with list<struct<T...>> nested columns (ARROW-7740)", {
  tf <- tempfile()
  on.exit(unlink(tf))
  writeLines('
    {"a":[{"b":1.0},{"b":2.0}]}
    {"a":[{"b":1.0},{"b":2.0}]}
  ', tf)

  one <- tibble::tibble(b = c(1, 2))
  expected <- tibble::tibble(a = c(list(one), list(one)))
  expect_equal(read_json_arrow(tf), expected, ignore_attr = TRUE)
})

test_that("Read literal data directly", {
  expected <- tibble::tibble(x = c(1L, 3L), y = c(2L, 4L))

  expect_identical(read_json_arrow(I('{"x": 1, "y": 2}\n{"x": 3, "y": 4}')), expected)
  expect_identical(read_json_arrow(charToRaw('{"x": 1, "y": 2}\n{"x": 3, "y": 4}')), expected)
  expect_identical(read_json_arrow(I(charToRaw('{"x": 1, "y": 2}\n{"x": 3, "y": 4}'))), expected)
  expect_identical(read_json_arrow(I(c('{"x": 1, "y": 2}', '{"x": 3, "y": 4}'))), expected)
})
