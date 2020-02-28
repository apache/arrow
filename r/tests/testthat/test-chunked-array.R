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

context("ChunkedArray")

test_that("ChunkedArray", {
  x <- chunked_array(1:10, 1:10, 1:5)
  expect_equal(x$type, int32())
  expect_equal(x$num_chunks, 3L)
  expect_equal(x$length(), 25L)
  expect_equal(x$as_vector(), c(1:10, 1:10, 1:5))

  y <- x$Slice(8)
  expect_equal(y$type, int32())
  expect_equal(y$num_chunks, 3L)
  expect_equal(length(y), 17L)
  expect_equal(as.vector(y), c(9:10, 1:10, 1:5))

  z <- x$Slice(8, 5)
  expect_equal(z$type, int32())
  expect_equal(z$num_chunks, 2L)
  expect_equal(z$length(), 5L)
  expect_equal(z$as_vector(), c(9:10, 1:3))

  x_dbl <- chunked_array(c(1,2,3), c(4,5,6))
  expect_equal(x_dbl$type, float64())
  expect_equal(x_dbl$num_chunks, 2L)
  expect_equal(x_dbl$length(), 6L)
  expect_equal(x_dbl$as_vector(), as.numeric(1:6))

  y_dbl <- x_dbl$Slice(2)
  expect_equal(y_dbl$type, float64())
  expect_equal(y_dbl$num_chunks, 2L)
  expect_equal(y_dbl$length(), 4L)
  expect_equal(y_dbl$as_vector(), as.numeric(3:6))

  z_dbl <- x_dbl$Slice(2, 2)
  expect_equal(z_dbl$type, float64())
  expect_equal(z_dbl$num_chunks, 2L)
  expect_equal(z_dbl$length(), 2L)
  expect_equal(z_dbl$as_vector(), as.numeric(3:4))
})

test_that("print ChunkedArray", {
  x1 <- chunked_array(c(1,2,3), c(4,5,6))
  expect_output(
    print(x1),
    paste(
      "ChunkedArray",
      "<double>",
      "[",
      "  1,",
      "  2,",
      "  3,",
      "  ...",
      "]",
      sep = "\n"
    ),
    fixed = TRUE
  )
  x2 <- chunked_array(1:30, c(4,5,6))
  expect_output(
    print(x2),
    paste(
      "ChunkedArray",
      "<int32>",
      "[",
      "  1,",
      "  2,",
      "  3,",
      "  4,",
      "  5,",
      "  6,",
      "  7,",
      "  8,",
      "  9,",
      "  10,",
      "  ...",
      "]",
      sep = "\n"
    ),
    fixed = TRUE
  )
  # If there's only one chunk, it should look like a regular Array
  x3 <- chunked_array(1:30)
  expect_output(
    print(x3),
    paste0("Chunked", paste(capture.output(print(Array$create(1:30))), collapse = "\n")),
    fixed = TRUE
  )
})

test_that("ChunkedArray handles !!! splicing", {
  data <- list(1, 2, 3)
  x <- chunked_array(!!!data)
  expect_equal(x$type, float64())
  expect_equal(x$num_chunks, 3L)
})

test_that("ChunkedArray handles NA", {
  data <- list(1:10, c(NA, 2:10), c(1:3, NA, 5L))
  x <- chunked_array(!!!data)
  expect_equal(x$type, int32())
  expect_equal(x$num_chunks, 3L)
  expect_equal(x$length(), 25L)
  expect_equal(x$as_vector(), c(1:10, c(NA, 2:10), c(1:3, NA, 5)))

  chunks <- x$chunks
  expect_equal(Array__Mask(chunks[[1]]), !is.na(data[[1]]))
  expect_equal(Array__Mask(chunks[[2]]), !is.na(data[[2]]))
  expect_equal(Array__Mask(chunks[[3]]), !is.na(data[[3]]))
})

test_that("ChunkedArray supports logical vectors (ARROW-3341)", {
  # with NA
  data <- purrr::rerun(3, sample(c(TRUE, FALSE, NA), 100, replace = TRUE))
  arr_lgl <- chunked_array(!!!data)
  expect_equal(arr_lgl$length(), 300L)
  expect_equal(arr_lgl$null_count, sum(unlist(map(data, is.na))))
  expect_identical(arr_lgl$as_vector(), purrr::flatten_lgl(data))

  chunks <- arr_lgl$chunks
  expect_identical(data[[1]], chunks[[1]]$as_vector())
  expect_identical(data[[2]], chunks[[2]]$as_vector())
  expect_identical(data[[3]], chunks[[3]]$as_vector())


  # without NA
  data <- purrr::rerun(3, sample(c(TRUE, FALSE), 100, replace = TRUE))
  arr_lgl <- chunked_array(!!!data)
  expect_equal(arr_lgl$length(), 300L)
  expect_equal(arr_lgl$null_count, sum(unlist(map(data, is.na))))
  expect_identical(arr_lgl$as_vector(), purrr::flatten_lgl(data))

  chunks <- arr_lgl$chunks
  expect_identical(data[[1]], chunks[[1]]$as_vector())
  expect_identical(data[[2]], chunks[[2]]$as_vector())
  expect_identical(data[[3]], chunks[[3]]$as_vector())
})

test_that("ChunkedArray supports character vectors (ARROW-3339)", {
  data <- list(
    c("itsy", NA, "spider"),
    c("Climbed", "up", "the", "water", "spout"),
    c("Down", "came", "the", "rain"),
    "And washed the spider out. "
  )
  arr_chr <- chunked_array(!!!data)
  expect_equal(arr_chr$length(), length(unlist(data)))
  expect_equal(arr_chr$null_count, 1L)
  expect_equal(arr_chr$as_vector(), purrr::flatten_chr(data))

  chunks <- arr_chr$chunks
  expect_equal(data, purrr::map(chunks, ~.$as_vector()))
})

test_that("ChunkedArray supports factors (ARROW-3716)", {
  f <- factor(c("itsy", "bitsy", "spider", "spider"))
  arr_fac <- chunked_array(f, f, f)
  expect_equal(arr_fac$length(), 12L)
  expect_equal(arr_fac$type$index_type, int8())
  expect_identical(arr_fac$as_vector(), vctrs::vec_c(f, f, f))
})

test_that("ChunkedArray supports dates (ARROW-3716)", {
  d <- Sys.Date() + 1:10
  a <- chunked_array(d, d)
  expect_equal(a$type, date32())
  expect_equal(a$length(), 20L)
  expect_equal(a$as_vector(), c(d, d))
})

test_that("ChunkedArray supports POSIXct (ARROW-3716)", {
  times <- lubridate::ymd_hms("2018-10-07 19:04:05") + 1:10
  a <- chunked_array(times, times)
  expect_equal(a$type$name, "timestamp")
  expect_equal(a$type$unit(), unclass(TimeUnit$MICRO))
  expect_equal(a$length(), 20L)
  expect_equal(as.numeric(a$as_vector()), as.numeric(c(times, times)))
})

test_that("ChunkedArray supports integer64 (ARROW-3716)", {
  x <- bit64::as.integer64(1:10)
  a <- chunked_array(x, x)
  expect_equal(a$type, int64())
  expect_equal(a$length(), 20L)
  expect_equal(a$as_vector(), c(x,x))
})

test_that("ChunkedArray supports difftime", {
  time <- hms::hms(56, 34, 12)
  a <- chunked_array(time, time)
  expect_equal(a$type, time32(unit = TimeUnit$SECOND))
  expect_equal(a$length(), 2L)
  expect_equal(a$as_vector(), c(time, time))
})

test_that("integer types casts for ChunkedArray (ARROW-3741)", {
  a <- chunked_array(1:10, 1:10)
  a_int8 <- a$cast(int8())
  a_int16 <- a$cast(int16())
  a_int32 <- a$cast(int32())
  a_int64 <- a$cast(int64())

  expect_is(a_int8, "ChunkedArray")
  expect_is(a_int16, "ChunkedArray")
  expect_is(a_int32, "ChunkedArray")
  expect_is(a_int64, "ChunkedArray")
  expect_equal(a_int8$type, int8())
  expect_equal(a_int16$type, int16())
  expect_equal(a_int32$type, int32())
  expect_equal(a_int64$type, int64())

  a_uint8 <- a$cast(uint8())
  a_uint16 <- a$cast(uint16())
  a_uint32 <- a$cast(uint32())
  a_uint64 <- a$cast(uint64())

  expect_is(a_uint8, "ChunkedArray")
  expect_is(a_uint16, "ChunkedArray")
  expect_is(a_uint32, "ChunkedArray")
  expect_is(a_uint64, "ChunkedArray")

  expect_equal(a_uint8$type, uint8())
  expect_equal(a_uint16$type, uint16())
  expect_equal(a_uint32$type, uint32())
  expect_equal(a_uint64$type, uint64())
})

test_that("chunked_array() supports the type= argument. conversion from INTSXP and int64 to all int types", {
  num_int32 <- 12L
  num_int64 <- bit64::as.integer64(10)

  types <- list(
    int8(), int16(), int32(), int64(),
    uint8(), uint16(), uint32(), uint64(),
    float32(), float64()
  )
  for(type in types) {
    expect_equal(chunked_array(num_int32, type = type)$type, type)
    expect_equal(chunked_array(num_int64, type = type)$type, type)
  }
})

test_that("Array$create() aborts on overflow", {
  expect_error(chunked_array(128L, type = int8())$type, "Invalid.*Value is too large")
  expect_error(chunked_array(-129L, type = int8())$type, "Invalid.*Value is too large")

  expect_error(chunked_array(256L, type = uint8())$type, "Invalid.*Value is too large")
  expect_error(chunked_array(-1L, type = uint8())$type, "Invalid.*Value is too large")

  expect_error(chunked_array(32768L, type = int16())$type, "Invalid.*Value is too large")
  expect_error(chunked_array(-32769L, type = int16())$type, "Invalid.*Value is too large")

  expect_error(chunked_array(65536L, type = uint16())$type, "Invalid.*Value is too large")
  expect_error(chunked_array(-1L, type = uint16())$type, "Invalid.*Value is too large")

  expect_error(chunked_array(65536L, type = uint16())$type, "Invalid.*Value is too large")
  expect_error(chunked_array(-1L, type = uint16())$type, "Invalid.*Value is too large")

  expect_error(chunked_array(bit64::as.integer64(2^31), type = int32()), "Invalid.*Value is too large")
  expect_error(chunked_array(bit64::as.integer64(2^32), type = uint32()), "Invalid.*Value is too large")
})

test_that("chunked_array() convert doubles to integers", {
  types <- list(
    int8(), int16(), int32(), int64(),
    uint8(), uint16(), uint32(), uint64()
  )
  for(type in types) {
    a <- chunked_array(10, type = type)
    expect_equal(a$type, type)
    if (type != uint64()) {
      # exception for unsigned integer 64 that
      # wa cannot handle yet
      expect_true(a$as_vector() == 10)
    }
  }
})

test_that("chunked_array() uses the first ... to infer type", {
  a <- chunked_array(10, 10L)
  expect_equal(a$type, float64())
})

test_that("chunked_array() handles downcasting", {
   a <- chunked_array(10L, 10)
   expect_equal(a$type, int32())
   expect_equal(a$as_vector(), c(10L, 10L))
})

test_that("chunked_array() makes chunks of the same type", {
  a <- chunked_array(10L, bit64::as.integer64(13), type = int64())
  for(chunk in a$chunks) {
    expect_equal(chunk$type, int64())
  }
})

test_that("chunked_array() handles 0 chunks if given a type", {
  types <- list(
    int8(), int16(), int32(), int64(),
    uint8(), uint16(), uint32(), uint64(),
    float32(), float64()
  )
  for(type in types) {
    a <- chunked_array(type = type)
    expect_equal(a$type, type)
    expect_equal(a$length(), 0L)
  }
})

test_that("chunked_array() can ingest arrays (ARROW-3815)", {
  expect_equal(
    chunked_array(1:5, Array$create(6:10))$as_vector(),
    1:10
  )
})

test_that("chunked_array() handles data frame -> struct arrays (ARROW-3811)", {
  df <- tibble::tibble(x = 1:10, y = x / 2, z = letters[1:10])
  a <- chunked_array(df, df)
  expect_equal(a$type, struct(x = int32(), y = float64(), z = utf8()))
  expect_equivalent(a$as_vector(), rbind(df, df))
})

test_that("ChunkedArray$View() (ARROW-6542)", {
  a <- ChunkedArray$create(1:3, 1:4)
  b <- a$View(float32())
  expect_equal(b$type, float32())
  expect_equal(length(b), 7L)
  expect_true(all(
    sapply(b$chunks, function(.x) .x$type == float32())
  ))
})

test_that("ChunkedArray$Validate()", {
  a <- ChunkedArray$create(1:10)
  expect_error(a$Validate(), NA)
})

test_that("[ ChunkedArray", {
  one_chunk <- chunked_array(2:11)
  x <- chunked_array(1:10, 31:40, 51:55)
  # Slice
  expect_vector(x[8:12], c(8:10, 31:32))
  # Take from same chunk
  expect_vector(x[c(11, 15, 12)], c(31, 35, 32))
  # Take from multiple chunks (calls Concatenate)
  expect_vector(x[c(2, 11, 15, 12, 3)], c(2, 31, 35, 32, 3))
  # Take with Array (note these are 0-based)
  take1 <- Array$create(c(10L, 14L, 11L))
  expect_vector(x[take1], c(31, 35, 32))
  # Take with ChunkedArray
  take2 <- ChunkedArray$create(c(10L, 14L), 11L)
  expect_vector(x[take2], c(31, 35, 32))

  # Filter (with recycling)
  expect_vector(
    one_chunk[c(FALSE, TRUE, FALSE, FALSE, TRUE)],
    c(3, 6, 8, 11)
  )
  # Filter where both are 1-chunk
  expect_vector(
    one_chunk[ChunkedArray$create(rep(c(FALSE, TRUE, FALSE, FALSE, TRUE), 2))],
    c(3, 6, 8, 11)
  )
  # Filter multi-chunk with logical (-> Array)
  expect_vector(
    x[c(FALSE, TRUE, FALSE, FALSE, TRUE)],
    c(2, 5, 7, 10, 32, 35, 37, 40, 52, 55)
  )
  # Filter with a chunked array with different sized chunks
  p1 <- c(FALSE, TRUE, FALSE, FALSE, TRUE)
  p2 <- c(TRUE, FALSE, TRUE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE)
  filt <- ChunkedArray$create(p1, p2, p2)
  expect_vector(
    x[filt],
    c(2, 5, 6, 8, 9, 35, 36, 38, 39, 55)
  )
})

test_that("ChunkedArray$Equals", {
  vec <- 11:20
  a <- ChunkedArray$create(vec[1:5], vec[6:10])
  b <- ChunkedArray$create(vec[1:5], vec[6:10])
  expect_equal(a, b)
  expect_true(a$Equals(b))
  expect_false(a$Equals(vec))
})
