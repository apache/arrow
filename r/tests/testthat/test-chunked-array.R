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

int_types <- c(int8(), int16(), int32(), int64())
uint_types <- c(uint8(), uint16(), uint32(), uint64())
float_types <- c(float32(), float64()) # float16() not really supported in C++ yet
all_numeric_types <- c(int_types, uint_types, float_types)

expect_chunked_roundtrip <- function(x, type) {
  a <- ChunkedArray$create(!!!x)
  flat_x <- unlist(x, recursive = FALSE)
  attributes(flat_x) <- attributes(x[[1]])
  expect_type_equal(a$type, type)
  expect_identical(a$num_chunks, length(x))
  expect_identical(length(a), length(flat_x))
  if (!inherits(type, "ListType")) {
    # TODO: revisit how missingness works with ListArrays
    # R list objects don't handle missingness the same way as other vectors.
    # Is there some vctrs thing we should do on the roundtrip back to R?
    expect_identical(as.vector(is.na(a)), is.na(flat_x))
  }
  expect_as_vector(a, flat_x)
  expect_as_vector(a$chunk(0), x[[1]])

  if (length(flat_x)) {
    a_sliced <- a$Slice(1)
    x_sliced <- flat_x[-1]
    expect_type_equal(a_sliced$type, type)
    expect_identical(length(a_sliced), length(x_sliced))
    if (!inherits(type, "ListType")) {
      expect_identical(as.vector(is.na(a_sliced)), is.na(x_sliced))
    }
    expect_as_vector(a_sliced, x_sliced)
  }
  invisible(a)
}

test_that("ChunkedArray", {
  x <- expect_chunked_roundtrip(list(1:10, 1:10, 1:5), int32())

  y <- x$Slice(8)
  expect_equal(y$type, int32())
  expect_equal(y$num_chunks, 3L)
  expect_equal(length(y), 17L)
  expect_as_vector(y, c(9:10, 1:10, 1:5))

  z <- x$Slice(8, 5)
  expect_equal(z$type, int32())
  expect_equal(z$num_chunks, 2L)
  expect_equal(z$length(), 5L)
  expect_equal(z$as_vector(), c(9:10, 1:3))

  expect_chunked_roundtrip(list(c(1, 2, 3), c(4, 5, 6)), float64())

  # input validation
  expect_error(x$chunk(14), "subscript out of bounds")
  expect_error(x$chunk("one"))
  expect_error(x$chunk(NA_integer_), "'i' cannot be NA")
  expect_error(x$chunk(-1), "subscript out of bounds")

  expect_error(x$Slice("ten"))
  expect_error(x$Slice(NA_integer_), "Slice 'offset' cannot be NA")
  expect_error(x$Slice(NA), "Slice 'offset' cannot be NA")
  expect_error(x$Slice(10, "ten"))
  expect_error(x$Slice(10, NA_integer_), "Slice 'length' cannot be NA")
  expect_error(x$Slice(NA_integer_, NA_integer_), "Slice 'offset' cannot be NA")
  expect_error(x$Slice(c(10, 10)))
  expect_error(x$Slice(10, c(10, 10)))
  expect_error(x$Slice(1000), "Slice 'offset' greater than array length")
  expect_error(x$Slice(-1), "Slice 'offset' cannot be negative")
  expect_error(z$Slice(10, 10), "Slice 'offset' greater than array length")
  expect_error(x$Slice(10, -1), "Slice 'length' cannot be negative")
  expect_error(x$Slice(-1, 10), "Slice 'offset' cannot be negative")

  expect_warning(x$Slice(10, 15), NA)
  expect_warning(
    overslice <- x$Slice(10, 16),
    "Slice 'length' greater than available length"
  )
  expect_equal(length(overslice), 15)
  expect_warning(z$Slice(2, 10), "Slice 'length' greater than available length")
})

test_that("ChunkedArray can be constructed from Array and ChunkedArrays", {
  expect_equal(
    chunked_array(Array$create(1:2), Array$create(3:4)),
    chunked_array(1:2, 3:4),
  )
  expect_equal(
    chunked_array(chunked_array(1:2, 3:4), chunked_array(5:6)),
    chunked_array(1:2, 3:4, 5:6),
  )

  # Cannot mix array types
  expect_error(
    chunked_array(Array$create(1:2), Array$create(c("a", "b"))),
    regexp = "Array chunks must all be same type"
  )
  expect_error(
    chunked_array(chunked_array(1:2), chunked_array(c("a", "b"))),
    regexp = "Array chunks must all be same type"
  )
})

test_that("print ChunkedArray", {
  verify_output(test_path("test-chunked-array.txt"), {
    chunked_array(c(1, 2, 3), c(4, 5, 6))
    chunked_array(1:30, c(4, 5, 6))
    chunked_array(1:30)
    chunked_array(factor(c("a", "b")), factor(c("c", "d")))
  })
})

test_that("ChunkedArray can be concatenated with c()", {
  a <- chunked_array(c(1, 2), 3)
  b <- chunked_array(c(4, 5), 6)
  expected <- chunked_array(c(1, 2), 3, c(4, 5), 6)
  expect_equal(c(a, b), expected)

  # Can handle Arrays and base vectors
  vectors <- list(chunked_array(1:10), Array$create(1:10), 1:10)
  expected <- chunked_array(1:10, 1:10, 1:10)
  expect_equal(do.call(c, vectors), expected)
})

test_that("ChunkedArray handles !!! splicing", {
  data <- list(1, 2, 3)
  x <- chunked_array(!!!data)
  expect_equal(x$type, float64())
  expect_equal(x$num_chunks, 3L)
})

test_that("ChunkedArray handles Inf", {
  data <- list(c(Inf, 2:10), c(1:3, Inf, 5L), 1:10)
  x <- chunked_array(!!!data)
  expect_equal(x$type, float64())
  expect_equal(x$num_chunks, 3L)
  expect_equal(length(x), 25L)
  expect_as_vector(x, c(c(Inf, 2:10), c(1:3, Inf, 5), 1:10))

  chunks <- x$chunks
  expect_as_vector(is.infinite(chunks[[2]]), is.infinite(data[[2]]))
  expect_equal(
    as.vector(is.infinite(x)),
    c(is.infinite(data[[1]]), is.infinite(data[[2]]), is.infinite(data[[3]]))
  )
})

test_that("ChunkedArray handles NA", {
  data <- list(1:10, c(NA, 2:10), c(1:3, NA, 5L))
  x <- chunked_array(!!!data)
  expect_equal(x$type, int32())
  expect_equal(x$num_chunks, 3L)
  expect_equal(length(x), 25L)
  expect_as_vector(x, c(1:10, c(NA, 2:10), c(1:3, NA, 5)))

  chunks <- x$chunks
  expect_as_vector(is.na(chunks[[2]]), is.na(data[[2]]))
  expect_as_vector(is.na(x), c(is.na(data[[1]]), is.na(data[[2]]), is.na(data[[3]])))
})

test_that("ChunkedArray handles NaN", {
  data <- list(as.numeric(1:10), c(NaN, 2:10), c(1:3, NaN, 5L))
  x <- chunked_array(!!!data)

  expect_equal(x$type, float64())
  expect_equal(x$num_chunks, 3L)
  expect_equal(length(x), 25L)
  expect_as_vector(x, c(1:10, c(NaN, 2:10), c(1:3, NaN, 5)))

  chunks <- x$chunks
  expect_as_vector(is.nan(chunks[[2]]), is.nan(data[[2]]))
  expect_as_vector(is.nan(x), c(is.nan(data[[1]]), is.nan(data[[2]]), is.nan(data[[3]])))
})

test_that("ChunkedArray supports logical vectors (ARROW-3341)", {
  # with NA
  data <- purrr::map(1:3, ~ sample(c(TRUE, FALSE, NA), 100, replace = TRUE))
  expect_chunked_roundtrip(data, bool())
  # without NA
  data <- purrr::map(1:3, ~ sample(c(TRUE, FALSE), 100, replace = TRUE))
  expect_chunked_roundtrip(data, bool())
})

test_that("ChunkedArray supports character vectors (ARROW-3339)", {
  data <- list(
    c("itsy", NA, "spider"),
    c("Climbed", "up", "the", "water", "spout"),
    c("Down", "came", "the", "rain"),
    "And washed the spider out. "
  )
  expect_chunked_roundtrip(data, utf8())
})

test_that("ChunkedArray supports factors (ARROW-3716)", {
  f <- factor(c("itsy", "bitsy", "spider", "spider"))
  expect_chunked_roundtrip(list(f, f, f), dictionary(int8()))
})

test_that("ChunkedArray supports dates (ARROW-3716)", {
  d <- Sys.Date() + 1:10
  expect_chunked_roundtrip(list(d, d), date32())
})

test_that("ChunkedArray supports POSIXct (ARROW-3716)", {
  times <- lubridate::ymd_hms("2018-10-07 19:04:05") + 1:10
  expect_chunked_roundtrip(list(times, times), timestamp("us", "UTC"))
})

test_that("ChunkedArray supports integer64 (ARROW-3716)", {
  x <- bit64::as.integer64(1:10) + MAX_INT
  expect_chunked_roundtrip(list(x, x), int64())
  # Also with a first chunk that would downcast
  zero <- Array$create(0L)$cast(int64())
  expect_type_equal(zero, int64())
  ca <- ChunkedArray$create(zero, x)
  expect_type_equal(ca, int64())
  expect_s3_class(as.vector(ca), "integer64")
  expect_identical(as.vector(ca), c(bit64::as.integer64(0L), x))
})

test_that("ChunkedArray supports hms", {
  time <- hms::hms(56, 34, 12)
  expect_chunked_roundtrip(list(time, time), time32("s"))
})

test_that("ChunkedArray supports difftime", {
  dur <- as.difftime(123, units = "secs")
  expect_chunked_roundtrip(list(dur, dur), duration(unit = "s"))
})

test_that("ChunkedArray supports empty arrays (ARROW-13761)", {
  types <- c(
    int8(), int16(), int32(), int64(), uint8(), uint16(), uint32(),
    uint64(), float32(), float64(), timestamp("ns"), binary(),
    large_binary(), fixed_size_binary(32), date32(), date64(),
    decimal128(4, 2), decimal256(4, 2),
    dictionary(), struct(x = int32())
  )

  empty_filter <- ChunkedArray$create(type = bool())
  for (type in types) {
    one_empty_chunk <- ChunkedArray$create(type = type)
    expect_type_equal(one_empty_chunk$type, type)
    if (type != struct(x = int32())) {
      expect_identical(length(one_empty_chunk), length(as.vector(one_empty_chunk)))
    } else {
      # struct -> tbl and length(tbl) is num_columns instead of num_rows
      expect_identical(length(as.vector(one_empty_chunk)), 1L)
    }
    zero_empty_chunks <- one_empty_chunk$Filter(empty_filter)
    expect_equal(zero_empty_chunks$num_chunks, 0)
    expect_type_equal(zero_empty_chunks$type, type)
    if (type != struct(x = int32())) {
      expect_identical(length(zero_empty_chunks), length(as.vector(zero_empty_chunks)))
    } else {
      expect_identical(length(as.vector(zero_empty_chunks)), 1L)
    }
  }
})

test_that("integer types casts for ChunkedArray (ARROW-3741)", {
  a <- chunked_array(1:10, 1:10)
  for (type in c(int_types, uint_types)) {
    casted <- a$cast(type)
    expect_r6_class(casted, "ChunkedArray")
    expect_type_equal(casted$type, type)
  }
  # Also test casting to double(), not actually a type, a base R function but should be alias for float64
  dbl <- a$cast(double())
  expect_r6_class(dbl, "ChunkedArray")
  expect_type_equal(dbl$type, float64())
})

test_that("chunked_array() supports the type= argument. conversion from INTSXP and int64 to all int types", {
  num_int32 <- 12L
  num_int64 <- bit64::as.integer64(10)
  for (type in all_numeric_types) {
    expect_type_equal(chunked_array(num_int32, type = type)$type, type)
    expect_type_equal(chunked_array(num_int64, type = type)$type, type)
  }
  # also test creating with double() "type"
  expect_type_equal(chunked_array(num_int32, type = double())$type, float64())
})

test_that("ChunkedArray$create() aborts on overflow", {
  expect_error(chunked_array(128L, type = int8())$type)
  expect_error(chunked_array(-129L, type = int8())$type)

  expect_error(chunked_array(256L, type = uint8())$type)
  expect_error(chunked_array(-1L, type = uint8())$type)

  expect_error(chunked_array(32768L, type = int16())$type)
  expect_error(chunked_array(-32769L, type = int16())$type)

  expect_error(chunked_array(65536L, type = uint16())$type)
  expect_error(chunked_array(-1L, type = uint16())$type)

  expect_error(chunked_array(65536L, type = uint16())$type)
  expect_error(chunked_array(-1L, type = uint16())$type)

  expect_error(chunked_array(bit64::as.integer64(2^31), type = int32()))
  expect_error(chunked_array(bit64::as.integer64(2^32), type = uint32()))
})

test_that("chunked_array() convert doubles to integers", {
  for (type in c(int_types, uint_types)) {
    a <- chunked_array(10, type = type)
    expect_type_equal(a$type, type)
    if (type != uint64()) {
      # exception for unsigned integer 64 that
      # wa cannot handle yet
      expect_true(as.vector(a) == 10)
    }
  }
})

test_that("chunked_array() uses the first ... to infer type", {
  a <- chunked_array(10, 10L)
  expect_type_equal(a$type, float64())
})

test_that("chunked_array() handles downcasting", {
  a <- chunked_array(10L, 10)
  expect_type_equal(a$type, int32())
  expect_as_vector(a, c(10L, 10L))
})

test_that("chunked_array() makes chunks of the same type", {
  a <- chunked_array(10L, bit64::as.integer64(13), type = int64())
  for (chunk in a$chunks) {
    expect_type_equal(chunk$type, int64())
  }
})

test_that("chunked_array() handles 0 chunks if given a type", {
  for (type in all_numeric_types) {
    a <- chunked_array(type = type)
    expect_type_equal(a$type, as_type(type))
    expect_equal(length(a), 0L)
  }
})

test_that("chunked_array() can ingest arrays (ARROW-3815)", {
  expect_equal(
    as.vector(chunked_array(1:5, Array$create(6:10))),
    1:10
  )
})

test_that("chunked_array() handles data frame -> struct arrays (ARROW-3811)", {
  df <- tibble::tibble(x = 1:10, y = x / 2, z = letters[1:10])
  a <- chunked_array(df, df)
  expect_type_equal(a$type, struct(x = int32(), y = float64(), z = utf8()))
  expect_equal(a$as_vector(), rbind(df, df), ignore_attr = TRUE)
})

test_that("ChunkedArray$View() (ARROW-6542)", {
  a <- ChunkedArray$create(1:3, 1:4)
  b <- a$View(float32())
  expect_equal(b$type, float32())
  expect_equal(length(b), 7L)
  expect_true(all(
    sapply(b$chunks, function(.x) .x$type == float32())
  ))
  # Input validation
  expect_error(a$View("not a type"), "type must be a DataType, not character")
})

test_that("ChunkedArray$Validate()", {
  a <- ChunkedArray$create(1:10)
  expect_error(a$Validate(), NA)
})

test_that("[ ChunkedArray", {
  one_chunk <- chunked_array(2:11)
  x <- chunked_array(1:10, 31:40, 51:55)
  # Slice
  expect_as_vector(x[8:12], c(8:10, 31:32))
  # Take from same chunk
  expect_as_vector(x[c(11, 15, 12)], c(31, 35, 32))
  # Take from multiple chunks (calls Concatenate)
  expect_as_vector(x[c(2, 11, 15, 12, 3)], c(2, 31, 35, 32, 3))
  # Take with Array (note these are 0-based)
  take1 <- Array$create(c(10L, 14L, 11L))
  expect_as_vector(x[take1], c(31, 35, 32))
  # Take with ChunkedArray
  take2 <- ChunkedArray$create(c(10L, 14L), 11L)
  expect_as_vector(x[take2], c(31, 35, 32))

  # Filter (with recycling)
  expect_as_vector(
    one_chunk[c(FALSE, TRUE, FALSE, FALSE, TRUE)],
    c(3, 6, 8, 11)
  )
  # Filter where both are 1-chunk
  expect_as_vector(
    one_chunk[ChunkedArray$create(rep(c(FALSE, TRUE, FALSE, FALSE, TRUE), 2))],
    c(3, 6, 8, 11)
  )
  # Filter multi-chunk with logical (-> Array)
  expect_as_vector(
    x[c(FALSE, TRUE, FALSE, FALSE, TRUE)],
    c(2, 5, 7, 10, 32, 35, 37, 40, 52, 55)
  )
  # Filter with a chunked array with different sized chunks
  p1 <- c(FALSE, TRUE, FALSE, FALSE, TRUE)
  p2 <- c(TRUE, FALSE, TRUE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE)
  filt <- ChunkedArray$create(p1, p2, p2)
  expect_as_vector(
    x[filt],
    c(2, 5, 6, 8, 9, 35, 36, 38, 39, 55)
  )
})

test_that("ChunkedArray head/tail", {
  vec <- 11:20
  a <- ChunkedArray$create(11:15, 16:20)
  expect_as_vector(head(a), head(vec))
  expect_as_vector(head(a, 4), head(vec, 4))
  expect_as_vector(head(a, 40), head(vec, 40))
  expect_as_vector(head(a, -4), head(vec, -4))
  expect_as_vector(head(a, -40), head(vec, -40))
  expect_as_vector(tail(a), tail(vec))
  expect_as_vector(tail(a, 4), tail(vec, 4))
  expect_as_vector(tail(a, 40), tail(vec, 40))
  expect_as_vector(tail(a, -40), tail(vec, -40))
})

test_that("ChunkedArray$Equals", {
  vec <- 11:20
  a <- ChunkedArray$create(vec[1:5], vec[6:10])
  b <- ChunkedArray$create(vec[1:5], vec[6:10])
  expect_equal(a, b)
  expect_true(a$Equals(b))
  expect_false(a$Equals(vec))
})

test_that("Converting a chunked array unifies factors (ARROW-8374)", {
  f1 <- factor(c("a"), levels = c("a", "b"))
  f2 <- factor(c("c"), levels = c("c", "d"))
  f3 <- factor(NA, levels = "a")
  f4 <- factor()

  res <- factor(c("a", "c", NA), levels = c("a", "b", "c", "d"))
  ca <- ChunkedArray$create(f1, f2, f3, f4)

  expect_identical(ca$as_vector(), res)
})

test_that("Handling string data with embedded nuls", {
  raws <- structure(list(
    as.raw(c(0x70, 0x65, 0x72, 0x73, 0x6f, 0x6e)),
    as.raw(c(0x77, 0x6f, 0x6d, 0x61, 0x6e)),
    as.raw(c(0x6d, 0x61, 0x00, 0x6e)), # <-- there's your nul, 0x00
    as.raw(c(0x66, 0x00, 0x00, 0x61, 0x00, 0x6e)), # multiple nuls
    as.raw(c(0x63, 0x61, 0x6d, 0x65, 0x72, 0x61)),
    as.raw(c(0x74, 0x76))
  ),
  class = c("arrow_binary", "vctrs_vctr", "list")
  )
  chunked_array_with_nul <- ChunkedArray$create(raws)$cast(utf8())

  # The behavior of the warnings/errors is slightly different with and without
  # altrep. Without it (i.e. 3.5.0 and below, the error would trigger immediately
  # on `as.vector()` where as with it, the error only happens on materialization)
  skip_on_r_older_than("3.6")

  v <- expect_error(as.vector(chunked_array_with_nul), NA)

  expect_error(
    v[],
    paste0(
      "embedded nul in string: 'ma\\0n'; to strip nuls when converting from Arrow to R, ",
      "set options(arrow.skip_nul = TRUE)"
    ),
    fixed = TRUE
  )

  withr::with_options(list(arrow.skip_nul = TRUE), {
    v <- expect_warning(as.vector(chunked_array_with_nul), NA)
    expect_warning(
      expect_identical(v[3], "man"),
      "Stripping '\\0' (nul) from character vector",
      fixed = TRUE
    )
  })
})

test_that("as_chunked_array() default method calls chunked_array()", {
  expect_equal(
    as_chunked_array(chunked_array(1:3, 4:5)),
    chunked_array(1:3, 4:5)
  )

  expect_equal(
    as_chunked_array(chunked_array(1:3, 4:5), type = float64()),
    chunked_array(
      Array$create(1:3, type = float64()),
      Array$create(4:5, type = float64())
    )
  )
})

test_that("as_chunked_array() works for ChunkedArray", {
  array <- chunked_array(type = null())
  expect_identical(as_chunked_array(array), array)
  expect_equal(
    as_chunked_array(array, type = int32()),
    chunked_array(type = int32())
  )
})

test_that("as_chunked_array() works for Array", {
  expect_equal(
    as_chunked_array(Array$create(logical(), type = null())),
    chunked_array(type = null())
  )

  expect_equal(
    as_chunked_array(Array$create(1:6)),
    chunked_array(Array$create(1:6))
  )

  expect_equal(
    as_chunked_array(Array$create(1:6), type = float64()),
    chunked_array(Array$create(1:6, type = float64()))
  )
})
