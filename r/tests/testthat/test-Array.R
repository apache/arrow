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

context("Array")

test_that("Array", {
  x <- Array$create(c(1:10, 1:10, 1:5))
  expect_equal(x$type, int32())
  expect_equal(length(x), 25L)
  expect_equal(x$as_vector(), c(1:10, 1:10, 1:5))

  y <- x$Slice(10)
  expect_equal(y$type, int32())
  expect_equal(length(y), 15L)
  expect_equal(y$as_vector(), c(1:10, 1:5))
  expect_true(x$RangeEquals(y, 10, 24, 0))

  z <- x$Slice(10, 5)
  expect_equal(z$type, int32())
  expect_equal(z$length(), 5L)
  expect_equal(z$as_vector(), c(1:5))
  expect_true(x$RangeEquals(z, 10, 15, 0))

  x_dbl <- Array$create(c(1,2,3,4,5,6))
  expect_equal(x_dbl$type, float64())
  expect_equal(x_dbl$length(), 6L)
  expect_equal(x_dbl$as_vector(), as.numeric(1:6))

  y_dbl <- x_dbl$Slice(3)
  expect_equal(y_dbl$type, float64())
  expect_equal(y_dbl$length(), 3L)
  expect_equal(y_dbl$offset, 3L)
  expect_equal(y_dbl$as_vector(), as.numeric(4:6))

  z_dbl <- x_dbl$Slice(3, 2)
  expect_equal(z_dbl$type, float64())
  expect_equal(z_dbl$length(), 2L)
  expect_equal(z_dbl$as_vector(), as.numeric(4:5))
})

test_that("Array supports NA", {
  x_int <- Array$create(as.integer(c(1:10, NA)))
  x_dbl <- Array$create(as.numeric(c(1:10, NA)))
  expect_true(x_int$IsValid(0L))
  expect_true(x_dbl$IsValid(0L))
  expect_true(x_int$IsNull(10L))
  expect_true(x_dbl$IsNull(10L))

  # this is not part of Array api
  expect_equal(Array__Mask(x_int), c(rep(TRUE, 10), FALSE))
  expect_equal(Array__Mask(x_dbl), c(rep(TRUE, 10), FALSE))
})

test_that("Array supports logical vectors (ARROW-3341)", {
  # with NA
  x <- sample(c(TRUE, FALSE, NA), 1000, replace = TRUE)
  arr_lgl <- Array$create(x)
  expect_identical(x, arr_lgl$as_vector())

  # without NA
  x <- sample(c(TRUE, FALSE), 1000, replace = TRUE)
  arr_lgl <- Array$create(x)
  expect_identical(x, arr_lgl$as_vector())
})

test_that("Array supports character vectors (ARROW-3339)", {
  # with NA
  x <- c("itsy", NA, "spider")
  arr_chr <- Array$create(x)
  expect_equal(arr_chr$length(), 3L)
  expect_identical(arr_chr$as_vector(), x)
  expect_true(arr_chr$IsValid(0))
  expect_true(arr_chr$IsNull(1))
  expect_true(arr_chr$IsValid(2))

  sl <- arr_chr$Slice(1)
  expect_equal(sl$as_vector(), x[2:3])

  # without NA
  x <- c("itsy", "bitsy", "spider")
  arr_chr <- Array$create(x)
  expect_equal(arr_chr$length(), 3L)
  expect_identical(arr_chr$as_vector(), x)
})

test_that("empty arrays are supported", {
  x <- character()
  expect_equal(Array$create(x)$as_vector(), x)

  x <- integer()
  expect_equal(Array$create(x)$as_vector(), x)

  x <- numeric()
  expect_equal(Array$create(x)$as_vector(), x)

  x <- factor(character())
  expect_equal(Array$create(x)$as_vector(), x)

  x <- logical()
  expect_equal(Array$create(x)$as_vector(), x)
})

test_that("array with all nulls are supported", {
  nas <- c(NA, NA)

  x <- as.logical(nas)
  expect_equal(Array$create(x)$as_vector(), x)

  x <- as.integer(nas)
  expect_equal(Array$create(x)$as_vector(), x)

  x <- as.numeric(nas)
  expect_equal(Array$create(x)$as_vector(), x)

  x <- as.character(nas)
  expect_equal(Array$create(x)$as_vector(), x)

  x <- as.factor(nas)
  expect_equal(Array$create(x)$as_vector(), x)
})

test_that("Array supports unordered factors (ARROW-3355)", {
  # without NA
  f <- factor(c("itsy", "bitsy", "spider", "spider"))
  arr_fac <- Array$create(f)
  expect_equal(arr_fac$length(), 4L)
  expect_equal(arr_fac$type$index_type, int8())
  expect_identical(arr_fac$as_vector(), f)
  expect_true(arr_fac$IsValid(0))
  expect_true(arr_fac$IsValid(1))
  expect_true(arr_fac$IsValid(2))
  expect_true(arr_fac$IsValid(3))

  sl <- arr_fac$Slice(1)
  expect_equal(sl$length(), 3L)
  expect_equal(arr_fac$type$index_type, int8())
  expect_equal(sl$as_vector(), f[2:4])

  # with NA
  f <- factor(c("itsy", "bitsy", NA, "spider", "spider"))
  arr_fac <- Array$create(f)
  expect_equal(arr_fac$length(), 5L)
  expect_equal(arr_fac$type$index_type, int8())
  expect_identical(arr_fac$as_vector(), f)
  expect_true(arr_fac$IsValid(0))
  expect_true(arr_fac$IsValid(1))
  expect_true(arr_fac$IsNull(2))
  expect_true(arr_fac$IsValid(3))
  expect_true(arr_fac$IsValid(4))

  sl <- arr_fac$Slice(1)
  expect_equal(sl$length(), 4L)
  expect_equal(arr_fac$type$index_type, int8())
  expect_equal(sl$as_vector(), f[2:5])
})

test_that("Array supports ordered factors (ARROW-3355)", {
  # without NA
  f <- ordered(c("itsy", "bitsy", "spider", "spider"))
  arr_fac <- Array$create(f)
  expect_equal(arr_fac$length(), 4L)
  expect_equal(arr_fac$type$index_type, int8())
  expect_identical(arr_fac$as_vector(), f)
  expect_true(arr_fac$IsValid(0))
  expect_true(arr_fac$IsValid(1))
  expect_true(arr_fac$IsValid(2))
  expect_true(arr_fac$IsValid(3))

  sl <- arr_fac$Slice(1)
  expect_equal(sl$length(), 3L)
  expect_equal(arr_fac$type$index_type, int8())
  expect_equal(sl$as_vector(), f[2:4])

  # with NA
  f <- ordered(c("itsy", "bitsy", NA, "spider", "spider"))
  arr_fac <- Array$create(f)
  expect_equal(arr_fac$length(), 5L)
  expect_equal(arr_fac$type$index_type, int8())
  expect_identical(arr_fac$as_vector(), f)
  expect_true(arr_fac$IsValid(0))
  expect_true(arr_fac$IsValid(1))
  expect_true(arr_fac$IsNull(2))
  expect_true(arr_fac$IsValid(3))
  expect_true(arr_fac$IsValid(4))

  sl <- arr_fac$Slice(1)
  expect_equal(sl$length(), 4L)
  expect_equal(arr_fac$type$index_type, int8())
  expect_equal(sl$as_vector(), f[2:5])
})

test_that("array supports Date (ARROW-3340)", {
  d <- Sys.Date() + 1:10
  a <- Array$create(d)
  expect_equal(a$type, date32())
  expect_equal(a$length(), 10L)
  expect_equal(a$as_vector(), d)

  d[5] <- NA
  a <- Array$create(d)
  expect_equal(a$type, date32())
  expect_equal(a$length(), 10L)
  expect_equal(a$as_vector(), d)
  expect_true(a$IsNull(4))

  d2 <- d + .5
  a <- Array$create(d2)
  expect_equal(a$type, date32())
  expect_equal(a$length(), 10L)
  expect_equal(a$as_vector(), d)
  expect_true(a$IsNull(4))
})

test_that("array supports POSIXct (ARROW-3340)", {
  times <- lubridate::ymd_hms("2018-10-07 19:04:05") + 1:10
  a <- Array$create(times)
  expect_equal(a$type$name, "timestamp")
  expect_equal(a$type$unit(), unclass(TimeUnit$MICRO))
  expect_equal(a$length(), 10L)
  expect_equal(as.numeric(a$as_vector()), as.numeric(times))

  times[5] <- NA
  a <- Array$create(times)
  expect_equal(a$type$name, "timestamp")
  expect_equal(a$type$unit(), unclass(TimeUnit$MICRO))
  expect_equal(a$length(), 10L)
  expect_equal(as.numeric(a$as_vector()), as.numeric(times))
  expect_true(a$IsNull(4))
})

test_that("array supports integer64", {
  x <- bit64::as.integer64(1:10)
  a <- Array$create(x)
  expect_equal(a$type, int64())
  expect_equal(a$length(), 10L)
  expect_equal(a$as_vector(), x)

  x[4] <- NA
  a <- Array$create(x)
  expect_equal(a$type, int64())
  expect_equal(a$length(), 10L)
  expect_equal(a$as_vector(), x)
  expect_true(a$IsNull(3L))
})

test_that("array$as_vector() correctly handles all NA inte64 (ARROW-3795)", {
  x <- bit64::as.integer64(NA)
  a <- Array$create(x)
  expect_true(is.na(a$as_vector()))
})

test_that("array supports difftime", {
  time <- hms::hms(56, 34, 12)
  a <- Array$create(c(time, time))
  expect_equal(a$type, time32(unit = TimeUnit$SECOND))
  expect_equal(a$length(), 2L)
  expect_equal(a$as_vector(), c(time, time))

  a <- Array$create(vctrs::vec_c(time, NA))
  expect_equal(a$type, time32(unit = TimeUnit$SECOND))
  expect_equal(a$length(), 2L)
  expect_true(a$IsNull(1))
  expect_equal(a$as_vector()[1], time)
  expect_true(is.na(a$as_vector()[2]))
})

test_that("support for NaN (ARROW-3615)", {
  x <- c(1, NA, NaN, -1)
  y <- Array$create(x)
  expect_true(y$IsValid(2))
  expect_equal(y$null_count, 1L)
})

test_that("integer types casts (ARROW-3741)", {
  a <- Array$create(c(1:10, NA))
  a_int8 <- a$cast(int8())
  a_int16 <- a$cast(int16())
  a_int32 <- a$cast(int32())
  a_int64 <- a$cast(int64())

  expect_equal(a_int8$type, int8())
  expect_equal(a_int16$type, int16())
  expect_equal(a_int32$type, int32())
  expect_equal(a_int64$type, int64())
  expect_true(a_int8$IsNull(10L))
  expect_true(a_int16$IsNull(10L))
  expect_true(a_int32$IsNull(10L))
  expect_true(a_int64$IsNull(10L))

  a_uint8 <- a$cast(uint8())
  a_uint16 <- a$cast(uint16())
  a_uint32 <- a$cast(uint32())
  a_uint64 <- a$cast(uint64())

  expect_equal(a_uint8$type, uint8())
  expect_equal(a_uint16$type, uint16())
  expect_equal(a_uint32$type, uint32())
  expect_equal(a_uint64$type, uint64())
  expect_true(a_uint8$IsNull(10L))
  expect_true(a_uint16$IsNull(10L))
  expect_true(a_uint32$IsNull(10L))
  expect_true(a_uint64$IsNull(10L))
})

test_that("integer types cast safety (ARROW-3741, ARROW-5541)", {
  a <- Array$create(-(1:10))
  expect_error(a$cast(uint8()), regexp = "Integer value out of bounds")
  expect_error(a$cast(uint16()), regexp = "Integer value out of bounds")
  expect_error(a$cast(uint32()), regexp = "Integer value out of bounds")
  expect_error(a$cast(uint64()), regexp = "Integer value out of bounds")

  expect_error(a$cast(uint8(), safe = FALSE), NA)
  expect_error(a$cast(uint16(), safe = FALSE), NA)
  expect_error(a$cast(uint32(), safe = FALSE), NA)
  expect_error(a$cast(uint64(), safe = FALSE), NA)
})

test_that("float types casts (ARROW-3741)", {
  x <- c(1, 2, 3, NA)
  a <- Array$create(x)
  a_f32 <- a$cast(float32())
  a_f64 <- a$cast(float64())

  expect_equal(a_f32$type, float32())
  expect_equal(a_f64$type, float64())

  expect_true(a_f32$IsNull(3L))
  expect_true(a_f64$IsNull(3L))

  expect_equal(a_f32$as_vector(), x)
  expect_equal(a_f64$as_vector(), x)
})

test_that("cast to half float works", {
  skip("until https://issues.apache.org/jira/browse/ARROW-3802")
  a <- Array$create(1:4)
  a_f16 <- a$cast(float16())
  expect_equal(a_16$type, float16())
})

test_that("Array$create() supports the type= argument. conversion from INTSXP and int64 to all int types", {
  num_int32 <- 12L
  num_int64 <- bit64::as.integer64(10)

  types <- list(
    int8(), int16(), int32(), int64(),
    uint8(), uint16(), uint32(), uint64(),
    float32(), float64()
  )
  for(type in types) {
    expect_equal(Array$create(num_int32, type = type)$type, type)
    expect_equal(Array$create(num_int64, type = type)$type, type)
  }
})

test_that("Array$create() aborts on overflow", {
  expect_error(Array$create(128L, type = int8())$type, "Invalid.*downsize")
  expect_error(Array$create(-129L, type = int8())$type, "Invalid.*downsize")

  expect_error(Array$create(256L, type = uint8())$type, "Invalid.*downsize")
  expect_error(Array$create(-1L, type = uint8())$type, "Invalid.*downsize")

  expect_error(Array$create(32768L, type = int16())$type, "Invalid.*downsize")
  expect_error(Array$create(-32769L, type = int16())$type, "Invalid.*downsize")

  expect_error(Array$create(65536L, type = uint16())$type, "Invalid.*downsize")
  expect_error(Array$create(-1L, type = uint16())$type, "Invalid.*downsize")

  expect_error(Array$create(65536L, type = uint16())$type, "Invalid.*downsize")
  expect_error(Array$create(-1L, type = uint16())$type, "Invalid.*downsize")

  expect_error(Array$create(bit64::as.integer64(2^31), type = int32()), "Invalid.*downsize")
  expect_error(Array$create(bit64::as.integer64(2^32), type = uint32()), "Invalid.*downsize")
})

test_that("Array$create() does not convert doubles to integer", {
  types <- list(
    int8(), int16(), int32(), int64(),
    uint8(), uint16(), uint32(), uint64()
  )
  for(type in types) {
    a <- Array$create(10, type = type)
    expect_equal(a$type, type)

    # exception for now because we cannot handle
    # unsigned 64 bit integers yet
    if (type != uint64()) {
      expect_true(a$as_vector() == 10L)
    }
  }
})

test_that("Array$create() converts raw vectors to uint8 arrays (ARROW-3794)", {
  expect_equal(Array$create(as.raw(1:10))$type, uint8())
})

test_that("Array<int8>$as_vector() converts to integer (ARROW-3794)", {
  a <- Array$create((-128):127)$cast(int8())
  expect_equal(a$type, int8())
  expect_equal(a$as_vector(), (-128):127)

  a <- Array$create(0:255)$cast(uint8())
  expect_equal(a$type, uint8())
  expect_equal(a$as_vector(), 0:255)
})

test_that("Array$create() recognise arrow::Array (ARROW-3815)", {
  a <- Array$create(1:10)
  expect_equal(a, Array$create(a))
})

test_that("Array$create() handles data frame -> struct arrays (ARROW-3811)", {
  df <- tibble::tibble(x = 1:10, y = x / 2, z = letters[1:10])
  a <- Array$create(df)
  expect_equal(a$type, struct(x = int32(), y = float64(), z = utf8()))
  expect_equivalent(a$as_vector(), df)
})

test_that("Array$create() can handle data frame with custom struct type (not infered)", {
  df <- tibble::tibble(x = 1:10, y = 1:10)
  type <- struct(x = float64(), y = int16())
  a <- Array$create(df, type = type)
  expect_equal(a$type, type)

  type <- struct(x = float64(), y = int16(), z = int32())
  expect_error(Array$create(df, type = type), regexp = "Number of fields in struct.* incompatible with number of columns in the data frame")

  type <- struct(y = int16(), x = float64())
  expect_error(Array$create(df, type = type), regexp = "Field name in position.*does not match the name of the column of the data frame")

  type <- struct(x = float64(), y = utf8())
  expect_error(Array$create(df, type = type), regexp = "Cannot convert R object to string array")
})
