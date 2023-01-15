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

# Defining some type groups for use here and in the following tests
int_types <- c(int8(), int16(), int32(), int64())
uint_types <- c(uint8(), uint16(), uint32(), uint64())
float_types <- c(float32(), float64()) # float16() not really supported in C++ yet

test_that("Integer Array", {
  ints <- c(1:10, 1:10, 1:5)
  x <- expect_array_roundtrip(ints, int32())
})

test_that("binary Array", {
  # if the type is given, we just need a list of raw vectors
  bin <- list(as.raw(1:10), as.raw(1:10))
  expect_array_roundtrip(bin, binary(), as = binary())
  expect_array_roundtrip(bin, large_binary(), as = large_binary())
  expect_array_roundtrip(bin, fixed_size_binary(10), as = fixed_size_binary(10))

  bin[[1L]] <- as.raw(1:20)
  expect_error(Array$create(bin, fixed_size_binary(10)))

  # otherwise the arrow type is deduced from the R classes
  bin <- vctrs::new_vctr(
    list(as.raw(1:10), as.raw(11:20)),
    class = "arrow_binary"
  )
  expect_array_roundtrip(bin, binary())

  bin <- vctrs::new_vctr(
    list(as.raw(1:10), as.raw(11:20)),
    class = "arrow_large_binary"
  )
  expect_array_roundtrip(bin, large_binary())

  bin <- vctrs::new_vctr(
    list(as.raw(1:10), as.raw(11:20)),
    class = "arrow_fixed_size_binary",
    byte_width = 10L
  )
  expect_array_roundtrip(bin, fixed_size_binary(byte_width = 10))

  # degenerate cases
  skip_on_linux_devel() # valgrind errors on these tests ARROW-12638
  bin <- vctrs::new_vctr(
    list(1:10),
    class = "arrow_binary"
  )
  expect_error(Array$create(bin))

  bin <- vctrs::new_vctr(
    list(1:10),
    ptype = raw(),
    class = "arrow_large_binary"
  )
  expect_error(Array$create(bin))

  bin <- vctrs::new_vctr(
    list(1:10),
    class = "arrow_fixed_size_binary",
    byte_width = 10
  )
  expect_error(Array$create(bin))

  bin <- vctrs::new_vctr(
    list(as.raw(1:5)),
    class = "arrow_fixed_size_binary",
    byte_width = 10
  )
  expect_error(Array$create(bin))

  bin <- vctrs::new_vctr(
    list(as.raw(1:5)),
    class = "arrow_fixed_size_binary"
  )
  expect_error(Array$create(bin))
})

test_that("Slice() and RangeEquals()", {
  ints <- c(1:10, 101:110, 201:205)
  x <- Array$create(ints)

  y <- x$Slice(10)
  expect_equal(y$type, int32())
  expect_equal(length(y), 15L)
  expect_as_vector(y, c(101:110, 201:205))
  expect_true(x$RangeEquals(y, 10, 24))
  expect_false(x$RangeEquals(y, 9, 23))
  expect_false(x$RangeEquals(y, 11, 24))

  z <- x$Slice(10, 5)
  expect_as_vector(z, c(101:105))
  expect_true(x$RangeEquals(z, 10, 15, 0))

  # Input validation
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

  expect_error(x$RangeEquals(10, 24, 0), 'other must be a "Array"')
  expect_error(x$RangeEquals(y, NA, 24), "'start_idx' cannot be NA")
  expect_error(x$RangeEquals(y, 10, NA), "'end_idx' cannot be NA")
  expect_error(x$RangeEquals(y, 10, 24, NA), "'other_start_idx' cannot be NA")
  expect_error(x$RangeEquals(y, "ten", 24))

  skip("TODO: (if anyone uses RangeEquals)")
  expect_error(x$RangeEquals(y, 10, 2400, 0)) # does not error
  expect_error(x$RangeEquals(y, 1000, 24, 0)) # does not error
  expect_error(x$RangeEquals(y, 10, 24, 1000)) # does not error
})

test_that("Double Array", {
  dbls <- c(1, 2, 3, 4, 5, 6)
  x_dbl <- expect_array_roundtrip(dbls, float64())
})

test_that("Array print method includes type", {
  x <- Array$create(c(1:10, 1:10, 1:5))
  expect_output(print(x), "Array\n<int32>\n[\n", fixed = TRUE)
})

test_that("Array supports NA", {
  x_int <- Array$create(as.integer(c(1:10, NA)))
  x_dbl <- Array$create(as.numeric(c(1:10, NA)))
  expect_true(x_int$IsValid(0))
  expect_true(x_dbl$IsValid(0L))
  expect_true(x_int$IsNull(10L))
  expect_true(x_dbl$IsNull(10))

  expect_as_vector(is.na(x_int), c(rep(FALSE, 10), TRUE))
  expect_as_vector(is.na(x_dbl), c(rep(FALSE, 10), TRUE))

  # Input validation
  expect_error(x_int$IsValid("ten"))
  expect_error(x_int$IsNull("ten"))
  expect_error(x_int$IsValid(c(10, 10)))
  expect_error(x_int$IsNull(c(10, 10)))
  expect_error(x_int$IsValid(NA), "'i' cannot be NA")
  expect_error(x_int$IsNull(NA), "'i' cannot be NA")
  expect_error(x_int$IsValid(1000), "subscript out of bounds")
  expect_error(x_int$IsValid(-1), "subscript out of bounds")
  expect_error(x_int$IsNull(1000), "subscript out of bounds")
  expect_error(x_int$IsNull(-1), "subscript out of bounds")
})

test_that("Array support null type (ARROW-7064)", {
  expect_array_roundtrip(vctrs::unspecified(10), null())
})

test_that("Array support 0-length NULL vectors (Arrow-17543)", {
  expect_type_equal(Array$create(c()), null())
  expect_type_equal(Array$create(NULL), null())
})

test_that("Array supports logical vectors (ARROW-3341)", {
  # with NA
  x <- sample(c(TRUE, FALSE, NA), 1000, replace = TRUE)
  expect_array_roundtrip(x, bool())

  # without NA
  x <- sample(c(TRUE, FALSE), 1000, replace = TRUE)
  expect_array_roundtrip(x, bool())
})

test_that("Array supports character vectors (ARROW-3339)", {
  # without NA
  expect_array_roundtrip(c("itsy", "bitsy", "spider"), utf8())
  expect_array_roundtrip(c("itsy", "bitsy", "spider"), large_utf8(), as = large_utf8())

  # with NA
  expect_array_roundtrip(c("itsy", NA, "spider"), utf8())
  expect_array_roundtrip(c("itsy", NA, "spider"), large_utf8(), as = large_utf8())
})

test_that("Character vectors > 2GB become large_utf8", {
  skip_on_cran()
  skip_if_not_running_large_memory_tests()
  big <- make_big_string()
  expect_array_roundtrip(big, large_utf8())
})

test_that("Arrays with length > INT_MAX can be created and inspected", {
  skip_on_cran()
  skip_if_not_running_large_memory_tests()

  big <- raw(as.double(.Machine$integer.max) + 2)
  big[length(big)] <- as.raw(0xff)
  big_array <- Array$create(big, type = uint8())
  expect_identical(length(big_array), length(big))
  expect_identical(
    Array__GetScalar(big_array, length(big) - 1)$as_vector(),
    255L
  )

  # Calling big_array$as_vector() will return an 8 GB integer vector
  # which is too big to run on CI.
})

test_that("empty arrays are supported", {
  expect_array_roundtrip(character(), utf8())
  expect_array_roundtrip(character(), large_utf8(), as = large_utf8())
  expect_array_roundtrip(integer(), int32())
  expect_array_roundtrip(numeric(), float64())
  expect_array_roundtrip(factor(character()), dictionary(int8(), utf8()))
  expect_array_roundtrip(logical(), bool())
})

test_that("array with all nulls are supported", {
  nas <- c(NA, NA)
  expect_array_roundtrip(as.character(nas), utf8())
  expect_array_roundtrip(as.integer(nas), int32())
  expect_array_roundtrip(as.numeric(nas), float64())
  expect_array_roundtrip(as.factor(nas), dictionary(int8(), utf8()))
  expect_array_roundtrip(as.logical(nas), bool())
})

test_that("Array supports unordered factors (ARROW-3355)", {
  # without NA
  f <- factor(c("itsy", "bitsy", "spider", "spider"))
  expect_array_roundtrip(f, dictionary(int8(), utf8()))

  # with NA
  f <- factor(c("itsy", "bitsy", NA, "spider", "spider"))
  expect_array_roundtrip(f, dictionary(int8(), utf8()))
})

test_that("Array supports ordered factors (ARROW-3355)", {
  # without NA
  f <- ordered(c("itsy", "bitsy", "spider", "spider"))
  arr_fac <- expect_array_roundtrip(f, dictionary(int8(), utf8(), ordered = TRUE))
  expect_true(arr_fac$ordered)

  # with NA
  f <- ordered(c("itsy", "bitsy", NA, "spider", "spider"))
  expect_array_roundtrip(f, dictionary(int8(), utf8(), ordered = TRUE))
})

test_that("array supports Date (ARROW-3340)", {
  d <- Sys.Date() + 1:10
  expect_array_roundtrip(d, date32())

  d[5] <- NA
  expect_array_roundtrip(d, date32())
})

test_that("array supports POSIXct (ARROW-3340)", {
  times <- lubridate::ymd_hms("2018-10-07 19:04:05") + 1:10
  expect_array_roundtrip(times, timestamp("us", "UTC"))

  times[5] <- NA
  expect_array_roundtrip(times, timestamp("us", "UTC"))

  times2 <- lubridate::ymd_hms("2018-10-07 19:04:05", tz = "US/Eastern") + 1:10
  expect_array_roundtrip(times2, timestamp("us", "US/Eastern"))
})

test_that("array uses local timezone for POSIXct without timezone", {
  withr::with_envvar(c(TZ = ""), {
    times <- strptime("2019-02-03 12:34:56", format = "%Y-%m-%d %H:%M:%S") + 1:10
    expect_equal(attr(times, "tzone"), NULL)
    expect_array_roundtrip(times, timestamp("us", Sys.timezone()))

    # Also test the INTSXP code path
    skip("Ingest_POSIXct only implemented for REALSXP")
    times_int <- as.integer(times)
    attributes(times_int) <- attributes(times)
    expect_array_roundtrip(times_int, timestamp("us", ""))
  })

  # If there is a timezone set, we record that
  withr::with_timezone("Pacific/Marquesas", {
    times <- strptime("2019-02-03 12:34:56", format = "%Y-%m-%d %H:%M:%S") + 1:10
    expect_equal(attr(times, "tzone"), "Pacific/Marquesas")
    expect_array_roundtrip(times, timestamp("us", "Pacific/Marquesas"))

    times_with_tz <- strptime(
      "2019-02-03 12:34:56",
      format = "%Y-%m-%d %H:%M:%S",
      tz = "Asia/Katmandu"
    ) + 1:10
    expect_equal(attr(times, "tzone"), "Asia/Katmandu")
    expect_array_roundtrip(times, timestamp("us", "Asia/Katmandu"))
  })

  # and although the TZ is NULL in R, we set it to the Sys.timezone()
  withr::with_timezone(NA, {
    times <- strptime("2019-02-03 12:34:56", format = "%Y-%m-%d %H:%M:%S") + 1:10
    expect_equal(attr(times, "tzone"), NULL)
    expect_array_roundtrip(times, timestamp("us", Sys.timezone()))
  })
})

test_that("Timezone handling in Arrow roundtrip (ARROW-3543)", {
  # Write a feather file as that's what the initial bug report used
  df <- tibble::tibble(
    no_tz = lubridate::ymd_hms("2018-10-07 19:04:05") + 1:10,
    yes_tz = lubridate::ymd_hms("2018-10-07 19:04:05", tz = "Pacific/Marquesas") + 1:10
  )
  if (!identical(Sys.timezone(), "Pacific/Marquesas")) {
    # Confirming that the columns are in fact different
    expect_false(any(df$no_tz == df$yes_tz))
  }
  feather_file <- tempfile()
  on.exit(unlink(feather_file))
  write_feather(df, feather_file)
  expect_identical(read_feather(feather_file), df)
})

test_that("array supports integer64", {
  x <- bit64::as.integer64(1:10) + MAX_INT
  expect_array_roundtrip(x, int64())

  x[4] <- NA
  expect_array_roundtrip(x, int64())

  # all NA int64 (ARROW-3795)
  all_na <- Array$create(bit64::as.integer64(NA))
  expect_type_equal(all_na, int64())
  expect_true(as.vector(is.na(all_na)))
})

test_that("array supports hms difftime", {
  time <- hms::hms(56, 34, 12)
  expect_array_roundtrip(c(time, time), time32("s"))
  expect_array_roundtrip(vctrs::vec_c(NA, time), time32("s"))
})

test_that("array supports difftime", {
  time <- as.difftime(1234, units = "secs")
  expect_array_roundtrip(c(time, time), duration("s"))
  expect_array_roundtrip(vctrs::vec_c(NA, time), duration("s"))
})

test_that("support for NaN (ARROW-3615)", {
  x <- c(1, NA, NaN, -1)
  y <- Array$create(x)
  expect_true(y$IsValid(2))
  expect_equal(y$null_count, 1L)
})

test_that("is.nan() evalutes to FALSE on NA (for consistency with base R)", {
  x <- c(1.0, NA, NaN, -1.0)
  compare_expression(is.nan(.input), x)
})

test_that("is.nan() evalutes to FALSE on non-floats (for consistency with base R)", {
  x <- c(1L, 2L, 3L)
  y <- c("foo", "bar")
  compare_expression(is.nan(.input), x)
  compare_expression(is.nan(.input), y)
})

test_that("is.na() evalutes to TRUE on NaN (for consistency with base R)", {
  x <- c(1, NA, NaN, -1)
  compare_expression(is.na(.input), x)
})

test_that("integer types casts (ARROW-3741)", {
  a <- Array$create(c(1:10, NA))
  for (type in c(int_types, uint_types)) {
    casted <- a$cast(type)
    expect_equal(casted$type, type)
    expect_identical(as.vector(is.na(casted)), c(rep(FALSE, 10), TRUE))
  }
})

test_that("integer types cast safety (ARROW-3741, ARROW-5541)", {
  a <- Array$create(-(1:10))
  for (type in uint_types) {
    expect_error(a$cast(type), regexp = "Integer value -1 not in range")
    expect_error(a$cast(type, safe = FALSE), NA)
  }
})

test_that("float types casts (ARROW-3741)", {
  x <- c(1, 2, 3, NA)
  a <- Array$create(x)
  for (type in float_types) {
    casted <- a$cast(type)
    expect_equal(casted$type, type)
    expect_identical(as.vector(is.na(casted)), c(rep(FALSE, 3), TRUE))
    expect_identical(as.vector(casted), x)
  }
})

test_that("cast to half float works", {
  skip("Need halffloat support: https://issues.apache.org/jira/browse/ARROW-3802")
  a <- Array$create(1:4)
  a_f16 <- a$cast(float16())
  expect_type_equal(a_16$type, float16())
})

test_that("cast input validation", {
  a <- Array$create(1:4)
  expect_error(a$cast("not a type"), "type must be a DataType, not character")
})

test_that("Array$create() supports the type= argument. conversion from INTSXP and int64 to all int types", {
  num_int32 <- 12L
  num_int64 <- bit64::as.integer64(10)

  types <- c(
    int_types,
    uint_types,
    float_types,
    double() # not actually a type, a base R function but should be alias for float64
  )
  for (type in types) {
    expect_type_equal(Array$create(num_int32, type = type)$type, as_type(type))
    expect_type_equal(Array$create(num_int64, type = type)$type, as_type(type))
  }

  # Input validation
  expect_error(
    Array$create(5, type = "not a type"),
    "type must be a DataType, not character"
  )
})

test_that("Array$create() aborts on overflow", {
  expect_error(Array$create(128L, type = int8()))
  expect_error(Array$create(-129L, type = int8()))

  expect_error(Array$create(256L, type = uint8()))
  expect_error(Array$create(-1L, type = uint8()))

  expect_error(Array$create(32768L, type = int16()))
  expect_error(Array$create(-32769L, type = int16()))

  expect_error(Array$create(65536L, type = uint16()))
  expect_error(Array$create(-1L, type = uint16()))

  expect_error(Array$create(65536L, type = uint16()))
  expect_error(Array$create(-1L, type = uint16()))

  expect_error(Array$create(bit64::as.integer64(2^31), type = int32()))
  expect_error(Array$create(bit64::as.integer64(2^32), type = uint32()))
})

test_that("Array$create() does not convert doubles to integer", {
  for (type in c(int_types, uint_types)) {
    a <- Array$create(10, type = type)
    expect_type_equal(a$type, type)
    expect_true(as.vector(a) == 10L)
  }
})

test_that("Array$create() converts raw vectors to uint8 arrays (ARROW-3794)", {
  expect_type_equal(Array$create(as.raw(1:10))$type, uint8())
})

test_that("Array<int8>$as_vector() converts to integer (ARROW-3794)", {
  i8 <- (-128):127
  a <- Array$create(i8)$cast(int8())
  expect_type_equal(a, int8())
  expect_as_vector(a, i8)

  u8 <- 0:255
  a <- Array$create(u8)$cast(uint8())
  expect_type_equal(a, uint8())
  expect_as_vector(a, u8)
})

test_that("Arrays of {,u}int{32,64} convert to integer if they can fit", {
  u32 <- Array$create(1L)$cast(uint32())
  expect_identical(as.vector(u32), 1L)

  u64 <- Array$create(1L)$cast(uint64())
  expect_identical(as.vector(u64), 1L)

  i64 <- Array$create(bit64::as.integer64(1:10))
  expect_identical(as.vector(i64), 1:10)
})

test_that("Arrays of uint{32,64} convert to numeric if they can't fit integer", {
  u32 <- Array$create(bit64::as.integer64(1) + MAX_INT)$cast(uint32())
  expect_identical(as.vector(u32), 1 + MAX_INT)

  u64 <- Array$create(bit64::as.integer64(1) + MAX_INT)$cast(uint64())
  expect_identical(as.vector(u64), 1 + MAX_INT)
})

test_that("Array$create() recognise arrow::Array (ARROW-3815)", {
  a <- Array$create(1:10)
  expect_equal(a, Array$create(a))
})

test_that("Array$create() handles data frame -> struct arrays (ARROW-3811)", {
  df <- tibble::tibble(x = 1:10, y = x / 2, z = letters[1:10])
  a <- Array$create(df)
  expect_type_equal(a$type, struct(x = int32(), y = float64(), z = utf8()))
  expect_as_vector(a, df)

  df <- structure(
    list(col = list(list(list(1)))),
    class = "data.frame", row.names = c(NA, -1L)
  )
  a <- Array$create(df)
  expect_type_equal(a$type, struct(col = list_of(list_of(list_of(float64())))))
  expect_as_vector(a, df, ignore_attr = TRUE)
})

test_that("StructArray methods", {
  df <- tibble::tibble(x = 1:10, y = x / 2, z = letters[1:10])
  a <- Array$create(df)
  expect_equal(a$x, Array$create(df$x))
  expect_equal(a[["x"]], Array$create(df$x))
  expect_equal(a[[1]], Array$create(df$x))
  expect_identical(names(a), c("x", "y", "z"))
  expect_identical(dim(a), c(10L, 3L))
})

test_that("StructArray creation", {
  # from data.frame
  a <- StructArray$create(example_data)
  expect_identical(names(a), c("int", "dbl", "dbl2", "lgl", "false", "chr", "fct"))
  expect_identical(dim(a), c(10L, 7L))
  expect_r6_class(a, "StructArray")

  # from Arrays
  str_array <- StructArray$create(a = Array$create(1:2), b = Array$create(c("a", "b")))
  expect_equal(str_array[[1]], Array$create(1:2))
  expect_equal(str_array[[2]], Array$create(c("a", "b")))
  expect_r6_class(str_array, "StructArray")
})

test_that("Array$create() can handle data frame with custom struct type (not inferred)", {
  df <- tibble::tibble(x = 1:10, y = 1:10)
  type <- struct(x = float64(), y = int16())
  a <- Array$create(df, type = type)
  expect_type_equal(a$type, type)
  type <- struct(x = float64(), y = int16(), z = int32())
  expect_error(
    Array$create(df, type = type),
    regexp = "Number of fields in struct.* incompatible with number of columns in the data frame"
  )

  type <- struct(y = int16(), x = float64())
  expect_error(
    Array$create(df, type = type),
    regexp = "Field name in position.*does not match the name of the column of the data frame"
  )

  type <- struct(x = float64(), y = utf8())
  expect_error(Array$create(df, type = type), regexp = "Invalid")
})

test_that("Array$create() supports tibble with no columns (ARROW-8354)", {
  df <- tibble::tibble()
  expect_equal(Array$create(df)$as_vector(), df)
})

test_that("Array$create() handles vector -> list arrays (ARROW-7662)", {
  # Should be able to create an empty list with a type hint.
  expect_r6_class(Array$create(list(), list_of(bool())), "ListArray")

  # logical
  expect_array_roundtrip(list(NA), list_of(bool()))
  expect_array_roundtrip(list(logical(0)), list_of(bool()))
  expect_array_roundtrip(list(c(TRUE), c(FALSE), c(FALSE, TRUE)), list_of(bool()))
  expect_array_roundtrip(list(c(TRUE), c(FALSE), NA, logical(0), c(FALSE, NA, TRUE)), list_of(bool()))

  # integer
  expect_array_roundtrip(list(NA_integer_), list_of(int32()))
  expect_array_roundtrip(list(integer(0)), list_of(int32()))
  expect_array_roundtrip(list(1:2, 3:4, 12:18), list_of(int32()))
  expect_array_roundtrip(list(c(1:2), NA_integer_, integer(0), c(12:18, NA_integer_)), list_of(int32()))

  # numeric
  expect_array_roundtrip(list(NA_real_), list_of(float64()))
  expect_array_roundtrip(list(numeric(0)), list_of(float64()))
  expect_array_roundtrip(list(1, c(2, 3), 4), list_of(float64()))
  expect_array_roundtrip(list(1, numeric(0), c(2, 3, NA_real_), 4), list_of(float64()))

  # character
  expect_array_roundtrip(list(NA_character_), list_of(utf8()))
  expect_array_roundtrip(list(character(0)), list_of(utf8()))
  expect_array_roundtrip(list("itsy", c("bitsy", "spider"), c("is")), list_of(utf8()))
  expect_array_roundtrip(list("itsy", character(0), c("bitsy", "spider", NA_character_), c("is")), list_of(utf8()))

  # factor
  expect_array_roundtrip(list(factor(c("b", "a"), levels = c("a", "b"))), list_of(dictionary(int8(), utf8())))
  expect_array_roundtrip(list(factor(NA, levels = c("a", "b"))), list_of(dictionary(int8(), utf8())))

  # struct
  expect_array_roundtrip(
    list(tibble::tibble(a = integer(0), b = integer(0), c = character(0), d = logical(0))),
    list_of(struct(a = int32(), b = int32(), c = utf8(), d = bool()))
  )
  expect_array_roundtrip(
    list(tibble::tibble(a = list(integer()))),
    list_of(struct(a = list_of(int32())))
  )
  # degenerated data frame
  df <- structure(list(x = 1:2, y = 1), class = "data.frame", row.names = 1:2)
  expect_error(Array$create(list(df)))
})

test_that("Array$create() handles list of dataframes -> map arrays", {
  # Should be able to create an empty map with a type hint.
  expect_r6_class(Array$create(list(), type = map_of(utf8(), boolean())), "MapArray")

  # MapType is alias for List<Struct<keys, values>>
  data <- list(
    data.frame(key = c("a", "b"), value = c(1, 2), stringsAsFactors = FALSE),
    data.frame(key = c("a", "c"), value = c(4, 7), stringsAsFactors = FALSE)
  )
  arr <- Array$create(data, type = map_of(utf8(), int32()))

  expect_r6_class(arr, "MapArray")
  expect_as_vector(arr, data, ignore_attr = TRUE)

  expect_equal(arr$keys()$type, utf8())
  expect_equal(arr$items()$type, int32())
  expect_equal(arr$keys(), Array$create(c("a", "b", "a", "c")))
  expect_equal(arr$items(), Array$create(c(1, 2, 4, 7), type = int32()))

  expect_equal(arr$keys_nested()$type, list_of(utf8()))
  expect_equal(arr$items_nested()$type, list_of(int32()))
  expect_equal(arr$keys_nested(), Array$create(list(c("a", "b"), c("a", "c")), type = list_of(utf8())))
  expect_equal(arr$items_nested(), Array$create(list(c(1, 2), c(4, 7)), type = list_of(int32())))
})

test_that("Array$create() handles vector -> large list arrays", {
  # Should be able to create an empty list with a type hint.
  expect_r6_class(Array$create(list(), type = large_list_of(bool())), "LargeListArray")

  # logical
  expect_array_roundtrip(list(NA), large_list_of(bool()), as = large_list_of(bool()))
  expect_array_roundtrip(list(logical(0)), large_list_of(bool()), as = large_list_of(bool()))
  expect_array_roundtrip(list(c(TRUE), c(FALSE), c(FALSE, TRUE)), large_list_of(bool()), as = large_list_of(bool()))
  expect_array_roundtrip(
    list(c(TRUE), c(FALSE), NA, logical(0), c(FALSE, NA, TRUE)),
    large_list_of(bool()),
    as = large_list_of(bool())
  )

  # integer
  expect_array_roundtrip(list(NA_integer_), large_list_of(int32()), as = large_list_of(int32()))
  expect_array_roundtrip(list(integer(0)), large_list_of(int32()), as = large_list_of(int32()))
  expect_array_roundtrip(list(1:2, 3:4, 12:18), large_list_of(int32()), as = large_list_of(int32()))
  expect_array_roundtrip(
    list(c(1:2), NA_integer_, integer(0), c(12:18, NA_integer_)),
    large_list_of(int32()),
    as = large_list_of(int32())
  )

  # numeric
  expect_array_roundtrip(list(NA_real_), large_list_of(float64()), as = large_list_of(float64()))
  expect_array_roundtrip(list(numeric(0)), large_list_of(float64()), as = large_list_of(float64()))
  expect_array_roundtrip(list(1, c(2, 3), 4), large_list_of(float64()), as = large_list_of(float64()))
  expect_array_roundtrip(
    list(1, numeric(0), c(2, 3, NA_real_), 4),
    large_list_of(float64()),
    as = large_list_of(float64())
  )

  # character
  expect_array_roundtrip(list(NA_character_), large_list_of(utf8()), as = large_list_of(utf8()))
  expect_array_roundtrip(list(character(0)), large_list_of(utf8()), as = large_list_of(utf8()))
  expect_array_roundtrip(
    list("itsy", c("bitsy", "spider"), c("is")),
    large_list_of(utf8()),
    as = large_list_of(utf8())
  )
  expect_array_roundtrip(
    list("itsy", character(0), c("bitsy", "spider", NA_character_), c("is")),
    large_list_of(utf8()),
    as = large_list_of(utf8())
  )

  # factor
  expect_array_roundtrip(
    list(factor(c("b", "a"), levels = c("a", "b"))),
    large_list_of(dictionary(int8(), utf8())),
    as = large_list_of(dictionary(int8(), utf8()))
  )
  expect_array_roundtrip(
    list(factor(NA, levels = c("a", "b"))),
    large_list_of(dictionary(int8(), utf8())),
    as = large_list_of(dictionary(int8(), utf8()))
  )

  # struct
  expect_array_roundtrip(
    list(tibble::tibble(a = integer(0), b = integer(0), c = character(0), d = logical(0))),
    large_list_of(struct(a = int32(), b = int32(), c = utf8(), d = bool())),
    as = large_list_of(struct(a = int32(), b = int32(), c = utf8(), d = bool()))
  )
  expect_array_roundtrip(
    list(tibble::tibble(a = list(integer()))),
    large_list_of(struct(a = list_of(int32()))),
    as = large_list_of(struct(a = list_of(int32())))
  )
})

test_that("Array$create() handles vector -> fixed size list arrays", {
  # Should be able to create an empty list with a type hint.
  expect_r6_class(Array$create(list(), type = fixed_size_list_of(bool(), 20)), "FixedSizeListArray")

  # logical
  expect_array_roundtrip(list(NA), fixed_size_list_of(bool(), 1L), as = fixed_size_list_of(bool(), 1L))
  expect_array_roundtrip(
    list(c(TRUE, FALSE), c(FALSE, TRUE)),
    fixed_size_list_of(bool(), 2L),
    as = fixed_size_list_of(bool(), 2L)
  )
  expect_array_roundtrip(
    list(c(TRUE), c(FALSE), NA),
    fixed_size_list_of(bool(), 1L),
    as = fixed_size_list_of(bool(), 1L)
  )

  # integer
  expect_array_roundtrip(list(NA_integer_), fixed_size_list_of(int32(), 1L), as = fixed_size_list_of(int32(), 1L))
  expect_array_roundtrip(list(1:2, 3:4, 11:12), fixed_size_list_of(int32(), 2L), as = fixed_size_list_of(int32(), 2L))
  expect_array_roundtrip(
    list(c(1:2), c(NA_integer_, 3L)),
    fixed_size_list_of(int32(), 2L),
    as = fixed_size_list_of(int32(), 2L)
  )

  # numeric
  expect_array_roundtrip(list(NA_real_), fixed_size_list_of(float64(), 1L), as = fixed_size_list_of(float64(), 1L))
  expect_array_roundtrip(
    list(c(1, 2), c(2, 3)),
    fixed_size_list_of(float64(), 2L),
    as = fixed_size_list_of(float64(), 2L)
  )
  expect_array_roundtrip(
    list(c(1, 2), c(NA_real_, 4)),
    fixed_size_list_of(float64(), 2L),
    as = fixed_size_list_of(float64(), 2L)
  )

  # character
  expect_array_roundtrip(list(NA_character_), fixed_size_list_of(utf8(), 1L), as = fixed_size_list_of(utf8(), 1L))
  expect_array_roundtrip(
    list(c("itsy", "bitsy"), c("spider", "is"), c(NA_character_, NA_character_), c("", "")),
    fixed_size_list_of(utf8(), 2L),
    as = fixed_size_list_of(utf8(), 2L)
  )

  # factor
  expect_array_roundtrip(
    list(factor(c("b", "a"), levels = c("a", "b"))),
    fixed_size_list_of(dictionary(int8(), utf8()), 2L),
    as = fixed_size_list_of(dictionary(int8(), utf8()), 2L)
  )

  # struct
  expect_array_roundtrip(
    list(tibble::tibble(a = 1L, b = 1L, c = "", d = TRUE)),
    fixed_size_list_of(struct(a = int32(), b = int32(), c = utf8(), d = bool()), 1L),
    as = fixed_size_list_of(struct(a = int32(), b = int32(), c = utf8(), d = bool()), 1L)
  )
  expect_array_roundtrip(
    list(tibble::tibble(a = list(1L))),
    fixed_size_list_of(struct(a = list_of(int32())), 1L),
    as = fixed_size_list_of(struct(a = list_of(int32())), 1L)
  )
  expect_array_roundtrip(
    list(tibble::tibble(a = list(1L))),
    list_of(struct(a = fixed_size_list_of(int32(), 1L))),
    as = list_of(struct(a = fixed_size_list_of(int32(), 1L)))
  )
})

test_that("Handling string data with embedded nuls", {
  raws <- structure(
    list(
      as.raw(c(0x70, 0x65, 0x72, 0x73, 0x6f, 0x6e)),
      as.raw(c(0x77, 0x6f, 0x6d, 0x61, 0x6e)),
      as.raw(c(0x6d, 0x61, 0x00, 0x6e)), # <-- there's your nul, 0x00
      as.raw(c(0x66, 0x00, 0x00, 0x61, 0x00, 0x6e)), # multiple nuls
      as.raw(c(0x63, 0x61, 0x6d, 0x65, 0x72, 0x61)),
      as.raw(c(0x74, 0x76))
    ),
    class = c("arrow_binary", "vctrs_vctr", "list")
  )
  expect_error(
    rawToChar(raws[[3]]),
    "embedded nul in string: 'ma\\0n'", # See?
    fixed = TRUE
  )
  array_with_nul <- Array$create(raws)$cast(utf8())

  # The behavior of the warnings/errors is slightly different with and without
  # altrep. Without it (i.e. 3.5.0 and below, the error would trigger immediately
  # on `as.vector()` where as with it, the error only happens on materialization)
  skip_on_r_older_than("3.6")

  # no error on conversion, because altrep laziness
  v <- expect_error(as.vector(array_with_nul), NA)

  # attempting materialization -> error

  expect_error(v[],
    paste0(
      "embedded nul in string: 'ma\\0n'; to strip nuls when converting from Arrow ",
      "to R, set options(arrow.skip_nul = TRUE)"
    ),
    fixed = TRUE
  )

  # also error on materializing v[3]
  expect_error(v[3],
    paste0(
      "embedded nul in string: 'ma\\0n'; to strip nuls when converting from Arrow ",
      "to R, set options(arrow.skip_nul = TRUE)"
    ),
    fixed = TRUE
  )

  withr::with_options(list(arrow.skip_nul = TRUE), {
    # no warning yet because altrep laziness
    v <- as.vector(array_with_nul)

    expect_warning(
      expect_identical(
        v[],
        c("person", "woman", "man", "fan", "camera", "tv")
      ),
      "Stripping '\\0' (nul) from character vector",
      fixed = TRUE
    )

    v <- as.vector(array_with_nul)
    expect_warning(
      expect_identical(v[3], "man"),
      "Stripping '\\0' (nul) from character vector",
      fixed = TRUE
    )

    v <- as.vector(array_with_nul)
    expect_warning(
      expect_identical(v[4], "fan"),
      "Stripping '\\0' (nul) from character vector",
      fixed = TRUE
    )
  })
})

test_that("Array$create() should have helpful error", {
  expect_error(Array$create(list(numeric(0)), list_of(bool())), "Expecting a logical vector")

  lgl <- logical(0)
  int <- integer(0)
  num <- numeric(0)
  char <- character(0)
  expect_error(Array$create(list(lgl, lgl, int)), "Expecting a logical vector")
  expect_error(Array$create(list(char, num, char)), "Expecting a character vector")

  a <- expect_error(Array$create("one", int32()))
  b <- expect_error(vec_to_Array("one", int32()))
  # the captured conditions (errors) are not identical, but their messages should be
  expect_s3_class(a, "rlang_error")
  expect_s3_class(b, "simpleError")
  expect_equal(a$message, b$message, ignore_attr = TRUE)
})

test_that("Array$View() (ARROW-6542)", {
  a <- Array$create(1:3)
  b <- a$View(float32())
  expect_equal(b$type, float32())
  expect_equal(length(b), 3L)

  # Input validation
  expect_error(a$View("not a type"), "type must be a DataType, not character")
})

test_that("Array$Validate()", {
  a <- Array$create(1:10)
  expect_error(a$Validate(), NA)
})

test_that("is.Array", {
  a <- Array$create(1, type = int32())
  expect_true(is.Array(a))
  expect_true(is.Array(a, "int32"))
  expect_true(is.Array(a, c("int32", "int16")))
  expect_false(is.Array(a, "utf8"))
  expect_true(is.Array(a$View(float32())), "float32")
  expect_false(is.Array(1))
  expect_true(is.Array(ChunkedArray$create(1, 2)))
})

test_that("Array$Take()", {
  a <- Array$create(10:20)
  expect_as_vector(a$Take(c(4, 2)), c(14, 12))
})

test_that("[ method on Array", {
  vec <- 11:20
  a <- Array$create(vec)
  expect_as_vector(a[5:9], vec[5:9])
  expect_as_vector(a[c(9, 3, 5)], vec[c(9, 3, 5)])
  expect_as_vector(a[rep(c(TRUE, FALSE), 5)], vec[c(1, 3, 5, 7, 9)])
  expect_as_vector(a[rep(c(TRUE, FALSE, NA, FALSE, TRUE), 2)], c(11, NA, 15, 16, NA, 20))
  expect_as_vector(a[-4], vec[-4])
  expect_as_vector(a[-1], vec[-1])
})

test_that("[ accepts Arrays and otherwise handles bad input", {
  vec <- 11:20
  a <- Array$create(vec)
  ind <- c(9, 3, 5)
  expect_error(
    a[Array$create(ind)],
    "Cannot extract rows with an Array of type double"
  )
  expect_as_vector(a[Array$create(ind - 1, type = int8())], vec[ind])
  expect_as_vector(a[Array$create(ind - 1, type = uint8())], vec[ind])
  expect_as_vector(a[ChunkedArray$create(8, 2, 4, type = uint8())], vec[ind])

  filt <- seq_along(vec) %in% ind
  expect_as_vector(a[Array$create(filt)], vec[filt])

  expect_error(
    a["string"],
    "Cannot extract rows with an object of class character"
  )
})

test_that("%in% works on dictionary arrays", {
  a1 <- Array$create(as.factor(c("A", "B", "C")))
  a2 <- DictionaryArray$create(c(0L, 1L, 2L), c(4.5, 3.2, 1.1))
  c1 <- Array$create(c(FALSE, TRUE, FALSE))
  c2 <- Array$create(c(FALSE, FALSE, FALSE))
  b1 <- Array$create("B")
  b2 <- Array$create(5.4)

  expect_equal(is_in(a1, b1), c1)
  expect_equal(is_in(a2, b2), c2)
  expect_error(is_in(a1, b2))
})

test_that("[ accepts Expressions", {
  vec <- 11:20
  a <- Array$create(vec)
  b <- Array$create(1:10)
  expect_as_vector(a[b > 4], vec[5:10])
})

test_that("Array head/tail", {
  vec <- 11:20
  a <- Array$create(vec)
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

test_that("Dictionary array: create from arrays, not factor", {
  a <- DictionaryArray$create(c(2L, 1L, 1L, 2L, 0L), c(4.5, 3.2, 1.1))
  expect_equal(a$type, dictionary(int32(), float64()))
})

test_that("Dictionary array: translate to R when dict isn't string", {
  a <- DictionaryArray$create(c(2L, 1L, 1L, 2L, 0L), c(4.5, 3.2, 1.1))
  expect_warning(
    expect_identical(
      as.vector(a),
      factor(c(3, 2, 2, 3, 1), labels = c("4.5", "3.2", "1.1"))
    )
  )
})

test_that("Array$Equals", {
  vec <- 11:20
  a <- Array$create(vec)
  b <- Array$create(vec)
  d <- Array$create(3:4)
  expect_equal(a, b)
  expect_true(a$Equals(b))
  expect_false(a$Equals(vec))
  expect_false(a$Equals(d))
})

test_that("Array$ApproxEquals", {
  vec <- c(1.0000000000001, 2.400000000000001)
  a <- Array$create(vec)
  b <- Array$create(round(vec, 1))
  expect_false(a$Equals(b))
  expect_true(a$ApproxEquals(b))
  expect_false(a$ApproxEquals(vec))
})

test_that("auto int64 conversion to int can be disabled (ARROW-10093)", {
  withr::with_options(list(arrow.int64_downcast = FALSE), {
    a <- Array$create(1:10, int64())
    expect_true(inherits(a$as_vector(), "integer64"))

    batch <- RecordBatch$create(x = a)
    expect_true(inherits(as.data.frame(batch)$x, "integer64"))

    tab <- Table$create(x = a)
    expect_true(inherits(as.data.frame(batch)$x, "integer64"))
  })
})

test_that("as_arrow_array() default method calls Array$create()", {
  expect_equal(
    as_arrow_array(1:10),
    Array$create(1:10)
  )

  expect_equal(
    as_arrow_array(1:10, type = float64()),
    Array$create(1:10, type = float64())
  )
})

test_that("as_arrow_array respects `type` argument (ARROW-17620)", {
  df <- tibble::tibble(x = 1:10, y = 1:10)
  type <- struct(x = float64(), y = int16())
  a <- Array$create(df, type = type)

  expect_type_equal(a, as_arrow_array(df, type = type))
})

test_that("as_arrow_array() works for Array", {
  array <- Array$create(logical(), type = null())
  expect_identical(as_arrow_array(array), array)
  expect_equal(
    as_arrow_array(array, type = int32()),
    Array$create(integer())
  )
})

test_that("as_arrow_array() works for Array", {
  scalar <- Scalar$create(TRUE)
  expect_equal(as_arrow_array(scalar), Array$create(TRUE))
  expect_equal(
    as_arrow_array(scalar, type = int32()),
    Array$create(1L)
  )
})

test_that("as_arrow_array() works for ChunkedArray", {
  expect_equal(
    as_arrow_array(chunked_array(type = null())),
    Array$create(logical(), type = null())
  )

  expect_equal(
    as_arrow_array(chunked_array(1:3, 4:6)),
    Array$create(1:6)
  )

  expect_equal(
    as_arrow_array(chunked_array(1:3, 4:6), type = float64()),
    Array$create(1:6, type = float64())
  )
})

test_that("as_arrow_array() works for vctrs_vctr types", {
  vctr <- vctrs::new_vctr(1:5, class = "custom_vctr")
  expect_equal(
    as_arrow_array(vctr),
    vctrs_extension_array(vctr)
  )

  # with explicit type
  expect_equal(
    as_arrow_array(
      vctr,
      type = vctrs_extension_type(
        vctrs::vec_ptype(vctr),
        storage_type = float64()
      )
    ),
    vctrs_extension_array(
      vctr,
      storage_type = float64()
    )
  )

  # with impossible type
  expect_snapshot_error(as_arrow_array(vctr, type = float64()))
})

test_that("as_arrow_array() works for nested extension types", {
  vctr <- vctrs::new_vctr(1:5, class = "custom_vctr")

  nested <- tibble::tibble(x = vctr)
  type <- infer_type(nested)

  # with type = NULL
  nested_array <- as_arrow_array(nested)
  expect_identical(as.vector(nested_array), nested)

  # with explicit type
  expect_equal(as_arrow_array(nested, type = type), nested_array)

  # with extension type
  extension_array <- vctrs_extension_array(nested)
  expect_equal(
    as_arrow_array(nested, type = extension_array$type),
    extension_array
  )

  # with an extension type for the data.frame but no extension columns
  nested_plain <- tibble::tibble(x = 1:5)
  extension_array <- vctrs_extension_array(nested_plain)
  expect_equal(
    as_arrow_array(nested_plain, type = extension_array$type),
    extension_array
  )
})

test_that("Array$create() calls as_arrow_array() for nested extension types", {
  vctr <- vctrs::new_vctr(1:5, class = "custom_vctr")

  nested <- tibble::tibble(x = vctr)
  type <- infer_type(nested)

  # with type = NULL
  nested_array <- Array$create(nested)
  expect_identical(as.vector(nested_array), nested)

  # with explicit type
  expect_equal(Array$create(nested, type = type), nested_array)

  # with extension type
  extension_array <- vctrs_extension_array(nested)
  expect_equal(
    Array$create(nested, type = extension_array$type),
    extension_array
  )

  # with an extension type for the data.frame but no extension columns
  nested_plain <- tibble::tibble(x = 1:5)
  extension_array <- vctrs_extension_array(nested_plain)
  expect_equal(
    Array$create(nested_plain, type = extension_array$type),
    extension_array
  )
})

test_that("as_arrow_array() default method errors", {
  vec <- structure(list(), class = "class_not_supported")

  # check errors simulating a call from C++
  expect_snapshot_error(as_arrow_array(vec, from_vec_to_array = TRUE))
  expect_snapshot_error(
    as_arrow_array(vec, type = float64(), from_vec_to_array = TRUE)
  )

  # check errors actually coming through C++
  expect_snapshot_error(Array$create(vec, type = float64()))
  expect_snapshot_error(
    RecordBatch$create(col = vec, schema = schema(col = float64()))
  )
})

test_that("as_arrow_array() works for blob::blob()", {
  skip_if_not_installed("blob")

  # empty
  expect_r6_class(as_arrow_array(blob::blob()), "Array")
  expect_equal(
    as_arrow_array(blob::blob()),
    as_arrow_array(list(), type = binary())
  )

  # all null
  expect_equal(
    as_arrow_array(blob::blob(NULL, NULL)),
    as_arrow_array(list(NULL, NULL), type = binary())
  )

  expect_equal(
    as_arrow_array(blob::blob(as.raw(1:5), NULL)),
    as_arrow_array(list(as.raw(1:5), NULL), type = binary())
  )

  expect_equal(
    as_arrow_array(blob::blob(as.raw(1:5)), type = large_binary()),
    as_arrow_array(list(as.raw(1:5)), type = large_binary())
  )

  expect_snapshot_error(
    as_arrow_array(blob::blob(as.raw(1:5)), type = int32())
  )
})

test_that("as_arrow_array() works for vctrs::list_of()", {
  # empty
  expect_r6_class(as_arrow_array(vctrs::list_of(.ptype = integer())), "Array")
  expect_equal(
    as_arrow_array(vctrs::list_of(.ptype = integer())),
    as_arrow_array(list(), type = list_of(int32()))
  )

  # all NULL
  expect_equal(
    as_arrow_array(vctrs::list_of(NULL, NULL, .ptype = integer())),
    as_arrow_array(list(NULL, NULL), type = list_of(int32()))
  )

  expect_equal(
    as_arrow_array(vctrs::list_of(1:5, NULL, .ptype = integer())),
    as_arrow_array(list(1:5, NULL), type = list_of(int32()))
  )

  expect_equal(
    as_arrow_array(
      vctrs::list_of(1:5, .ptype = integer()),
      type = large_list_of(int32())
    ),
    as_arrow_array(list(1:5), type = large_list_of(int32()))
  )

  expect_snapshot_error(
    as_arrow_array(vctrs::list_of(1:5, .ptype = integer()), type = int32())
  )
})

test_that("concat_arrays works", {
  concat_empty <- concat_arrays()
  expect_true(concat_empty$type == null())
  expect_equal(concat_empty$length(), 0L)

  concat_empty_typed <- concat_arrays(type = int64())
  expect_true(concat_empty_typed$type == int64())
  expect_equal(concat_empty$length(), 0L)

  concat_int <- concat_arrays(Array$create(1:3), Array$create(4:5))
  expect_true(concat_int$type == int32())
  expect_true(all(concat_int == Array$create(1:5)))

  concat_int64 <- concat_arrays(
    Array$create(1:3),
    Array$create(4:5, type = int64()),
    type = int64()
  )
  expect_true(concat_int64$type == int64())
  expect_true(all(concat_int == Array$create(1:5)))

  expect_error(
    concat_arrays(
      Array$create(1:3),
      Array$create(4:5, type = int64())
    ),
    "must be identically typed"
  )
})

test_that("concat_arrays() coerces its input to Array", {
  concat_ints <- concat_arrays(1L, 2L)
  expect_true(concat_ints$type == int32())
  expect_true(all(concat_ints == Array$create(c(1L, 2L))))

  expect_error(
    concat_arrays(1L, "not a number", type = int32()),
    "cannot convert"
  )

  expect_error(
    concat_arrays(1L, "not a number"),
    "must be identically typed"
  )
})

test_that("Array doesn't support c()", {
  expect_snapshot_error(
    c(Array$create(1:2), Array$create(3:5))
  )
})

test_that("Array to C-interface", {
  # create a struct array since that's one of the more complicated array types
  df <- tibble::tibble(x = 1:10, y = x / 2, z = letters[1:10])
  arr <- Array$create(df)

  # export the array via the C-interface
  schema_ptr <- allocate_arrow_schema()
  array_ptr <- allocate_arrow_array()
  arr$export_to_c(array_ptr, schema_ptr)

  # then import it and check that the roundtripped value is the same
  circle <- Array$import_from_c(array_ptr, schema_ptr)
  expect_equal(arr, circle)

  # must clean up the pointers or we leak
  delete_arrow_schema(schema_ptr)
  delete_arrow_array(array_ptr)
})

test_that("Can convert R integer/double to decimal (ARROW-11631)", {
  # Check both decimal128 and decimal256
  decimal128_from_dbl <- Array$create(c(1, NA_real_), type = decimal128(12, 2))
  decimal256_from_dbl <- Array$create(c(1, NA_real_), type = decimal256(12, 2))
  decimal128_from_int <- Array$create(c(1L, NA_integer_), type = decimal128(12, 2))
  decimal256_from_int <- Array$create(c(1L, NA_integer_), type = decimal256(12, 2))

  # Check ALTREP input
  altrep_dbl <- as.vector(Array$create(c(1, NA_real_)))
  altrep_int <- as.vector(Array$create(c(1L, NA_integer_)))
  decimal_from_altrep_dbl <- Array$create(altrep_dbl, type = decimal128(12, 2))
  decimal_from_altrep_int <- Array$create(altrep_int, type = decimal128(12, 2))

  expect_equal(
    decimal128_from_dbl,
    Array$create(c(1, NA))$cast(decimal128(12, 2))
  )

  expect_equal(
    decimal256_from_dbl,
    Array$create(c(1, NA))$cast(decimal256(12, 2))
  )

  expect_equal(
    decimal128_from_int,
    Array$create(c(1, NA))$cast(decimal128(12, 2))
  )

  expect_equal(
    decimal256_from_int,
    Array$create(c(1, NA))$cast(decimal256(12, 2))
  )

  expect_equal(
    decimal_from_altrep_dbl,
    Array$create(c(1, NA))$cast(decimal128(12, 2))
  )

  expect_equal(
    decimal_from_altrep_int,
    Array$create(c(1, NA))$cast(decimal128(12, 2))
  )

  # Check that other types aren't silently but invalidly converted
  expect_error(
    Array$create(complex(), decimal128(12, 2)),
    "Conversion to decimal from non-integer/double"
  )
})
