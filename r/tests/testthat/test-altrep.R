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

skip_if_r_version("3.5.0")

test_that("is_arrow_altrep() does not include base altrep", {
  expect_false(is_arrow_altrep(1:10))
})

test_that("altrep vectors from int32 and dbl arrays with no nulls", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  v_int <- Array$create(1:1000)
  v_dbl <- Array$create(as.numeric(1:1000))
  c_int <- ChunkedArray$create(1:1000)
  c_dbl <- ChunkedArray$create(as.numeric(1:1000))

  expect_true(is_arrow_altrep(as.vector(v_int)))
  expect_true(is_arrow_altrep(as.vector(v_int$Slice(1))))
  expect_true(is_arrow_altrep(as.vector(v_dbl)))
  expect_true(is_arrow_altrep(as.vector(v_dbl$Slice(1))))

  expect_equal(c_int$num_chunks, 1L)
  expect_true(is_arrow_altrep(as.vector(c_int)))
  expect_true(is_arrow_altrep(as.vector(c_int$Slice(1))))

  expect_equal(c_dbl$num_chunks, 1L)
  expect_true(is_arrow_altrep(as.vector(c_dbl)))
  expect_true(is_arrow_altrep(as.vector(c_dbl$Slice(1))))

  withr::local_options(list(arrow.use_altrep = NULL))
  expect_true(is_arrow_altrep(as.vector(v_int)))
  expect_true(is_arrow_altrep(as.vector(v_int$Slice(1))))
  expect_true(is_arrow_altrep(as.vector(v_dbl)))
  expect_true(is_arrow_altrep(as.vector(v_dbl$Slice(1))))

  withr::local_options(list(arrow.use_altrep = FALSE))
  expect_false(is_arrow_altrep(as.vector(v_int)))
  expect_false(is_arrow_altrep(as.vector(v_int$Slice(1))))
  expect_false(is_arrow_altrep(as.vector(v_dbl)))
  expect_false(is_arrow_altrep(as.vector(v_dbl$Slice(1))))
})

test_that("altrep vectors from int32 and dbl arrays with nulls", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  v_int <- Array$create(c(1L, NA, 3L))
  v_dbl <- Array$create(c(1, NA, 3))
  c_int <- ChunkedArray$create(c(1L, NA, 3L))
  c_dbl <- ChunkedArray$create(c(1, NA, 3))

  expect_true(is_arrow_altrep(as.vector(v_int)))
  expect_true(is_arrow_altrep(as.vector(v_int$Slice(1))))
  expect_true(is_arrow_altrep(as.vector(v_dbl)))
  expect_true(is_arrow_altrep(as.vector(v_dbl$Slice(1))))
  expect_true(is_arrow_altrep(as.vector(c_int)))
  expect_true(is_arrow_altrep(as.vector(c_int$Slice(1))))
  expect_true(is_arrow_altrep(as.vector(c_dbl)))
  expect_true(is_arrow_altrep(as.vector(c_dbl$Slice(1))))

  expect_true(is_arrow_altrep(as.vector(v_int$Slice(2))))
  expect_true(is_arrow_altrep(as.vector(v_dbl$Slice(2))))
  expect_true(is_arrow_altrep(as.vector(c_int$Slice(2))))
  expect_true(is_arrow_altrep(as.vector(c_dbl$Slice(2))))

  # chunked array with 2 chunks cannot be altrep
  c_int <- ChunkedArray$create(0L, c(1L, NA, 3L))
  c_dbl <- ChunkedArray$create(0, c(1, NA, 3))
  expect_equal(c_int$num_chunks, 2L)
  expect_equal(c_dbl$num_chunks, 2L)

  expect_true(is_arrow_altrep(as.vector(c_int)))
  expect_true(is_arrow_altrep(as.vector(c_dbl)))
  expect_true(is_arrow_altrep(as.vector(c_int$Slice(3))))
  expect_true(is_arrow_altrep(as.vector(c_dbl$Slice(3))))
})

test_that("empty vectors are not altrep", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  v_int <- Array$create(integer())
  v_dbl <- Array$create(numeric())
  v_str <- Array$create(character())

  expect_false(is_arrow_altrep(as.vector(v_int)))
  expect_false(is_arrow_altrep(as.vector(v_dbl)))
  expect_false(is_arrow_altrep(as.vector(v_str)))
})

test_that("ChunkedArray sith 0 chunks are not altrep", {
  z_int <- ChunkedArray$create(type = int32())
  z_dbl <- ChunkedArray$create(type = float64())
  z_str <- ChunkedArray$create(type = utf8())

  expect_false(is_arrow_altrep(as.vector(z_int)))
  expect_false(is_arrow_altrep(as.vector(z_dbl)))
  expect_false(is_arrow_altrep(as.vector(z_str)))
})

test_that("chunked array become altrep", {
  s1 <- c("un", "deux", NA)
  s2 <- c("quatre", "cinq")
  a <- Array$create(s1)
  v <- a$as_vector()
  expect_equal(v, s1)
  expect_true(is_arrow_altrep(v))

  ca <- ChunkedArray$create(s1, s2)
  cv <- ca$as_vector()
  expect_equal(cv, c(s1, s2))
  expect_true(is_arrow_altrep(cv))

  # chunked array with 2 chunks
  c_int <- ChunkedArray$create(0L, c(1L, NA, 3L))
  c_dbl <- ChunkedArray$create(0, c(1, NA, 3))
  expect_equal(c_int$num_chunks, 2L)
  expect_equal(c_dbl$num_chunks, 2L)

  expect_true(is_arrow_altrep(as.vector(c_int)))
  expect_true(is_arrow_altrep(as.vector(c_dbl)))
})


test_that("as.data.frame(<Table>, <RecordBatch>) can create altrep vectors", {
  withr::local_options(list(arrow.use_altrep = TRUE))

  table <- Table$create(int = c(1L, 2L, 3L), dbl = c(1, 2, 3), str = c("un", "deux", "trois"))
  df_table <- as.data.frame(table)
  expect_true(is_arrow_altrep(df_table$int))
  expect_true(is_arrow_altrep(df_table$dbl))
  expect_true(is_arrow_altrep(df_table$str))

  batch <- RecordBatch$create(int = c(1L, 2L, 3L), dbl = c(1, 2, 3), str = c("un", "deux", "trois"))
  df_batch <- as.data.frame(batch)
  expect_true(is_arrow_altrep(df_batch$int))
  expect_true(is_arrow_altrep(df_batch$dbl))
  expect_true(is_arrow_altrep(df_batch$str))
})

expect_altrep_roundtrip <- function(x, fn, ..., .expect_warning = NA) {
  alt <- Array$create(x)$as_vector()

  expect_true(is_arrow_altrep(alt))
  expect_warning(
    expect_identical(fn(x, ...), fn(alt, ...)), .expect_warning
  )
  expect_true(is_arrow_altrep(alt))

  alt2 <- ChunkedArray$create(x, x)$as_vector()
  expect_true(is_arrow_altrep(alt2))
  expect_warning(
    expect_identical(fn(c(x, x), ...), fn(alt2, ...)), .expect_warning
  )
  expect_true(is_arrow_altrep(alt2))
}

test_that("altrep min/max/sum identical to R versions for double", {
  x <- c(1, 2, 3)
  expect_altrep_roundtrip(x, min, na.rm = TRUE)
  expect_altrep_roundtrip(x, max, na.rm = TRUE)
  expect_altrep_roundtrip(x, sum, na.rm = TRUE)

  expect_altrep_roundtrip(x, min)
  expect_altrep_roundtrip(x, max)
  expect_altrep_roundtrip(x, sum)

  x <- c(1, 2, NA_real_)
  expect_altrep_roundtrip(x, min, na.rm = TRUE)
  expect_altrep_roundtrip(x, max, na.rm = TRUE)
  expect_altrep_roundtrip(x, sum, na.rm = TRUE)

  expect_altrep_roundtrip(x, min)
  expect_altrep_roundtrip(x, max)
  expect_altrep_roundtrip(x, sum)

  x <- rep(NA_real_, 3)
  expect_altrep_roundtrip(x, min, na.rm = TRUE, .expect_warning = "no non-missing arguments to min")
  expect_altrep_roundtrip(x, max, na.rm = TRUE, .expect_warning = "no non-missing arguments to max")
  expect_altrep_roundtrip(x, sum, na.rm = TRUE)

  expect_altrep_roundtrip(x, min)
  expect_altrep_roundtrip(x, max)
  expect_altrep_roundtrip(x, sum)
})

test_that("altrep min/max/sum identical to R versions for int", {
  x <- c(1L, 2L, 3L)
  expect_altrep_roundtrip(x, min, na.rm = TRUE)
  expect_altrep_roundtrip(x, max, na.rm = TRUE)
  expect_altrep_roundtrip(x, sum, na.rm = TRUE)

  expect_altrep_roundtrip(x, min)
  expect_altrep_roundtrip(x, max)
  expect_altrep_roundtrip(x, sum)

  x <- c(1L, 2L, NA_integer_)
  expect_altrep_roundtrip(x, min, na.rm = TRUE)
  expect_altrep_roundtrip(x, max, na.rm = TRUE)
  expect_altrep_roundtrip(x, sum, na.rm = TRUE)

  expect_altrep_roundtrip(x, min)
  expect_altrep_roundtrip(x, max)
  expect_altrep_roundtrip(x, sum)

  x <- rep(NA_integer_, 3)
  expect_altrep_roundtrip(x, min, na.rm = TRUE, .expect_warning = "no non-missing arguments to min")
  expect_altrep_roundtrip(x, max, na.rm = TRUE, .expect_warning = "no non-missing arguments to max")
  expect_altrep_roundtrip(x, sum, na.rm = TRUE)

  expect_altrep_roundtrip(x, min)
  expect_altrep_roundtrip(x, max)
  expect_altrep_roundtrip(x, sum)

  # sum(x) is INT_MIN -> convert to double.
  x <- as.integer(c(-2^31 + 1L, -1L))
  expect_altrep_roundtrip(x, sum)
})

test_that("altrep vectors handle serialization", {
  ints <- c(1L, 2L, NA_integer_)
  dbls <- c(1, 2, NA_real_)
  strs <- c("un", "deux", NA_character_)

  expect_identical(ints, unserialize(serialize(Array$create(ints)$as_vector(), NULL)))
  expect_identical(dbls, unserialize(serialize(Array$create(dbls)$as_vector(), NULL)))
  expect_identical(strs, unserialize(serialize(Array$create(strs)$as_vector(), NULL)))
  expect_identical(strs, unserialize(serialize(Array$create(strs, large_utf8())$as_vector(), NULL)))
})

test_that("altrep vectors handle coercion", {
  ints <- c(1L, 2L, NA_integer_)
  dbls <- c(1, 2, NA_real_)
  strs <- c("1", "2", NA_character_)

  expect_identical(ints, as.integer(Array$create(dbls)$as_vector()))
  expect_identical(ints, as.integer(Array$create(strs)$as_vector()))

  expect_identical(dbls, as.numeric(Array$create(ints)$as_vector()))
  expect_identical(dbls, as.numeric(Array$create(strs)$as_vector()))

  expect_identical(strs, as.character(Array$create(ints)$as_vector()))
  expect_identical(strs, as.character(Array$create(dbls)$as_vector()))
})

test_that("columns of struct types may be altrep", {
  numbers <- runif(10)
  st <- Array$create(data.frame(x = 1:10, y = numbers))
  df <- st$as_vector()

  expect_true(is_arrow_altrep(df$x))
  expect_true(is_arrow_altrep(df$y))

  expect_equal(df$x, 1:10)
  expect_equal(df$y, numbers)

  st <- ChunkedArray$create(
    data.frame(x = 1:10, y = numbers),
    data.frame(x = 1:10, y = numbers)
  )
  df <- st$as_vector()
  expect_true(is_arrow_altrep(df$x))
  expect_true(is_arrow_altrep(df$y))
  expect_equal(df$x, rep(1:10, 2))
  expect_equal(df$y, rep(numbers, 2))
})

test_that("Conversion from altrep R vector to Array uses the existing Array/ChunkedArray", {
  a_int <- Array$create(c(1L, 2L, 3L))
  b_int <- Array$create(a_int$as_vector())
  expect_true(a_int$Same(b_int))

  a_dbl <- Array$create(c(1, 2, 3))
  b_dbl <- Array$create(a_dbl$as_vector())
  expect_true(a_dbl$Same(b_dbl))

  a_str <- Array$create(c("un", "deux", "trois"))
  b_str <- Array$create(a_str$as_vector())
  expect_true(a_str$Same(b_str))

  ca_int <- ChunkedArray$create(c(1L, 2L, 3L), c(4L, 5L, 6L))
  cb_int <- ChunkedArray$create(ca_int$as_vector())
  expect_true(ca_int$chunk(0)$Same(cb_int$chunk(0)))
  expect_true(ca_int$chunk(1)$Same(cb_int$chunk(1)))

  ca_dbl <- ChunkedArray$create(c(1, 2, 3), c(4, 5, 6))
  cb_dbl <- ChunkedArray$create(ca_dbl$as_vector())
  expect_true(ca_dbl$chunk(0)$Same(cb_dbl$chunk(0)))
  expect_true(ca_dbl$chunk(1)$Same(cb_dbl$chunk(1)))

  ca_str <- ChunkedArray$create(c("un", "deux", "trois"), c("quatre", "cinq", "six"))
  cb_str <- ChunkedArray$create(ca_str$as_vector())
  expect_true(ca_str$chunk(0)$Same(cb_str$chunk(0)))
  expect_true(ca_str$chunk(1)$Same(cb_str$chunk(1)))
})

test_that("ChunkedArray$create(...) keeps Array even when from altrep vectors", {
  a <- ChunkedArray$create(c(1, 2, 3), c(4, 5, 6))
  b <- ChunkedArray$create(c(7, 8, 9))
  c <- Array$create(c(10, 11, 12))
  d <- Array$create(c(13, 14, 15))
  e <- ChunkedArray$create(c(16, 17), c(18, 19))

  x <- ChunkedArray$create(
    # converter to R vectors (with altrep but keeping underlying arrays)
    a$as_vector(), # 2 chunks
    b$as_vector(), # 1 chunk
    c$as_vector(), # 1 array

    # passed in directly
    d,
    e
  )

  expect_true(x$chunk(0)$Same(a$chunk(0)))
  expect_true(x$chunk(1)$Same(a$chunk(1)))
  expect_true(x$chunk(2)$Same(b$chunk(0)))
  expect_true(x$chunk(3)$Same(c))
  expect_true(x$chunk(4)$Same(d))
  expect_true(x$chunk(5)$Same(e$chunk(0)))
  expect_true(x$chunk(6)$Same(e$chunk(1)))
})

test_that("dictionaries chunked arrays are made altrep", {
  # without unification
  x <- ChunkedArray$create(
    factor(c("a", "b"), levels = letters[1:5]),
    factor(c("d", "c", "a", NA, "e"), levels = letters[1:5])
  )
  f <- x$as_vector()
  expect_true(is_arrow_altrep(f))
  expect_equal(levels(f), letters[1:5])
  expect_equal(as.integer(f), c(1L, 2L, 4L, 3L, 1L, NA_integer_, 5L))

  # with unification
  x <- ChunkedArray$create(
    factor(c("a", "b"), levels = c("a", "b")),
    factor(c("d", "c", "a", NA, "e"), levels = c("d", "c", "a", "e"))
  )
  f <- x$as_vector()
  expect_true(is_arrow_altrep(f))
  expect_equal(levels(f), c("a", "b", "d", "c", "e"))
  expect_equal(as.integer(f), c(1L, 2L, 3L, 4L, 1L, NA_integer_, 5L))
})


test_that("R checks for bounds", {
  v_int <- Array$create(c(1, 2, 3))$as_vector()
  v_dbl <- Array$create(c(1L, 2L, 3L))$as_vector()
  v_str <- Array$create(c("un", "deux", "trois"))$as_vector()

  expect_error(v_int[[5]], "subscript out of bounds")
  expect_error(v_dbl[[5]], "subscript out of bounds")
  expect_error(v_str[[5]], "subscript out of bounds")

  # excluded from the snapshot because something has changed in R at some point
  # not really worth investigating when/where
  # https://github.com/apache/arrow/runs/3870446814#step:17:38473
  expect_error(v_int[[-1]])
  expect_error(v_dbl[[-1]])
  expect_error(v_str[[-1]])
})

test_that("Operations on altrep R vectors don't modify the original", {
  a_int <- Array$create(c(1L, 2L, 3L))
  b_int <- a_int$as_vector()
  c_int <- -b_int
  expect_false(isTRUE(all.equal(b_int, c_int)))
})
