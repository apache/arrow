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

skip_on_r_older_than("3.6")

test_that("altrep test functions do not include base altrep", {
  expect_false(is_arrow_altrep(1:10))
  expect_identical(test_arrow_altrep_is_materialized(1:10), NA)
  expect_error(
    test_arrow_altrep_force_materialize(1:10),
    "is not arrow ALTREP"
  )
  expect_error(
    test_arrow_altrep_copy_by_element(1:10),
    "is not arrow ALTREP"
  )
  expect_error(
    test_arrow_altrep_copy_by_region(1:10, 1024),
    "is not arrow ALTREP"
  )
  expect_error(
    test_arrow_altrep_copy_by_dataptr(1:10),
    "is not arrow ALTREP"
  )
})

test_that(".Internal(inspect()) prints out Arrow altrep info", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  alt <- as.vector(Array$create(1:1000))

  expect_output(.Internal(inspect(alt)), "\\] arrow::array_int_vector")
  expect_true(test_arrow_altrep_force_materialize(alt))
  expect_output(.Internal(inspect(alt)), "materialized arrow::array_int_vector")
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

test_that("element access methods for int32 ALTREP with no nulls", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  original <- 1:1000
  v_int <- Array$create(original)
  altrep <- as.vector(v_int)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # altrep-aware iterating should not materialize
  expect_identical(test_arrow_altrep_copy_by_element(altrep), original)
  expect_identical(test_arrow_altrep_copy_by_region(altrep, 123), original)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # because there are no nulls, DATAPTR() does not materialize
  expect_identical(test_arrow_altrep_copy_by_dataptr(altrep), original)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # test element access after forcing materialization
  expect_true(test_arrow_altrep_force_materialize(altrep))
  expect_true(test_arrow_altrep_is_materialized(altrep))
  expect_identical(test_arrow_altrep_copy_by_element(altrep), original)
  expect_identical(test_arrow_altrep_copy_by_region(altrep, 123), original)
  expect_identical(test_arrow_altrep_copy_by_dataptr(altrep), original)
})

test_that("element access methods for double ALTREP with no nulls", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  original <- as.double(1:1000)
  v_dbl <- Array$create(original)
  altrep <- as.vector(v_dbl)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # altrep-aware iterating should not materialize
  expect_identical(test_arrow_altrep_copy_by_element(altrep), original)
  expect_identical(test_arrow_altrep_copy_by_region(altrep, 123), original)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # because there are no nulls, DATAPTR() does not materialize
  expect_identical(test_arrow_altrep_copy_by_dataptr(altrep), original)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # test element access after forcing materialization
  expect_true(test_arrow_altrep_force_materialize(altrep))
  expect_true(test_arrow_altrep_is_materialized(altrep))
  expect_identical(test_arrow_altrep_copy_by_element(altrep), original)
  expect_identical(test_arrow_altrep_copy_by_region(altrep, 123), original)
  expect_identical(test_arrow_altrep_copy_by_dataptr(altrep), original)
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

  c_int <- ChunkedArray$create(0L, c(1L, NA, 3L))
  c_dbl <- ChunkedArray$create(0, c(1, NA, 3))
  expect_equal(c_int$num_chunks, 2L)
  expect_equal(c_dbl$num_chunks, 2L)

  expect_true(is_arrow_altrep(as.vector(c_int)))
  expect_true(is_arrow_altrep(as.vector(c_dbl)))
  expect_true(is_arrow_altrep(as.vector(c_int$Slice(3))))
  expect_true(is_arrow_altrep(as.vector(c_dbl$Slice(3))))
})

test_that("element access methods for int32 ALTREP with nulls", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  original <- c(NA, 1:1000)
  v_int <- Array$create(original)
  altrep <- as.vector(v_int)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # altrep-aware iterating should not materialize
  expect_identical(test_arrow_altrep_copy_by_element(altrep), original)
  expect_identical(test_arrow_altrep_copy_by_region(altrep, 123), original)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # because there are no nulls, DATAPTR() does not materialize
  expect_identical(test_arrow_altrep_copy_by_dataptr(altrep), original)
  expect_true(test_arrow_altrep_is_materialized(altrep))

  # test element access after materialization
  expect_true(test_arrow_altrep_is_materialized(altrep))
  expect_identical(test_arrow_altrep_copy_by_element(altrep), original)
  expect_identical(test_arrow_altrep_copy_by_region(altrep, 123), original)
  expect_identical(test_arrow_altrep_copy_by_dataptr(altrep), original)
})

test_that("element access methods for double ALTREP with nulls", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  original <- as.double(c(NA, 1:1000))
  v_dbl <- Array$create(original)
  altrep <- as.vector(v_dbl)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # altrep-aware iterating should not materialize
  expect_identical(test_arrow_altrep_copy_by_element(altrep), original)
  expect_identical(test_arrow_altrep_copy_by_region(altrep, 123), original)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # because there are no nulls, DATAPTR() does not materialize
  expect_identical(test_arrow_altrep_copy_by_dataptr(altrep), original)
  expect_true(test_arrow_altrep_is_materialized(altrep))

  # test element access after materialization
  expect_true(test_arrow_altrep_is_materialized(altrep))
  expect_identical(test_arrow_altrep_copy_by_element(altrep), original)
  expect_identical(test_arrow_altrep_copy_by_region(altrep, 123), original)
  expect_identical(test_arrow_altrep_copy_by_dataptr(altrep), original)
})

test_that("altrep vectors from string arrays", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  v_chr <- Array$create(c("one", NA, "three"))
  c_chr <- ChunkedArray$create(c("one", NA, "three"))

  expect_true(is_arrow_altrep(as.vector(v_chr)))
  expect_true(is_arrow_altrep(as.vector(v_chr$Slice(1))))
  expect_true(is_arrow_altrep(as.vector(c_chr)))
  expect_true(is_arrow_altrep(as.vector(c_chr$Slice(1))))

  expect_true(is_arrow_altrep(as.vector(v_chr$Slice(2))))
  expect_true(is_arrow_altrep(as.vector(c_chr$Slice(2))))

  c_chr <- ChunkedArray$create("zero", c("one", NA, "three"))
  expect_equal(c_chr$num_chunks, 2L)

  expect_true(is_arrow_altrep(as.vector(c_chr)))
  expect_true(is_arrow_altrep(as.vector(c_chr$Slice(3))))
})

test_that("can't SET_STRING_ELT() on character ALTREP", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  alt <- as.vector(Array$create(c("one", "two", "three")))
  expect_error(
    test_arrow_altrep_set_string_elt(alt, 0, "value"),
    "are immutable"
  )
})

test_that("element access methods for character ALTREP", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  original <- as.character(c(NA, 1:1000))
  v_chr <- Array$create(original)
  altrep <- as.vector(v_chr)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # altrep-aware iterating should not materialize
  expect_identical(test_arrow_altrep_copy_by_element(altrep), original)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # DATAPTR() should always materialize for strings
  expect_identical(test_arrow_altrep_copy_by_dataptr(altrep), original)
  expect_true(test_arrow_altrep_is_materialized(altrep))

  # test element access after materialization
  expect_true(test_arrow_altrep_is_materialized(altrep))
  expect_identical(test_arrow_altrep_copy_by_element(altrep), original)
  expect_identical(test_arrow_altrep_copy_by_dataptr(altrep), original)
})

test_that("element access methods for character ALTREP from large_utf8()", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  original <- as.character(c(NA, 1:1000))
  v_chr <- Array$create(original, type = large_utf8())
  altrep <- as.vector(v_chr)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # altrep-aware iterating should not materialize
  expect_identical(test_arrow_altrep_copy_by_element(altrep), original)
  expect_false(test_arrow_altrep_is_materialized(altrep))

  # DATAPTR() should always materialize for strings
  expect_identical(test_arrow_altrep_copy_by_dataptr(altrep), original)
  expect_true(test_arrow_altrep_is_materialized(altrep))

  # test element access after materialization
  expect_true(test_arrow_altrep_is_materialized(altrep))
  expect_identical(test_arrow_altrep_copy_by_element(altrep), original)
  expect_identical(test_arrow_altrep_copy_by_dataptr(altrep), original)
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
  # check altrep Array
  alt <- Array$create(x)$as_vector()
  expect_true(is_arrow_altrep(alt))
  expect_warning(
    expect_identical(fn(alt, ...), suppressWarnings(fn(x, ...))), .expect_warning
  )
  expect_false(test_arrow_altrep_is_materialized(alt))

  # check altrep ChunkedArray
  alt2 <- ChunkedArray$create(x, x)$as_vector()
  expect_true(is_arrow_altrep(alt2))
  expect_warning(
    expect_identical(fn(alt2, ...), suppressWarnings(fn(c(x, x), ...))), .expect_warning
  )
  expect_false(test_arrow_altrep_is_materialized(alt2))

  # Check materialized altrep
  alt3 <- Array$create(x)$as_vector()
  expect_true(test_arrow_altrep_force_materialize(alt3))
  expect_warning(
    expect_identical(fn(alt3, ...), suppressWarnings(fn(x, ...))), .expect_warning
  )
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
  fctrs <- as.factor(strs)

  expect_identical(ints, unserialize(serialize(Array$create(ints)$as_vector(), NULL)))
  expect_identical(dbls, unserialize(serialize(Array$create(dbls)$as_vector(), NULL)))
  expect_identical(strs, unserialize(serialize(Array$create(strs)$as_vector(), NULL)))
  expect_identical(strs, unserialize(serialize(Array$create(strs, large_utf8())$as_vector(), NULL)))
  expect_identical(fctrs, unserialize(serialize(Array$create(fctrs)$as_vector(), NULL)))
})

test_that("altrep vectors handle coercion", {
  ints <- c(1L, 2L, NA_integer_)
  dbls <- c(1, 2, NA_real_)
  strs <- c("1", "2", NA_character_)
  fctrs <- as.factor(strs)

  expect_identical(ints, as.integer(Array$create(dbls)$as_vector()))
  expect_identical(ints, as.integer(Array$create(strs)$as_vector()))

  expect_identical(dbls, as.numeric(Array$create(ints)$as_vector()))
  expect_identical(dbls, as.numeric(Array$create(strs)$as_vector()))

  expect_identical(strs, as.character(Array$create(ints)$as_vector()))
  expect_identical(strs, as.character(Array$create(dbls)$as_vector()))

  expect_identical(fctrs, as.factor(Array$create(fctrs)$as_vector()))
  expect_identical(strs, as.character(Array$create(fctrs)$as_vector()))
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

test_that("element access methods for ALTREP factors", {
  index_types <- list(int8(), uint8(), int16(), uint16(), int32(), uint32())

  for (index_type in index_types) {
    # without unification
    int_indices <- c(1L, 2L, 4L, 3L, 1L, NA_integer_, 5L)
    x <- ChunkedArray$create(
      factor(c("a", "b"), levels = letters[1:5]),
      factor(c("d", "c", "a", NA, "e"), levels = letters[1:5]),
      type = dictionary(index_type, string())
    )
    f <- x$as_vector()
    expect_true(is_arrow_altrep(f))
    # This may fail interactively because str() currently
    # calls unclass(f), which calls our duplicate method
    expect_false(test_arrow_altrep_is_materialized(f))

    expect_identical(test_arrow_altrep_copy_by_element(f), int_indices)
    expect_identical(test_arrow_altrep_copy_by_region(f, 3), int_indices)
    expect_false(test_arrow_altrep_is_materialized(f))

    expect_identical(test_arrow_altrep_copy_by_dataptr(f), int_indices)
    expect_true(test_arrow_altrep_is_materialized(f))

    expect_identical(test_arrow_altrep_copy_by_element(f), int_indices)
    expect_identical(test_arrow_altrep_copy_by_region(f, 3), int_indices)

    # with unification
    int_indices <- c(1L, 2L, 3L, 4L, 1L, NA_integer_, 5L)
    x <- ChunkedArray$create(
      factor(c("a", "b"), levels = c("a", "b")),
      factor(c("d", "c", "a", NA, "e"), levels = c("d", "c", "a", "e")),
      type = dictionary(index_type, string())
    )
    f <- x$as_vector()
    expect_true(is_arrow_altrep(f))
    expect_false(test_arrow_altrep_is_materialized(f))

    expect_identical(test_arrow_altrep_copy_by_element(f), int_indices)
    expect_identical(test_arrow_altrep_copy_by_region(f, 3), int_indices)
    expect_false(test_arrow_altrep_is_materialized(f))

    expect_identical(test_arrow_altrep_copy_by_dataptr(f), int_indices)
    expect_true(test_arrow_altrep_is_materialized(f))

    expect_identical(test_arrow_altrep_copy_by_element(f), int_indices)
    expect_identical(test_arrow_altrep_copy_by_region(f, 3), int_indices)
  }
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

test_that("Materialized ALTREP arrays don't cause arrow to crash when attempting to bypass", {
  a_int <- Array$create(c(1L, 2L, 3L))
  b_int <- a_int$as_vector()
  expect_true(is_arrow_altrep(b_int))
  expect_false(test_arrow_altrep_is_materialized(b_int))

  # Some operations that use altrep bypass
  expect_equal(infer_type(b_int), int32())
  expect_equal(as_arrow_array(b_int), a_int)

  # Still shouldn't have materialized yet
  expect_false(test_arrow_altrep_is_materialized(b_int))

  # Force it to materialize and check again
  test_arrow_altrep_force_materialize(b_int)
  expect_true(test_arrow_altrep_is_materialized(b_int))
  expect_equal(infer_type(b_int), int32())
  expect_equal(as_arrow_array(b_int), a_int)
})
