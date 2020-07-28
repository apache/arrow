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

context("DataType")

test_that("null type works as expected",{
  x <- null()
  expect_equal(x$id, 0L)
  expect_equal(x$name, "null")
  expect_equal(x$ToString(), "null")
  expect_true(x == x)
  expect_false(x == int8())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
})

test_that("boolean type work as expected",{
  x <- boolean()
  expect_equal(x$id, Type$BOOL)
  expect_equal(x$name, "bool")
  expect_equal(x$ToString(), "bool")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 1L)
})

test_that("int types works as expected",{
  x <- uint8()
  expect_equal(x$id, Type$UINT8)
  expect_equal(x$name, "uint8")
  expect_equal(x$ToString(), "uint8")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 8L)

  x <- int8()
  expect_equal(x$id, Type$INT8)
  expect_equal(x$name, "int8")
  expect_equal(x$ToString(), "int8")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 8L)

  x <- uint16()
  expect_equal(x$id, Type$UINT16)
  expect_equal(x$name, "uint16")
  expect_equal(x$ToString(), "uint16")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 16L)

  x <- int16()
  expect_equal(x$id, Type$INT16)
  expect_equal(x$name, "int16")
  expect_equal(x$ToString(), "int16")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 16L)

  x <- uint32()
  expect_equal(x$id, Type$UINT32)
  expect_equal(x$name, "uint32")
  expect_equal(x$ToString(), "uint32")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 32L)

  x <- int32()
  expect_equal(x$id, Type$INT32)
  expect_equal(x$name, "int32")
  expect_equal(x$ToString(), "int32")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 32L)

  x <- uint64()
  expect_equal(x$id, Type$UINT64)
  expect_equal(x$name, "uint64")
  expect_equal(x$ToString(), "uint64")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 64L)

  x <- int64()
  expect_equal(x$id, Type$INT64)
  expect_equal(x$name, "int64")
  expect_equal(x$ToString(), "int64")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 64L)
})

test_that("float types work as expected",{
  x <- float16()
  expect_equal(x$id, Type$HALF_FLOAT)
  expect_equal(x$name, "halffloat")
  expect_equal(x$ToString(), "halffloat")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 16L)

  x <- float32()
  expect_equal(x$id, Type$FLOAT)
  expect_equal(x$name, "float")
  expect_equal(x$ToString(), "float")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 32L)

  x <- float64()
  expect_equal(x$id, Type$DOUBLE)
  expect_equal(x$name, "double")
  expect_equal(x$ToString(), "double")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 64L)
})

test_that("utf8 type works as expected",{
  x <- utf8()
  expect_equal(x$id, Type$STRING)
  expect_equal(x$name, "utf8")
  expect_equal(x$ToString(), "string")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
})

test_that("date types work as expected", {
  x <- date32()
  expect_equal(x$id, Type$DATE32)
  expect_equal(x$name, "date32")
  expect_equal(x$ToString(), "date32[day]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$unit(), unclass(DateUnit$DAY))

  x <- date64()
  expect_equal(x$id, Type$DATE64)
  expect_equal(x$name, "date64")
  expect_equal(x$ToString(), "date64[ms]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$unit(), unclass(DateUnit$MILLI))
})

test_that("timestamp type works as expected", {
  x <- timestamp(TimeUnit$SECOND)
  expect_equal(x$id, Type$TIMESTAMP)
  expect_equal(x$name, "timestamp")
  expect_equal(x$ToString(), "timestamp[s]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$timezone(), "")
  expect_equal(x$unit(), unclass(TimeUnit$SECOND))

  x <- timestamp(TimeUnit$MILLI)
  expect_equal(x$id, Type$TIMESTAMP)
  expect_equal(x$name, "timestamp")
  expect_equal(x$ToString(), "timestamp[ms]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$timezone(), "")
  expect_equal(x$unit(), unclass(TimeUnit$MILLI))

  x <- timestamp(TimeUnit$MICRO)
  expect_equal(x$id, Type$TIMESTAMP)
  expect_equal(x$name, "timestamp")
  expect_equal(x$ToString(), "timestamp[us]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$timezone(), "")
  expect_equal(x$unit(), unclass(TimeUnit$MICRO))

  x <- timestamp(TimeUnit$NANO)
  expect_equal(x$id, Type$TIMESTAMP)
  expect_equal(x$name, "timestamp")
  expect_equal(x$ToString(), "timestamp[ns]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$timezone(), "")
  expect_equal(x$unit(), unclass(TimeUnit$NANO))
})

test_that("timestamp with timezone", {
  expect_equal(timestamp(timezone = "EST")$ToString(), "timestamp[s, tz=EST]")
})

test_that("time32 types work as expected", {
  x <- time32(TimeUnit$SECOND)
  expect_equal(x$id, Type$TIME32)
  expect_equal(x$name, "time32")
  expect_equal(x$ToString(), "time32[s]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 32L)
  expect_equal(x$unit(), unclass(TimeUnit$SECOND))

  x <- time32(TimeUnit$MILLI)
  expect_equal(x$id, Type$TIME32)
  expect_equal(x$name, "time32")
  expect_equal(x$ToString(), "time32[ms]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 32L)
  expect_equal(x$unit(), unclass(TimeUnit$MILLI))
})

test_that("time64 types work as expected", {
  x <- time64(TimeUnit$MICRO)
  expect_equal(x$id, Type$TIME64)
  expect_equal(x$name, "time64")
  expect_equal(x$ToString(), "time64[us]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$unit(), unclass(TimeUnit$MICRO))

  x <- time64(TimeUnit$NANO)
  expect_equal(x$id, Type$TIME64)
  expect_equal(x$name, "time64")
  expect_equal(x$ToString(), "time64[ns]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 0L)
  expect_equal(x$children(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$unit(), unclass(TimeUnit$NANO))
})

test_that("time type unit validation", {
  expect_equal(time32(TimeUnit$SECOND), time32("s"))
  expect_equal(time32(TimeUnit$MILLI), time32("ms"))
  expect_equal(time32(), time32(TimeUnit$MILLI))
  expect_error(time32(4), '"unit" should be one of 1 or 0')
  expect_error(time32(NULL), '"unit" should be one of "ms" or "s"')
  expect_match_arg_error(time32("years"))

  expect_equal(time64(TimeUnit$NANO), time64("n"))
  expect_equal(time64(TimeUnit$MICRO), time64("us"))
  expect_equal(time64(), time64(TimeUnit$NANO))
  expect_error(time64(4), '"unit" should be one of 3 or 2')
  expect_error(time64(NULL), '"unit" should be one of "ns" or "us"')
  expect_match_arg_error(time64("years"))
})

test_that("timestamp type input validation", {
  expect_equal(timestamp("ms"), timestamp(TimeUnit$MILLI))
  expect_equal(timestamp(), timestamp(TimeUnit$SECOND))
  expect_error(
    timestamp(NULL),
    '"unit" should be one of "ns", "us", "ms", or "s"'
  )
  expect_error(
    timestamp(timezone = 1231231),
    "timezone is not a string"
  )
  expect_error(
    timestamp(timezone = c("not", "a", "timezone")),
    "timezone is not a string"
  )
})

test_that("list type works as expected", {
  x <- list_of(int32())
  expect_equal(x$id, Type$LIST)
  expect_equal(x$name, "list")
  expect_equal(x$ToString(), "list<item: int32>")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 1L)
  expect_equal(
    x$children(),
    list(field("item", int32()))
  )
  expect_equal(x$value_type, int32())
  expect_equal(x$value_field, field("item", int32()))
})

test_that("struct type works as expected", {
  x <- struct(x = int32(), y = boolean())
  expect_equal(x$id, Type$STRUCT)
  expect_equal(x$name, "struct")
  expect_equal(x$ToString(), "struct<x: int32, y: bool>")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_children(), 2L)
  expect_equal(
    x$children(),
    list(field("x", int32()), field("y", boolean()))
  )
  expect_equal(x$GetFieldIndex("x"), 0L)
  expect_equal(x$GetFieldIndex("y"), 1L)
  expect_equal(x$GetFieldIndex("z"), -1L)

  expect_equal(x$GetFieldByName("x"), field("x", int32()))
  expect_equal(x$GetFieldByName("y"), field("y", boolean()))
  expect_null(x$GetFieldByName("z"))
})

test_that("DictionaryType works as expected (ARROW-3355)", {
  d <- dictionary(int32(), utf8())
  expect_equal(d, d)
  expect_true(d == d)
  expect_false(d == int32())
  expect_equal(d$id, Type$DICTIONARY)
  expect_equal(d$bit_width, 32L)
  expect_equal(d$ToString(), "dictionary<values=string, indices=int32>")
  expect_equal(d$index_type, int32())
  expect_equal(d$value_type, utf8())
  ord <- dictionary(ordered = TRUE)
  expect_equal(ord$ToString(), "dictionary<values=string, indices=int32, ordered>")
})

test_that("DictionaryType validation", {
  expect_error(
    dictionary(utf8(), int32()),
    "Dictionary index type should be .*integer, got string"
  )
  expect_error(dictionary(4, utf8()), 'index_type must be a "DataType"')
  expect_error(dictionary(int8(), "strings"), 'value_type must be a "DataType"')
})

test_that("decimal type and validation", {
  expect_error(decimal())
  expect_error(decimal("four"), '"precision" must be an integer')
  expect_error(decimal(4))
  expect_error(decimal(4, "two"), '"scale" must be an integer')
  expect_error(decimal(NA, 2), '"precision" must be an integer')
  expect_error(decimal(0, 2), "Invalid: Decimal precision out of range: 0")
  expect_error(decimal(100, 2), "Invalid: Decimal precision out of range: 100")
  expect_error(decimal(4, NA), '"scale" must be an integer')

  expect_is(decimal(4, 2), "Decimal128Type")

})

test_that("Binary", {
  expect_is(binary(), "Binary")
  expect_equal(binary()$ToString(), "binary")
})

test_that("FixedSizeBinary", {
  expect_is(fixed_size_binary(4), "FixedSizeBinary")
  expect_equal(fixed_size_binary(4)$ToString(), "fixed_size_binary[4]")

  # input validation
  expect_error(fixed_size_binary(NA), "'byte_width' cannot be NA")
  expect_error(fixed_size_binary(-1), "'byte_width' must be > 0")
  expect_error(fixed_size_binary("four"), class = "Rcpp::not_compatible")
  expect_error(fixed_size_binary(c(2, 4)), class = "Rcpp::not_compatible")
})
