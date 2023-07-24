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

test_that("null type works as expected", {
  x <- null()
  expect_equal(x$id, 0L)
  expect_equal(x$name, "null")
  expect_equal(x$ToString(), "null")
  expect_true(x == x)
  expect_false(x == int8())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
})

test_that("boolean type work as expected", {
  x <- boolean()
  expect_equal(x$id, Type$BOOL)
  expect_equal(x$name, "bool")
  expect_equal(x$ToString(), "bool")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 1L)
})

test_that("int types works as expected", {
  x <- uint8()
  expect_equal(x$id, Type$UINT8)
  expect_equal(x$name, "uint8")
  expect_equal(x$ToString(), "uint8")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 8L)

  x <- int8()
  expect_equal(x$id, Type$INT8)
  expect_equal(x$name, "int8")
  expect_equal(x$ToString(), "int8")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 8L)

  x <- uint16()
  expect_equal(x$id, Type$UINT16)
  expect_equal(x$name, "uint16")
  expect_equal(x$ToString(), "uint16")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 16L)

  x <- int16()
  expect_equal(x$id, Type$INT16)
  expect_equal(x$name, "int16")
  expect_equal(x$ToString(), "int16")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 16L)

  x <- uint32()
  expect_equal(x$id, Type$UINT32)
  expect_equal(x$name, "uint32")
  expect_equal(x$ToString(), "uint32")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 32L)

  x <- int32()
  expect_equal(x$id, Type$INT32)
  expect_equal(x$name, "int32")
  expect_equal(x$ToString(), "int32")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 32L)

  x <- uint64()
  expect_equal(x$id, Type$UINT64)
  expect_equal(x$name, "uint64")
  expect_equal(x$ToString(), "uint64")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 64L)

  x <- int64()
  expect_equal(x$id, Type$INT64)
  expect_equal(x$name, "int64")
  expect_equal(x$ToString(), "int64")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 64L)
})

test_that("float types work as expected", {
  x <- float16()
  expect_equal(x$id, Type$HALF_FLOAT)
  expect_equal(x$name, "halffloat")
  expect_equal(x$ToString(), "halffloat")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 16L)

  x <- float32()
  expect_equal(x$id, Type$FLOAT)
  expect_equal(x$name, "float")
  expect_equal(x$ToString(), "float")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 32L)

  x <- float64()
  expect_equal(x$id, Type$DOUBLE)
  expect_equal(x$name, "double")
  expect_equal(x$ToString(), "double")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 64L)
})

test_that("utf8 type works as expected", {
  x <- utf8()
  expect_equal(x$id, Type$STRING)
  expect_equal(x$name, "utf8")
  expect_equal(x$ToString(), "string")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
})

test_that("date types work as expected", {
  x <- date32()
  expect_equal(x$id, Type$DATE32)
  expect_equal(x$name, "date32")
  expect_equal(x$ToString(), "date32[day]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$unit(), unclass(DateUnit$DAY))

  x <- date64()
  expect_equal(x$id, Type$DATE64)
  expect_equal(x$name, "date64")
  expect_equal(x$ToString(), "date64[ms]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$unit(), unclass(DateUnit$MILLI))
})

test_that("timestamp type works as expected", {
  x <- timestamp(TimeUnit$SECOND)
  expect_equal(x$id, Type$TIMESTAMP)
  expect_equal(x$name, "timestamp")
  expect_equal(x$ToString(), "timestamp[s]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$timezone(), "")
  expect_equal(x$unit(), unclass(TimeUnit$SECOND))

  x <- timestamp(TimeUnit$MILLI)
  expect_equal(x$id, Type$TIMESTAMP)
  expect_equal(x$name, "timestamp")
  expect_equal(x$ToString(), "timestamp[ms]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$timezone(), "")
  expect_equal(x$unit(), unclass(TimeUnit$MILLI))

  x <- timestamp(TimeUnit$MICRO)
  expect_equal(x$id, Type$TIMESTAMP)
  expect_equal(x$name, "timestamp")
  expect_equal(x$ToString(), "timestamp[us]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$timezone(), "")
  expect_equal(x$unit(), unclass(TimeUnit$MICRO))

  x <- timestamp(TimeUnit$NANO)
  expect_equal(x$id, Type$TIMESTAMP)
  expect_equal(x$name, "timestamp")
  expect_equal(x$ToString(), "timestamp[ns]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
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
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 32L)
  expect_equal(x$unit(), unclass(TimeUnit$SECOND))

  x <- time32(TimeUnit$MILLI)
  expect_equal(x$id, Type$TIME32)
  expect_equal(x$name, "time32")
  expect_equal(x$ToString(), "time32[ms]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
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
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$unit(), unclass(TimeUnit$MICRO))

  x <- time64(TimeUnit$NANO)
  expect_equal(x$id, Type$TIME64)
  expect_equal(x$name, "time64")
  expect_equal(x$ToString(), "time64[ns]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$unit(), unclass(TimeUnit$NANO))
})

test_that("duration types work as expected", {
  x <- duration(TimeUnit$MICRO)
  expect_equal(x$id, Type$DURATION)
  expect_equal(x$name, "duration")
  expect_equal(x$ToString(), "duration[us]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$unit(), unclass(TimeUnit$MICRO))

  x <- duration(TimeUnit$SECOND)
  expect_equal(x$id, Type$DURATION)
  expect_equal(x$name, "duration")
  expect_equal(x$ToString(), "duration[s]")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 0L)
  expect_equal(x$fields(), list())
  expect_equal(x$bit_width, 64L)
  expect_equal(x$unit(), unclass(TimeUnit$SECOND))
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

  expect_equal(duration(TimeUnit$NANO), duration("n"))
  expect_equal(duration(TimeUnit$MICRO), duration("us"))
  expect_equal(duration(), duration(TimeUnit$SECOND))
  expect_error(duration(4), '"unit" should be one of 0, 1, 2, or 3')
  expect_error(duration(NULL), '"unit" should be one of "s", "ms", "us", or "ns"')
  expect_match_arg_error(duration("years"))
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
  expect_equal(x$num_fields, 1L)
  expect_equal(
    x$fields()[[1]],
    field("item", int32())
  )
  expect_equal(x$value_type, int32())
  expect_equal(x$value_field, field("item", int32()))

  # nullability matters in comparison
  expect_false(x$Equals(list_of(field("item", int32(), nullable = FALSE))))

  # field names don't matter by default
  other_name <- list_of(field("other", int32()))
  expect_equal(x, other_name, ignore_attr = TRUE)
  expect_false(x$Equals(other_name, check_metadata = TRUE))
})

test_that("map type works as expected", {
  x <- map_of(int32(), utf8())
  expect_equal(x$id, Type$MAP)
  expect_equal(x$name, "map")
  expect_equal(x$ToString(), "map<int32, string>")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(
    x$key_field,
    field("key", int32(), nullable = FALSE)
  )
  expect_equal(
    x$item_field,
    field("value", utf8())
  )
  expect_equal(x$key_type, int32())
  expect_equal(x$item_type, string())
  # TODO: (ARROW-15102): Enable constructing StructTypes with non-nullable fields, so
  # we can make this comparison:
  # expect_equal(x$value_type, struct(key = x$key_field, value = x$item_field)) # nolint
  expect_false(x$keys_sorted)

  # nullability matters in comparison
  expect_false(x$Equals(map_of(int32(), field("value", utf8(), nullable = FALSE))))

  # field names don't matter by default
  other_name <- map_of(int32(), field("other", utf8()))
  expect_equal(x, other_name, ignore_attr = TRUE)
  expect_false(x$Equals(other_name, check_metadata = TRUE))
})

test_that("map type validates arguments", {
  expect_error(
    map_of(field("key", int32(), nullable = TRUE), utf8()),
    "cannot be nullable"
  )
  expect_error(map_of(1L, utf8()), "must be a DataType or Field")
  expect_error(map_of(int32(), 1L), "must be a DataType or Field")

  # field construction
  ty <- map_of(
    field("the_keys", int32(), nullable = FALSE),
    field("my_values", utf8(), nullable = FALSE)
  )
  expect_equal(ty$key_field$name, "the_keys")
  expect_equal(ty$item_field$name, "my_values")
  expect_equal(ty$key_field$nullable, FALSE)
  expect_equal(ty$item_field$nullable, FALSE)
})

test_that("struct type works as expected", {
  x <- struct(x = int32(), y = boolean())
  expect_equal(x$id, Type$STRUCT)
  expect_equal(x$name, "struct")
  expect_equal(x$ToString(), "struct<x: int32, y: bool>")
  expect_true(x == x)
  expect_false(x == null())
  expect_equal(x$num_fields, 2L)
  expect_equal(
    x$fields()[[1]],
    field("x", int32())
  )
  expect_equal(
    x$fields()[[2]],
    field("y", boolean())
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
  expect_r6_class(decimal(4, 2), "Decimal128Type")
  expect_r6_class(decimal(39, 2), "Decimal256Type")

  expect_error(decimal("four"), "`precision` must be an integer")
  expect_error(decimal(4, "two"), "`scale` must be an integer")
  expect_error(decimal(NA, 2), "`precision` must be an integer")
  expect_error(decimal(4, NA), "`scale` must be an integer")
  # TODO remove precision range tests below once functionality is tested in C++ (ARROW-15162)
  expect_error(decimal(0, 2), "Invalid: Decimal precision out of range [1, 38]: 0", fixed = TRUE)
  expect_error(decimal(100, 2), "Invalid: Decimal precision out of range [1, 76]: 100", fixed = TRUE)

  # decimal() creates either decimal128 or decimal256 based on precision
  expect_identical(class(decimal(38, 2)), class(decimal128(38, 2)))
  expect_identical(class(decimal(39, 2)), class(decimal256(38, 2)))

  expect_r6_class(decimal128(4, 2), "Decimal128Type")

  expect_error(decimal128("four"), "`precision` must be an integer")
  expect_error(decimal128(4, "two"), "`scale` must be an integer")
  expect_error(decimal128(NA, 2), "`precision` must be an integer")
  expect_error(decimal128(4, NA), "`scale` must be an integer")
  expect_error(decimal128(3:4, NA), "`precision` must have size 1. not size 2")
  expect_error(decimal128(4, 2:3), "`scale` must have size 1. not size 2")
  # TODO remove precision range tests below once functionality is tested in C++ (ARROW-15162)
  expect_error(decimal128(0, 2), "Invalid: Decimal precision out of range [1, 38]: 0", fixed = TRUE)
  expect_error(decimal128(100, 2), "Invalid: Decimal precision out of range [1, 38]: 100", fixed = TRUE)


  expect_r6_class(decimal256(4, 2), "Decimal256Type")

  expect_error(decimal256("four"), "`precision` must be an integer")
  expect_error(decimal256(4, "two"), "`scale` must be an integer")
  expect_error(decimal256(NA, 2), "`precision` must be an integer")
  expect_error(decimal256(4, NA), "`scale` must be an integer")
  expect_error(decimal256(3:4, NA), "`precision` must have size 1. not size 2")
  expect_error(decimal256(4, 2:3), "`scale` must have size 1. not size 2")
  # TODO remove precision range tests below once functionality is tested in C++ (ARROW-15162)
  expect_error(decimal256(0, 2), "Invalid: Decimal precision out of range [1, 76]: 0", fixed = TRUE)
  expect_error(decimal256(100, 2), "Invalid: Decimal precision out of range [1, 76]: 100", fixed = TRUE)
})

test_that("Binary", {
  expect_r6_class(binary(), "Binary")
  expect_equal(binary()$ToString(), "binary")
})

test_that("FixedSizeBinary", {
  expect_r6_class(fixed_size_binary(4), "FixedSizeBinary")
  expect_equal(fixed_size_binary(4)$ToString(), "fixed_size_binary[4]")

  # input validation
  expect_error(fixed_size_binary(NA), "'byte_width' cannot be NA")
  expect_error(fixed_size_binary(-1), "'byte_width' must be > 0")
  expect_error(fixed_size_binary("four"))
  expect_error(fixed_size_binary(c(2, 4)))
})

test_that("DataType to C-interface", {
  datatype <- timestamp("ms", timezone = "Pacific/Marquesas")

  # export the datatype via the C-interface
  ptr <- allocate_arrow_schema()
  datatype$export_to_c(ptr)

  # then import it and check that the roundtripped value is the same
  circle <- DataType$import_from_c(ptr)
  expect_equal(circle, datatype)

  # must clean up the pointer or we leak
  delete_arrow_schema(ptr)
})

test_that("DataType$code()", {
  expect_code_roundtrip(int8())
  expect_code_roundtrip(uint8())
  expect_code_roundtrip(int16())
  expect_code_roundtrip(uint16())
  expect_code_roundtrip(int32())
  expect_code_roundtrip(uint32())
  expect_code_roundtrip(int64())
  expect_code_roundtrip(uint64())

  expect_code_roundtrip(float16())
  expect_code_roundtrip(float32())
  expect_code_roundtrip(float64())

  expect_code_roundtrip(null())

  expect_code_roundtrip(boolean())
  expect_code_roundtrip(utf8())
  expect_code_roundtrip(large_utf8())

  expect_code_roundtrip(binary())
  expect_code_roundtrip(large_binary())
  expect_code_roundtrip(fixed_size_binary(byte_width = 91))

  expect_code_roundtrip(date32())
  expect_code_roundtrip(date64())

  expect_code_roundtrip(time32())
  expect_code_roundtrip(time32(unit = "ms"))
  expect_code_roundtrip(time32(unit = "s"))

  expect_code_roundtrip(time64())
  expect_code_roundtrip(time64(unit = "ns"))
  expect_code_roundtrip(time64(unit = "us"))

  expect_code_roundtrip(timestamp())
  expect_code_roundtrip(timestamp(unit = "s"))
  expect_code_roundtrip(timestamp(unit = "ms"))
  expect_code_roundtrip(timestamp(unit = "ns"))
  expect_code_roundtrip(timestamp(unit = "us"))

  expect_code_roundtrip(timestamp(unit = "s", timezone = "CET"))
  expect_code_roundtrip(timestamp(unit = "ms", timezone = "CET"))
  expect_code_roundtrip(timestamp(unit = "ns", timezone = "CET"))
  expect_code_roundtrip(timestamp(unit = "us", timezone = "CET"))

  expect_code_roundtrip(decimal(precision = 3, scale = 5))

  expect_code_roundtrip(
    struct(a = int32(), b = struct(c = list_of(utf8())), d = float64())
  )

  expect_code_roundtrip(list_of(int32()))
  expect_code_roundtrip(large_list_of(int32()))
  expect_code_roundtrip(
    fixed_size_list_of(int32(), list_size = 7L)
  )

  expect_code_roundtrip(dictionary())
  expect_code_roundtrip(dictionary(index_type = int8()))
  expect_code_roundtrip(dictionary(index_type = int8(), value_type = large_utf8()))
  expect_code_roundtrip(dictionary(index_type = int8(), ordered = TRUE))

  skip_if(packageVersion("rlang") < 1)
  # Are these unsupported for a reason?
  expect_error(
    eval(DayTimeInterval__initialize()$code()),
    "Unsupported type"
  )
  expect_error(
    eval(struct(a = DayTimeInterval__initialize())$code()),
    "Unsupported type"
  )
})

test_that("as_data_type() works for DataType", {
  expect_equal(as_data_type(int32()), int32())
})

test_that("as_data_type() works for Field", {
  expect_equal(as_data_type(field("a field", int32())), int32())
})

test_that("as_data_type() works for Schema", {
  expect_equal(
    as_data_type(schema(col1 = int32(), col2 = string())),
    struct(col1 = int32(), col2 = string())
  )
})
