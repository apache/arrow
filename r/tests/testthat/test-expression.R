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


test_that("C++ expressions", {
  skip_if_not_available("dataset")
  f <- Expression$field_ref("f")
  expect_identical(f$field_name, "f")
  g <- Expression$field_ref("g")
  date <- Expression$scalar(as.Date("2020-01-15"))
  ts <- Expression$scalar(as.POSIXct("2020-01-17 11:11:11"))
  i64 <- Expression$scalar(bit64::as.integer64(42))
  time <- Expression$scalar(hms::hms(56, 34, 12))

  expect_r6_class(f == g, "Expression")
  expect_r6_class(f == 4, "Expression")
  expect_r6_class(f == "", "Expression")
  expect_r6_class(f == NULL, "Expression")
  expect_r6_class(f == date, "Expression")
  expect_r6_class(f == i64, "Expression")
  expect_r6_class(f == time, "Expression")
  # can't seem to make this work right now because of R Ops.method dispatch
  # expect_r6_class(f == as.Date("2020-01-15"), "Expression") # nolint
  expect_r6_class(f == ts, "Expression")
  expect_r6_class(f <= 2L, "Expression")
  expect_r6_class(f != FALSE, "Expression")
  expect_r6_class(f > 4, "Expression")
  expect_r6_class(f < 4 & f > 2, "Expression")
  expect_r6_class(f < 4 | f > 2, "Expression")
  expect_r6_class(!(f < 4), "Expression")
  expect_output(
    print(f > 4),
    "Expression\n(f > 4)",
    fixed = TRUE
  )
  expect_equal(
    f$type(schema(f = float64())),
    float64()
  )
  expect_equal(
    (f > 4)$type(schema(f = float64())),
    bool()
  )
  # Interprets that as a list type
  expect_r6_class(f == c(1L, 2L), "Expression")

  # Non-Expression inputs are wrapped in Expression$scalar()
  expect_equal(
    Expression$create("add", 1, 2),
    Expression$create("add", Expression$scalar(1), Expression$scalar(2))
  )
})

test_that("Field reference expression schemas and types", {
  x <- Expression$field_ref("x")

  # type() throws error when schema is NULL
  expect_error(x$type(), "schema")

  # type() returns type when schema is set
  x$schema <- Schema$create(x = int32())
  expect_equal(x$type(), int32())
})

test_that("Nested field refs", {
  x <- Expression$field_ref("x")
  nested <- x$y
  expect_r6_class(nested, "Expression")
  expect_r6_class(x[["y"]], "Expression")
  expect_r6_class(nested$z, "Expression")
  expect_error(Expression$scalar(42L)$y, "Cannot extract a field from an Expression of type int32")
})

test_that("Scalar expression schemas and types", {
  # type() works on scalars without setting the schema
  expect_equal(
    Expression$scalar("foo")$type(),
    arrow::string()
  )
  expect_equal(
    Expression$scalar(42L)$type(),
    int32()
  )
})

test_that("Expression schemas and types", {
  x <- Expression$field_ref("x")
  y <- Expression$field_ref("y")
  z <- Expression$scalar(42L)

  # type() throws error when both schemas are unset
  expect_error(
    Expression$create("add_checked", x, y)$type(),
    "schema"
  )

  # type() throws error when left schema is unset
  y$schema <- Schema$create(y = float64())
  expect_error(
    Expression$create("add_checked", x, y)$type(),
    "schema"
  )

  # type() throws error when right schema is unset
  x$schema <- Schema$create(x = int32())
  y$schema <- NULL
  expect_error(
    Expression$create("add_checked", x, y)$type(),
    "schema"
  )

  # type() returns type when both schemas are set
  y$schema <- Schema$create(y = float64())
  expect_equal(
    Expression$create("add_checked", x, y)$type(),
    float64()
  )

  # type() returns type when one arg has schema set and one is scalar
  expect_equal(
    Expression$create("add_checked", x, z)$type(),
    int32()
  )
})

test_that("Nested field ref types", {
  nested <- Expression$field_ref("x")$y
  schm <- schema(x = struct(y = int32(), z = double()))
  expect_equal(nested$type(schm), int32())
  # implicit casting and schema propagation
  x <- Expression$field_ref("x")
  x$schema <- schm
  expect_equal((x$y * 2)$type(), int32())
})

test_that("Nested field from a non-field-ref (struct_field kernel)", {
  x <- Expression$scalar(tibble::tibble(a = 1, b = "two"))
  expect_true(inherits(x$a, "Expression"))
  expect_equal(x$a$type(), float64())
  expect_error(x$c, "field 'c' not found in struct<a: double, b: string>")
})
