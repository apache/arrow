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


test_that("field() factory", {
  x <- field("x", int32())
  expect_equal(x$type, int32())
  expect_equal(x$name, "x")
  expect_true(x$nullable)
  expect_true(x == x)
  expect_false(x == field("x", int64()))
})

test_that("Field with nullable values", {
  x <- field("x", int32(), nullable = FALSE)
  expect_equal(x$type, int32())
  expect_false(x$nullable)
  expect_true(x == x)
  expect_false(x == field("x", int32()))
})

test_that("Field validation", {
  expect_error(schema(b = 32), "b must be a DataType, not numeric")
})

test_that("Print method for field", {
  expect_output(print(field("x", int32())), "Field\nx: int32")
  expect_output(
    print(field("zz", dictionary())),
    "Field\nzz: dictionary<values=string, indices=int32>"
  )

  expect_output(
    print(field("x", int32(), nullable = FALSE)),
    "Field\nx: int32 not null"
  )

})

test_that("Field to C-interface", {
  field <- field("x", time32("s"))

  # export the field via the C-interface
  ptr <- allocate_arrow_schema()
  field$export_to_c(ptr)

  # then import it and check that the roundtripped value is the same
  circle <- Field$import_from_c(ptr)
  expect_equal(circle, field)

  # must clean up the pointer or we leak
  delete_arrow_schema(ptr)
})
