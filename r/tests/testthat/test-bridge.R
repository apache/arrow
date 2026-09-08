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

test_that("Pointer wrapper accepts external pointers", {
  ptr <- allocate_arrow_schema()
  exportable <- int32()
  exportable$export_to_c(ptr)

  # make sure exportable is released and deleted
  expect_equal(DataType$import_from_c(ptr), int32())
  delete_arrow_schema(ptr)
})

test_that("Pointer wrapper accepts double-casted pointers", {
  ptr <- allocate_arrow_schema()
  exportable <- int32()
  exportable$export_to_c(external_pointer_addr_double(ptr))

  # make sure exportable is released and deleted
  expect_equal(DataType$import_from_c(external_pointer_addr_double(ptr)), int32())
  delete_arrow_schema(ptr)
})

test_that("Pointer wrapper accepts integer64-casted pointers", {
  ptr <- allocate_arrow_schema()
  exportable <- int32()
  exportable$export_to_c(external_pointer_addr_integer64(ptr))

  # make sure exportable is released and deleted
  expect_equal(DataType$import_from_c(external_pointer_addr_integer64(ptr)), int32())
  delete_arrow_schema(ptr)
})

test_that("Pointer wrapper accepts raw representation of pointers", {
  ptr <- allocate_arrow_schema()
  exportable <- int32()
  exportable$export_to_c(external_pointer_addr_raw(ptr))

  # make sure exportable is released and deleted
  expect_equal(DataType$import_from_c(external_pointer_addr_raw(ptr)), int32())
  delete_arrow_schema(ptr)
})

test_that("Pointer wrapper accepts character representation of pointers", {
  ptr <- allocate_arrow_schema()
  exportable <- int32()
  exportable$export_to_c(external_pointer_addr_character(ptr))

  # make sure exportable is released and deleted
  expect_equal(DataType$import_from_c(external_pointer_addr_character(ptr)), int32())
  delete_arrow_schema(ptr)
})

test_that("Pointer wrapper errors for unknown object", {
  expect_error(
    DataType$import_from_c(integer()),
    "Can't convert input object to pointer"
  )

  expect_error(
    DataType$import_from_c(NA_character_),
    "Can't convert NA_character_ to pointer"
  )

  expect_error(
    DataType$import_from_c("this is not an integer"),
    "Can't parse 'this is not an integer'"
  )
})

test_that("the C Data Interface allocators are exported", {
  exported <- getNamespaceExports("arrow")
  for (f in c(
    "allocate_arrow_schema", "delete_arrow_schema",
    "allocate_arrow_array", "delete_arrow_array",
    "allocate_arrow_array_stream", "delete_arrow_array_stream"
  )) {
    expect_true(f %in% exported, label = paste(f, "is exported"))
  }
})

test_that("an Array round-trips through the exported C Data Interface functions", {
  array_ptr <- arrow::allocate_arrow_array()
  schema_ptr <- arrow::allocate_arrow_schema()
  on.exit({
    arrow::delete_arrow_array(array_ptr)
    arrow::delete_arrow_schema(schema_ptr)
  })
  a <- Array(c(1.5, NA, 3))
  a(array_ptr, schema_ptr)
  expect_equal(Array(array_ptr, schema_ptr), a)
})
