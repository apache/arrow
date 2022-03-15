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

test_that("extension types can be created", {
  type <- MakeExtensionType(
    int32(),
    "arrow_r.simple_extension",
    charToRaw("some custom metadata"),
  )

  expect_r6_class(type, "ExtensionType")
  expect_identical(type$extension_name(), "arrow_r.simple_extension")
  expect_true(type$storage_type() == int32())
  expect_identical(type$storage_id(), int32()$id)
  expect_identical(type$Serialize(), charToRaw("some custom metadata"))
  expect_identical(type$ToString(), "ExtensionType <some custom metadata>")

  storage <- Array$create(1:10)
  array <- type$MakeArray(storage$data())
  expect_r6_class(array, "ExtensionArray")
  expect_r6_class(array$type, "ExtensionType")

  expect_true(array$type == type)
  expect_true(all(array$storage() == storage))
})

test_that("extension type subclasses work", {
  SomeExtensionTypeSubclass <- R6Class(
    "SomeExtensionTypeSubclass", inherit = ExtensionType,
    public = list(
      some_custom_method = function() {
        private$some_custom_field
      },

      .Deserialize = function(storage_type, extension_name, extension_metadata) {
        private$some_custom_field <- head(extension_metadata, 5)
      }
    ),
    private = list(
      some_custom_field = NULL
    )
  )

  SomeExtensionArraySubclass <- R6Class(
    "SomeExtensionArraySubclass", inherit = ExtensionArray,
    public = list(
      some_custom_method = function() {
        self$type$some_custom_method()
      }
    )
  )

  type <- MakeExtensionType(
    int32(),
    "some_extension_subclass",
    charToRaw("some custom metadata"),
    type_class = SomeExtensionTypeSubclass,
    array_class = SomeExtensionArraySubclass
  )

  expect_r6_class(type, "SomeExtensionTypeSubclass")
  expect_identical(type$some_custom_method(), charToRaw("some "))

  RegisterExtensionType(type)

  # create a new type instance with storage/metadata not identical
  # to the registered type
  type2 <- MakeExtensionType(
    float64(),
    "some_extension_subclass",
    charToRaw("some other custom metadata"),
    type_class = SomeExtensionTypeSubclass,
    array_class = SomeExtensionArraySubclass
  )

  ptr_type <- allocate_arrow_schema()
  type2$export_to_c(ptr_type)
  type3 <- DataType$import_from_c(ptr_type)
  delete_arrow_schema(ptr_type)

  expect_identical(type3$extension_name(), "some_extension_subclass")
  expect_identical(type3$some_custom_method(), type2$some_custom_method())
  expect_identical(type3$Serialize(), type2$Serialize())
  expect_true(type3$storage_type() == type2$storage_type())

  array <- type3$WrapArray(Array$create(1:10))
  expect_r6_class(array, "SomeExtensionArraySubclass")
  expect_identical(array$some_custom_method(), type3$some_custom_method())

  expect_identical(UnregisterExtensionType("some_extension_subclass"), type)
})

test_that("extension subclasses can override the ExtensionEquals method", {
  SomeExtensionTypeSubclass <- R6Class(
    "SomeExtensionTypeSubclass", inherit = ExtensionType,
    public = list(
      field_values = NULL,

      .Deserialize = function(storage_type, extension_name, extension_metadata) {
        self$field_values <- unserialize(extension_metadata)
      },

      .ExtensionEquals = function(other) {
        if (!inherits(other, "SomeExtensionTypeSubclass")) {
          return(FALSE)
        }

        setequal(names(other$field_values), names(self$field_values)) &&
          identical(
            other$field_values[names(self$field_values)],
            self$field_values
          )
      }
    )
  )

  type <- MakeExtensionType(
    int32(),
    "some_extension_subclass",
    serialize(list(field1 = "value1", field2 = "value2"), NULL),
    type_class = SomeExtensionTypeSubclass
  )

  RegisterExtensionType(type)

  expect_true(type$.ExtensionEquals(type))
  expect_true(type$Equals(type))

  type2 <- MakeExtensionType(
    int32(),
    "some_extension_subclass",
    serialize(list(field2 = "value2", field1 = "value1"), NULL),
    type_class = SomeExtensionTypeSubclass
  )

  expect_true(type$.ExtensionEquals(type2))
  expect_true(type$Equals(type2))

  UnregisterExtensionType("some_extension_subclass")
})

test_that("vctrs extension type works", {
  custom_vctr <- vctrs::new_vctr(
    1:4,
    attr_key = "attr_val",
    class = "arrow_custom_test"
  )

  type <- vctrs_extension_type(custom_vctr)
  expect_r6_class(type, "VctrsExtensionType")
  expect_identical(type$ptype(), vctrs::vec_ptype(custom_vctr))
  expect_true(type$Equals(type))
  expect_match(type$ToString(), "arrow_custom_test")

  array_in <- VctrsExtensionArray$create(custom_vctr)
  expect_true(array_in$type$Equals(type))
  expect_identical(VctrsExtensionArray$create(array_in), array_in)

  tf <- tempfile()
  on.exit(unlink(tf))
  write_feather(arrow_table(col = array_in), tf)
  table_out <- read_feather(tf, as_data_frame = FALSE)
  array_out <- table_out$col$chunk(0)

  expect_r6_class(array_out$type, "VctrsExtensionType")
  expect_r6_class(array_out, "VctrsExtensionArray")

  expect_true(array_out$type$Equals(type))
  expect_identical(
    array_out$as_vector(),
    custom_vctr
  )

  chunked_array_out <- table_out$col
  expect_true(chunked_array_out$type$Equals(type))
  expect_identical(
    chunked_array_out$as_vector(),
    custom_vctr
  )
})

