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
  type <- new_extension_type(
    int32(),
    "arrow_r.simple_extension",
    charToRaw("some custom metadata"),
  )

  expect_r6_class(type, "ExtensionType")
  expect_identical(type$extension_name(), "arrow_r.simple_extension")
  expect_true(type$storage_type() == int32())
  expect_identical(type$storage_id(), int32()$id)
  expect_identical(type$extension_metadata(), charToRaw("some custom metadata"))
  expect_identical(type$ToString(), "ExtensionType <some custom metadata>")

  storage <- Array$create(1:10)
  array <- type$WrapArray(storage)
  expect_r6_class(array, "ExtensionArray")
  expect_r6_class(array$type, "ExtensionType")

  expect_true(array$type == type)
  expect_equal(array$storage(), storage)

  expect_identical(array$as_vector(), 1:10)
  expect_identical(chunked_array(array)$as_vector(), 1:10)

  expect_snapshot_error(
    type$as_vector("not an extension array or chunked array")
  )
})

test_that("extension type subclasses work", {
  SomeExtensionTypeSubclass <- R6Class(
    "SomeExtensionTypeSubclass",
    inherit = ExtensionType,
    public = list(
      some_custom_method = function() {
        private$some_custom_field
      },
      deserialize_instance = function() {
        private$some_custom_field <- head(self$extension_metadata(), 5)
      }
    ),
    private = list(
      some_custom_field = NULL
    )
  )

  type <- new_extension_type(
    int32(),
    "some_extension_subclass",
    charToRaw("some custom metadata"),
    type_class = SomeExtensionTypeSubclass
  )

  expect_r6_class(type, "SomeExtensionTypeSubclass")
  expect_identical(type$some_custom_method(), charToRaw("some "))

  register_extension_type(type)

  # create a new type instance with storage/metadata not identical
  # to the registered type
  type2 <- new_extension_type(
    float64(),
    "some_extension_subclass",
    charToRaw("some other custom metadata"),
    type_class = SomeExtensionTypeSubclass
  )

  ptr_type <- allocate_arrow_schema()
  type2$export_to_c(ptr_type)
  type3 <- DataType$import_from_c(ptr_type)
  delete_arrow_schema(ptr_type)

  expect_identical(type3$extension_name(), "some_extension_subclass")
  expect_identical(type3$some_custom_method(), type2$some_custom_method())
  expect_identical(type3$extension_metadata(), type2$extension_metadata())
  expect_true(type3$storage_type() == type2$storage_type())

  array <- type3$WrapArray(Array$create(1:10))
  expect_r6_class(array, "ExtensionArray")

  unregister_extension_type("some_extension_subclass")
})

test_that("extension types can use UTF-8 for metadata", {
  type <- new_extension_type(
    int32(),
    "arrow.test.simple_extension",
    "\U0001f4a9\U0001f4a9\U0001f4a9\U0001f4a9"
  )

  expect_identical(
    type$extension_metadata_utf8(),
    "\U0001f4a9\U0001f4a9\U0001f4a9\U0001f4a9"
  )

  expect_match(type$ToString(), "\U0001f4a9", fixed = TRUE)
})

test_that("extension types can be printed that don't use UTF-8 for metadata", {
  type <- new_extension_type(
    int32(),
    "arrow.test.simple_extension",
    as.raw(0:5)
  )

  expect_match(type$ToString(), "00 01 02 03 04 05")
})

test_that("extension subclasses can override the ExtensionEquals method", {
  SomeExtensionTypeSubclass <- R6Class(
    "SomeExtensionTypeSubclass",
    inherit = ExtensionType,
    public = list(
      field_values = NULL,
      deserialize_instance = function() {
        self$field_values <- unserialize(self$extension_metadata())
      },
      ExtensionEquals = function(other) {
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

  type <- new_extension_type(
    int32(),
    "some_extension_subclass",
    serialize(list(field1 = "value1", field2 = "value2"), NULL),
    type_class = SomeExtensionTypeSubclass
  )

  register_extension_type(type)

  expect_true(type$ExtensionEquals(type))
  expect_true(type$Equals(type))

  type2 <- new_extension_type(
    int32(),
    "some_extension_subclass",
    serialize(list(field2 = "value2", field1 = "value1"), NULL),
    type_class = SomeExtensionTypeSubclass
  )

  expect_true(type$ExtensionEquals(type2))
  expect_true(type$Equals(type2))

  unregister_extension_type("some_extension_subclass")
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
  expect_identical(type$ToString(), "<arrow_custom_test[0]>")

  array_in <- vctrs_extension_array(custom_vctr)
  expect_true(array_in$type$Equals(type))
  expect_identical(vctrs_extension_array(array_in), array_in)

  tf <- tempfile()
  on.exit(unlink(tf))
  write_ipc_file(arrow_table(col = array_in), tf)
  table_out <- read_ipc_file(tf, as_data_frame = FALSE)
  array_out <- table_out$col$chunk(0)

  expect_r6_class(array_out$type, "VctrsExtensionType")
  expect_r6_class(array_out, "ExtensionArray")

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

  expect_snapshot_error(
    type$as_vector("not an extension array or chunked array")
  )
})

test_that("chunked arrays can roundtrip extension types", {
  custom_vctr1 <- vctrs::new_vctr(1:4, class = "arrow_custom_test")
  custom_vctr2 <- vctrs::new_vctr(5:8, class = "arrow_custom_test")
  custom_array1 <- vctrs_extension_array(custom_vctr1)
  custom_array2 <- vctrs_extension_array(custom_vctr2)

  custom_chunked <- chunked_array(custom_array1, custom_array2)
  expect_r6_class(custom_chunked$type, "VctrsExtensionType")
  expect_identical(
    custom_chunked$as_vector(),
    vctrs::new_vctr(1:8, class = "arrow_custom_test")
  )
})

test_that("RecordBatch can roundtrip extension types", {
  custom_vctr <- vctrs::new_vctr(1:8, class = "arrow_custom_test")
  custom_array <- vctrs_extension_array(custom_vctr)
  normal_vctr <- letters[1:8]

  custom_record_batch <- record_batch(custom = custom_array)
  expect_identical(
    custom_record_batch$to_data_frame(),
    tibble::tibble(
      custom = custom_vctr
    )
  )

  mixed_record_batch <- record_batch(
    custom = custom_array,
    normal = normal_vctr
  )
  expect_identical(
    mixed_record_batch$to_data_frame(),
    tibble::tibble(
      custom = custom_vctr,
      normal = normal_vctr
    )
  )

  # check both column orders, since column order should stay in the same
  # order whether the columns are extension types or not
  mixed_record_batch2 <- record_batch(
    normal = normal_vctr,
    custom = custom_array
  )
  expect_identical(
    mixed_record_batch2$to_data_frame(),
    tibble::tibble(
      normal = normal_vctr,
      custom = custom_vctr
    )
  )
})

test_that("Table can roundtrip extension types", {
  custom_vctr <- vctrs::new_vctr(1:8, class = "arrow_custom_test")
  custom_array <- vctrs_extension_array(custom_vctr)
  normal_vctr <- letters[1:8]

  custom_table <- arrow_table(custom = custom_array)
  expect_identical(
    custom_table$to_data_frame(),
    tibble::tibble(
      custom = custom_vctr
    )
  )

  mixed_table <- arrow_table(
    custom = custom_array,
    normal = normal_vctr
  )
  expect_identical(
    mixed_table$to_data_frame(),
    tibble::tibble(
      custom = custom_vctr,
      normal = normal_vctr
    )
  )

  # check both column orders, since column order should stay in the same
  # order whether the columns are extension types or not
  mixed_table2 <- arrow_table(
    normal = normal_vctr,
    custom = custom_array
  )
  expect_identical(
    mixed_table2$to_data_frame(),
    tibble::tibble(
      normal = normal_vctr,
      custom = custom_vctr
    )
  )
})

test_that("Dataset/arrow_dplyr_query can roundtrip extension types", {
  skip_if_not_available("dataset")
  skip_if_not(CanRunWithCapturedR())

  tf <- tempfile()
  on.exit(unlink(tf, recursive = TRUE))

  df <- expand.grid(
    number = 1:10,
    letter = letters,
    stringsAsFactors = FALSE,
    KEEP.OUT.ATTRS = FALSE
  ) |>
    tibble::as_tibble()

  df$extension <- vctrs::new_vctr(df$letter, class = "arrow_custom_vctr")

  table <- arrow_table(
    number = df$number,
    letter = df$letter,
    extension = vctrs_extension_array(df$extension)
  )

  table |>
    dplyr::group_by(number) |>
    write_dataset(tf)

  roundtripped <- open_dataset(tf) |>
    dplyr::select(number, letter, extension) |>
    dplyr::collect()

  expect_identical(unclass(roundtripped$extension), roundtripped$letter)
})

test_that("Handling vctrs_rcrd type", {
  df <- data.frame(
    x = vctrs::new_rcrd(fields = list(special = 1:3), class = "special")
  )
  tab <- arrow_table(df)
  expect_identical(as.data.frame(tab), df)
})

test_that("extension types defined outside of R can be used", {
  # arrow.uuid is a canonical extension type registered by libarrow itself,
  # so there is no R6 class registered for it from R (GH-53951). An IPC
  # round trip with the extension metadata on the field gives us an instance.
  storage <- Array$create(
    list(as.raw(1:16), NULL, as.raw(17:32)),
    type = fixed_size_binary(16)
  )
  uuid_field <- field(
    "x",
    fixed_size_binary(16),
    metadata = c("ARROW:extension:name" = "arrow.uuid")
  )
  uuid_schema <- schema(uuid_field)
  table <- read_ipc_stream(
    write_to_raw(Table$create(x = storage, schema = uuid_schema)),
    as_data_frame = FALSE
  )

  type <- table$x$type
  expect_r6_class(type, "ExtensionType")
  expect_identical(class(type)[1], "ExtensionType")
  expect_identical(type$extension_name(), "arrow.uuid")
  expect_true(type$storage_type() == fixed_size_binary(16))
  expect_identical(type$extension_metadata(), raw())
  expect_false(ExtensionType__is_r_type(type))
  expect_error(ExtensionType__r6_class(type), "was not created from R")

  # Printing uses libarrow's representation rather than the R one
  expect_identical(type$ToString(), "extension<arrow.uuid>")
  expect_output(print(table$x), "extension<arrow.uuid>")

  # Conversion to R falls back to converting the storage array
  expect_identical(as.vector(table$x), as.vector(storage))
  expect_identical(as.vector(table$x$chunk(0)), as.vector(storage))
  expect_identical(as.data.frame(table)$x, as.vector(storage))

  # ...including slices and empty results
  expect_identical(as.vector(table$x$Slice(1, 2)), as.vector(storage$Slice(1, 2)))
  expect_identical(as.data.frame(table$Slice(0, 0))$x, as.vector(storage)[0])

  # ...also when there is more than one chunk
  chunked <- concat_tables(table, table)
  expect_equal(chunked$x$num_chunks, 2L)
  expect_identical(as.vector(chunked$x), as.vector(concat_arrays(storage, storage)))
  expect_identical(as.data.frame(chunked)$x, as.vector(concat_arrays(storage, storage)))

  # ...and when the extension column is nested inside a struct
  nested_storage <- StructArray$create(x = storage)
  # struct() drops field metadata, so build the type from the Field directly
  nested_schema <- schema(field("s", struct__(list(uuid_field))))
  nested <- read_ipc_stream(
    write_to_raw(Table$create(s = nested_storage, schema = nested_schema)),
    as_data_frame = FALSE
  )
  expect_identical(
    nested$s$type$GetFieldByName("x")$type$extension_name(),
    "arrow.uuid"
  )
  expect_identical(as.data.frame(nested)$s$x, as.vector(storage))

  # ...and inside a list
  list_storage <- Array$create(
    list(list(as.raw(1:16), as.raw(17:32)), NULL, list(as.raw(17:32))),
    type = list_of(fixed_size_binary(16))
  )
  list_schema <- schema(field("l", list_of(uuid_field)))
  listed <- read_ipc_stream(
    write_to_raw(Table$create(l = list_storage, schema = list_schema)),
    as_data_frame = FALSE
  )
  expect_identical(
    listed$l$type$value_type$extension_name(),
    "arrow.uuid"
  )
  expect_identical(as.vector(listed$l), as.vector(list_storage))
})
