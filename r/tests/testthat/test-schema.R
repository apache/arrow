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


test_that("Alternate type names are supported", {
  expect_equal(
    schema(b = double(), c = bool(), d = string(), e = float(), f = halffloat()),
    schema(b = float64(), c = boolean(), d = utf8(), e = float32(), f = float16())
  )
  expect_equal(names(schema(b = double(), c = bool(), d = string())), c("b", "c", "d"))
})

test_that("Schema print method", {
  expect_output(
    print(schema(b = double(), c = bool(), d = string())),
    paste(
      "Schema",
      "b: double",
      "c: bool",
      "d: string",
      sep = "\n"
    ),
    fixed = TRUE
  )
})

test_that("Schema$code()", {
  expect_code_roundtrip(
    schema(a = int32(), b = struct(c = double(), d = utf8()), e = list_of(binary()))
  )

  skip_if(packageVersion("rlang") < 1)
  expect_error(
    eval(schema(x = int32(), y = DayTimeInterval__initialize())$code()),
    "Unsupported type"
  )
})

test_that("Schema with non-nullable fields", {
  expect_output(
    print(
      schema(
        field("b", double()),
        field("c", bool(), nullable = FALSE),
        field("d", string())
      )
    ),
    paste(
      "Schema",
      "b: double",
      "c: bool not null",
      "d: string",
      sep = "\n"
    ),
    fixed = TRUE
  )
})

test_that("Schema $GetFieldByName", {
  schm <- schema(b = double(), c = string())
  expect_equal(schm$GetFieldByName("b"), field("b", double()))
  expect_null(schm$GetFieldByName("f"))
  # TODO: schema(b = double(), b = string())$GetFieldByName("b") # nolint
  # also returns NULL and probably should error bc duplicated names
})

test_that("Schema extract (returns Field)", {
  # TODO: should this return a Field or the Type?
  # I think of Schema like list(name = type, name = type, ...)
  # but in practice it is more like list(list(name, type), list(name, type), ...)
  # -> Field names in a Schema may be duplicated
  # -> Fields may have metadata (though we don't really handle that in R)
  schm <- schema(b = double(), c = string())
  expect_equal(schm$b, field("b", double()))
  expect_equal(schm[["b"]], field("b", double()))
  expect_equal(schm[[1]], field("b", double()))

  expect_null(schm[["ZZZ"]])
  expect_error(schm[[42]]) # Should have better error message
})

test_that("Schema slicing", {
  schm <- schema(b = double(), c = string(), d = int8())
  expect_equal(schm[2:3], schema(c = string(), d = int8()))
  expect_equal(schm[-1], schema(c = string(), d = int8()))
  expect_equal(schm[c("d", "c")], schema(d = int8(), c = string()))
  expect_equal(schm[c(FALSE, TRUE, TRUE)], schema(c = string(), d = int8()))
  expect_error(schm[c("c", "ZZZ")], 'Invalid field name: "ZZZ"')
  expect_error(schm[c("XXX", "c", "ZZZ")], 'Invalid field names: "XXX" and "ZZZ"')
})

test_that("Schema modification", {
  schm <- schema(b = double(), c = string(), d = int8())
  schm$c <- boolean()
  expect_equal(schm, schema(b = double(), c = boolean(), d = int8()))
  schm[["d"]] <- int16()
  expect_equal(schm, schema(b = double(), c = boolean(), d = int16()))
  schm$b <- NULL
  expect_equal(schm, schema(c = boolean(), d = int16()))
  # NULL assigning something that doesn't exist doesn't modify
  schm$zzzz <- NULL
  expect_equal(schm, schema(c = boolean(), d = int16()))
  # Adding a field
  schm$fff <- int32()
  expect_equal(schm, schema(c = boolean(), d = int16(), fff = int32()))

  # By index
  schm <- schema(b = double(), c = string(), d = int8())
  schm[[2]] <- int32()
  expect_equal(schm, schema(b = double(), c = int32(), d = int8()))

  # Adding actual Fields
  # If assigning by name, note that this can modify the resulting name
  schm <- schema(b = double(), c = string(), d = int8())
  schm$c <- field("x", int32())
  expect_equal(schm, schema(b = double(), x = int32(), d = int8()))
  schm[[2]] <- field("y", int64())
  expect_equal(schm, schema(b = double(), y = int64(), d = int8()))

  # Error handling
  expect_error(schm$c <- 4, "value must be a DataType")
  expect_error(schm[[-3]] <- int32(), "i not greater than 0")
  expect_error(schm[[0]] <- int32(), "i not greater than 0")
  expect_error(schm[[NA_integer_]] <- int32(), "!is.na(i) is not TRUE", fixed = TRUE)
  expect_error(schm[[TRUE]] <- int32(), "i is not a numeric or integer vector")
  expect_error(schm[[c(2, 4)]] <- int32(), "length(i) not equal to 1", fixed = TRUE)
})

test_that("Metadata is preserved when modifying Schema", {
  schm <- schema(b = double(), c = string(), d = int8())
  schm$metadata$foo <- "bar"
  expect_identical(schm$metadata, list(foo = "bar"))
  schm$c <- field("x", int32())
  expect_identical(schm$metadata, list(foo = "bar"))
})

test_that("reading schema from Buffer", {
  # TODO: this uses the streaming format, i.e. from RecordBatchStreamWriter
  #       maybe there is an easier way to serialize a schema
  batch <- record_batch(x = 1:10)
  expect_r6_class(batch, "RecordBatch")

  stream <- BufferOutputStream$create()
  writer <- RecordBatchStreamWriter$create(stream, batch$schema)
  expect_r6_class(writer, "RecordBatchWriter")
  writer$close()

  buffer <- stream$finish()
  expect_r6_class(buffer, "Buffer")

  reader <- MessageReader$create(buffer)
  expect_r6_class(reader, "MessageReader")

  message <- reader$ReadNextMessage()
  expect_r6_class(message, "Message")
  expect_equal(message$type, MessageType$SCHEMA)

  stream <- BufferReader$create(buffer)
  expect_r6_class(stream, "BufferReader")
  message <- read_message(stream)
  expect_r6_class(message, "Message")
  expect_equal(message$type, MessageType$SCHEMA)
})

test_that("Input validation when creating a table with a schema", {
  expect_error(
    Table$create(b = 1, schema = c(b = float64())), # list not Schema
    "`schema` must be an arrow::Schema or NULL"
  )
})

test_that("Schema$Equals", {
  a <- schema(b = double(), c = bool())
  b <- a$WithMetadata(list(some = "metadata"))

  # different metadata
  expect_failure(expect_equal(a, b))
  expect_false(a$Equals(b, check_metadata = TRUE))

  # Metadata not checked
  expect_equal(a, b, ignore_attr = TRUE)

  # Non-schema object
  expect_false(a$Equals(42))
})

test_that("unify_schemas", {
  a <- schema(b = double(), c = bool())
  z <- schema(b = double(), k = utf8())
  expect_equal(
    unify_schemas(a, z),
    schema(b = double(), c = bool(), k = utf8())
  )
  # returns NULL when any arg is NULL
  expect_null(
    unify_schemas(a, NULL, z)
  )
  # returns NULL when all args are NULL
  expect_null(
    unify_schemas(NULL, NULL)
  )
  # errors when no args
  expect_error(
    unify_schemas(),
    "Must provide at least one schema to unify"
  )
})

test_that("Schema to C-interface", {
  schema <- schema(b = double(), c = bool())

  # export the schema via the C-interface
  ptr <- allocate_arrow_schema()
  schema$export_to_c(ptr)

  # then import it and check that the roundtripped value is the same
  circle <- Schema$import_from_c(ptr)
  expect_equal(circle, schema)

  # must clean up the pointer or we leak
  delete_arrow_schema(ptr)
})

test_that("Schemas from lists", {
  name_list_schema <- schema(list(b = double(), c = string(), d = int8()))


  field_list_schema <- schema(
    list(
      field("b", double()),
      field("c", bool()),
      field("d", string())
    )
  )

  expect_equal(name_list_schema, schema(b = double(), c = string(), d = int8()))
  expect_equal(field_list_schema, schema(b = double(), c = bool(), d = string()))
})

test_that("as_schema() works for Schema objects", {
  schema <- schema(col1 = int32())
  expect_identical(as_schema(schema), schema)
})

test_that("as_schema() works for StructType objects", {
  struct_type <- struct(col1 = int32())
  expect_equal(as_schema(struct_type), schema(col1 = int32()))
})

test_that("schema name assignment", {
  schm <- schema(x = int8(), y = string(), z = double())
  expect_identical(names(schm), c("x", "y", "z"))
  names(schm) <- c("a", "b", "c")
  expect_identical(names(schm), c("a", "b", "c"))
  expect_error(names(schm) <- "f", regexp = "Replacement names must contain same number of items as current names")
  expect_error(names(schm) <- NULL, regexp = "Replacement names must be character vector, not NULL")

  # Test that R metadata is updated appropriately
  df <- data.frame(x = 1:3, y = c("a", "b", "c"))
  schm2 <- arrow_table(df)$schema
  names(schm2) <- c("col1", "col2")
  expect_identical(names(schm2), c("col1", "col2"))
  expect_identical(names(schm2$r_metadata$columns), c("col1", "col2"))
})
