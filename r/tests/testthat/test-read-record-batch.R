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


test_that("RecordBatchFileWriter / RecordBatchFileReader roundtrips", {
  tab <- Table$create(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10]
  )

  tf <- tempfile()
  expect_error(
    RecordBatchFileWriter$create(tf, tab$schema),
    "RecordBatchFileWriter$create() requires an Arrow InputStream. Try providing FileOutputStream$create(tf)",
    fixed = TRUE
  )

  stream <- FileOutputStream$create(tf)
  writer <- RecordBatchFileWriter$create(stream, tab$schema)
  expect_r6_class(writer, "RecordBatchWriter")
  writer$write_table(tab)
  writer$close()
  stream$close()

  expect_equal(read_feather(tf, as_data_frame = FALSE, mmap = FALSE), tab)
  # Make sure connections are closed
  expect_error(file.remove(tf), NA)
  skip_on_os("windows") # This should pass, we've closed the stream
  expect_false(file.exists(tf))
})

test_that("record_batch() handles (raw|Buffer|InputStream, Schema) (ARROW-3450, ARROW-3505)", {
  tbl <- tibble::tibble(
    int = 1:10, dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10]
  )
  batch <- record_batch(!!!tbl)
  schema <- batch$schema

  raw <- batch$serialize()
  batch2 <- record_batch(raw, schema = schema)
  batch3 <- record_batch(buffer(raw), schema = schema)
  stream <- BufferReader$create(raw)
  stream$close()

  expect_equal(batch, batch2)
  expect_equal(batch, batch3)
})

test_that("record_batch() can handle (Message, Schema) parameters (ARROW-3499)", {
  batch <- record_batch(x = 1:10)
  schema <- batch$schema

  raw <- batch$serialize()
  stream <- BufferReader$create(raw)

  message <- read_message(stream)
  batch2 <- record_batch(message, schema = schema)
  expect_equal(batch, batch2)
  stream$close()
})
