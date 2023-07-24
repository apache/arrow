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


test_that("We handle non-UTF strings", {
  x <- iconv("Veitingastaðir", to = "latin1")
  df <- tibble::tibble(
    chr = x,
    fct = as.factor(x)
  )
  names(df) <- iconv(paste(x, names(df), sep = "_"), to = "latin1")
  df_struct <- tibble::tibble(a = df)

  raw_schema <- list(utf8(), dictionary(int8(), utf8()))
  names(raw_schema) <- names(df)

  # Confirm setup
  expect_identical(Encoding(x), "latin1")
  expect_identical(Encoding(names(df)), c("latin1", "latin1"))
  expect_identical(Encoding(df[[1]]), "latin1")
  expect_identical(Encoding(levels(df[[2]])), "latin1")

  # Array
  expect_identical(as.vector(Array$create(x)), x)
  # struct
  expect_identical(as.vector(Array$create(df)), df)

  # ChunkedArray
  expect_identical(as.vector(ChunkedArray$create(x)), x)
  # struct
  expect_identical(as.vector(ChunkedArray$create(df)), df)

  # Table (including field name)
  expect_equal_data_frame(Table$create(df), df)
  expect_equal_data_frame(Table$create(df_struct), df_struct)

  # RecordBatch
  expect_equal_data_frame(record_batch(df), df)
  expect_equal_data_frame(record_batch(df_struct), df_struct)

  # Schema field name
  df_schema <- schema(raw_schema)
  expect_identical(names(df_schema), names(df))

  df_struct_schema <- schema(a = do.call(struct, raw_schema))

  # Create table/batch with schema
  expect_equal_data_frame(Table$create(df, schema = df_schema), df)
  expect_equal_data_frame(Table$create(df_struct, schema = df_struct_schema), df_struct)
  expect_equal_data_frame(record_batch(df, schema = df_schema), df)
  expect_equal_data_frame(record_batch(df_struct, schema = df_struct_schema), df_struct)

  # Serialization
  feather_file <- tempfile()
  write_feather(df_struct, feather_file)
  expect_identical(read_feather(feather_file), df_struct)

  if (arrow_with_parquet()) {
    parquet_file <- tempfile()
    write_parquet(df, parquet_file) # Parquet doesn't yet support nested types
    expect_identical(read_parquet(parquet_file), df)
  }
})
