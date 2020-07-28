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

context("RecordBatch")

test_that("RecordBatch", {
  # Note that we're reusing `tbl` and `batch` throughout the tests in this file
  tbl <- tibble::tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10],
    fct = factor(letters[1:10])
  )
  batch <- record_batch(tbl)

  expect_equal(batch, batch)
  expect_equal(
    batch$schema,
    schema(
      int = int32(), dbl = float64(),
      lgl = boolean(), chr = utf8(),
      fct = dictionary(int8(), utf8())
    )
  )
  expect_equal(batch$num_columns, 5L)
  expect_equal(batch$num_rows, 10L)
  expect_equal(batch$column_name(0), "int")
  expect_equal(batch$column_name(1), "dbl")
  expect_equal(batch$column_name(2), "lgl")
  expect_equal(batch$column_name(3), "chr")
  expect_equal(batch$column_name(4), "fct")
  expect_equal(names(batch), c("int", "dbl", "lgl", "chr", "fct"))

  # input validation
  expect_error(batch$column_name(NA), "'i' cannot be NA")
  expect_error(batch$column_name(-1), "subscript out of bounds")
  expect_error(batch$column_name(1000), "subscript out of bounds")
  expect_error(batch$column_name(1:2), class = "Rcpp::not_compatible")
  expect_error(batch$column_name("one"), class = "Rcpp::not_compatible")

  col_int <- batch$column(0)
  expect_true(inherits(col_int, 'Array'))
  expect_equal(col_int$as_vector(), tbl$int)
  expect_equal(col_int$type, int32())

  col_dbl <- batch$column(1)
  expect_true(inherits(col_dbl, 'Array'))
  expect_equal(col_dbl$as_vector(), tbl$dbl)
  expect_equal(col_dbl$type, float64())

  col_lgl <- batch$column(2)
  expect_true(inherits(col_dbl, 'Array'))
  expect_equal(col_lgl$as_vector(), tbl$lgl)
  expect_equal(col_lgl$type, boolean())

  col_chr <- batch$column(3)
  expect_true(inherits(col_chr, 'Array'))
  expect_equal(col_chr$as_vector(), tbl$chr)
  expect_equal(col_chr$type, utf8())

  col_fct <- batch$column(4)
  expect_true(inherits(col_fct, 'Array'))
  expect_equal(col_fct$as_vector(), tbl$fct)
  expect_equal(col_fct$type, dictionary(int8(), utf8()))

  # input validation
  expect_error(batch$column(NA), "'i' cannot be NA")
  expect_error(batch$column(-1), "subscript out of bounds")
  expect_error(batch$column(1000), "subscript out of bounds")
  expect_error(batch$column(1:2), class = "Rcpp::not_compatible")
  expect_error(batch$column("one"), class = "Rcpp::not_compatible")

  batch2 <- batch$RemoveColumn(0)
  expect_equal(
    batch2$schema,
    schema(dbl = float64(), lgl = boolean(), chr = utf8(), fct = dictionary(int8(), utf8()))
  )
  expect_equal(batch2$column(0), batch$column(1))
  expect_data_frame(batch2, tbl[,-1])

  # input validation
  expect_error(batch$RemoveColumn(NA), "'i' cannot be NA")
  expect_error(batch$RemoveColumn(-1), "subscript out of bounds")
  expect_error(batch$RemoveColumn(1000), "subscript out of bounds")
  expect_error(batch$RemoveColumn(1:2), class = "Rcpp::not_compatible")
  expect_error(batch$RemoveColumn("one"), class = "Rcpp::not_compatible")
})

test_that("RecordBatch S3 methods", {
  tab <- RecordBatch$create(example_data)
  for (f in c("dim", "nrow", "ncol", "dimnames", "colnames", "row.names", "as.list")) {
    fun <- get(f)
    expect_identical(fun(tab), fun(example_data), info = f)
  }
})

test_that("RecordBatch$Slice", {
  batch3 <- batch$Slice(5)
  expect_data_frame(batch3, tbl[6:10,])

  batch4 <- batch$Slice(5, 2)
  expect_data_frame(batch4, tbl[6:7,])

  # Input validation
  expect_error(batch$Slice("ten"), class = "Rcpp::not_compatible")
  expect_error(batch$Slice(NA_integer_), "Slice 'offset' cannot be NA")
  expect_error(batch$Slice(NA), "Slice 'offset' cannot be NA")
  expect_error(batch$Slice(10, "ten"), class = "Rcpp::not_compatible")
  expect_error(batch$Slice(10, NA_integer_), "Slice 'length' cannot be NA")
  expect_error(batch$Slice(NA_integer_, NA_integer_), "Slice 'offset' cannot be NA")
  expect_error(batch$Slice(c(10, 10)), class = "Rcpp::not_compatible")
  expect_error(batch$Slice(10, c(10, 10)), class = "Rcpp::not_compatible")
  expect_error(batch$Slice(1000), "Slice 'offset' greater than array length")
  expect_error(batch$Slice(-1), "Slice 'offset' cannot be negative")
  expect_error(batch4$Slice(10, 10), "Slice 'offset' greater than array length")
  expect_error(batch$Slice(10, -1), "Slice 'length' cannot be negative")
  expect_error(batch$Slice(-1, 10), "Slice 'offset' cannot be negative")
})

test_that("[ on RecordBatch", {
  expect_data_frame(batch[6:7,], tbl[6:7,])
  expect_data_frame(batch[c(6, 7),], tbl[6:7,])
  expect_data_frame(batch[6:7, 2:4], tbl[6:7, 2:4])
  expect_data_frame(batch[, c("dbl", "fct")], tbl[, c(2, 5)])
  expect_identical(as.vector(batch[, "chr", drop = TRUE]), tbl$chr)
  expect_data_frame(batch[c(7, 3, 5), 2:4], tbl[c(7, 3, 5), 2:4])
  expect_data_frame(
    batch[rep(c(FALSE, TRUE), 5),],
    tbl[c(2, 4, 6, 8, 10),]
  )
  # bool Array
  expect_data_frame(batch[batch$lgl,], tbl[tbl$lgl,])
  # int Array
  expect_data_frame(batch[Array$create(5:6), 2:4], tbl[6:7, 2:4])
})

test_that("[[ and $ on RecordBatch", {
  expect_vector(batch[["int"]], tbl$int)
  expect_vector(batch$int, tbl$int)
  expect_vector(batch[[4]], tbl$chr)
  expect_null(batch$qwerty)
  expect_null(batch[["asdf"]])
  expect_error(batch[[c(4, 3)]], class = "Rcpp::not_compatible")
  expect_error(batch[[NA]], "'i' must be character or numeric, not logical")
  expect_error(batch[[NULL]], "'i' must be character or numeric, not NULL")
  expect_error(batch[[c("asdf", "jkl;")]], 'name is not a string', fixed = TRUE)
})

test_that("head and tail on RecordBatch", {
  expect_data_frame(head(batch), head(tbl))
  expect_data_frame(head(batch, 4), head(tbl, 4))
  expect_data_frame(head(batch, 40), head(tbl, 40))
  expect_data_frame(head(batch, -4), head(tbl, -4))
  expect_data_frame(head(batch, -40), head(tbl, -40))
  expect_data_frame(tail(batch), tail(tbl))
  expect_data_frame(tail(batch, 4), tail(tbl, 4))
  expect_data_frame(tail(batch, 40), tail(tbl, 40))
  expect_data_frame(tail(batch, -4), tail(tbl, -4))
  expect_data_frame(tail(batch, -40), tail(tbl, -40))
})

test_that("RecordBatch print method", {
  expect_output(
    print(batch),
    paste(
      "RecordBatch",
      "10 rows x 5 columns",
      "$int <int32>",
      "$dbl <double>",
      "$lgl <bool>",
      "$chr <string>",
      "$fct <dictionary<values=string, indices=int8>>",
      sep = "\n"
    ),
    fixed = TRUE
  )
})

test_that("RecordBatch with 0 rows are supported", {
  tbl <- tibble::tibble(
    int = integer(),
    dbl = numeric(),
    lgl = logical(),
    chr = character(),
    fct = factor(character(), levels = c("a", "b"))
  )

  batch <- record_batch(tbl)
  expect_equal(batch$num_columns, 5L)
  expect_equal(batch$num_rows, 0L)
  expect_equal(
    batch$schema,
    schema(
      int = int32(),
      dbl = float64(),
      lgl = boolean(),
      chr = utf8(),
      fct = dictionary(int8(), utf8())
    )
  )
})

test_that("RecordBatch cast (ARROW-3741)", {
  batch <- record_batch(x = 1:10, y = 1:10)

  expect_error(batch$cast(schema(x = int32())))
  expect_error(batch$cast(schema(x = int32(), z = int32())))

  s2 <- schema(x = int16(), y = int64())
  batch2 <- batch$cast(s2)
  expect_equal(batch2$schema, s2)
  expect_equal(batch2$column(0L)$type, int16())
  expect_equal(batch2$column(1L)$type, int64())
})

test_that("record_batch() handles schema= argument", {
  s <- schema(x = int32(), y = int32())
  batch <- record_batch(x = 1:10, y = 1:10, schema = s)
  expect_equal(s, batch$schema)

  s <- schema(x = int32(), y = float64())
  batch <- record_batch(x = 1:10, y = 1:10, schema = s)
  expect_equal(s, batch$schema)

  s <- schema(x = int32(), y = utf8())
  expect_error(record_batch(x = 1:10, y = 1:10, schema = s))
})

test_that("record_batch(schema=) does some basic consistency checking of the schema", {
  s <- schema(x = int32())
  expect_error(record_batch(x = 1:10, y = 1:10, schema = s))
  expect_error(record_batch(z = 1:10, schema = s))
})

test_that("RecordBatch dim() and nrow() (ARROW-3816)", {
  batch <- record_batch(x = 1:10, y  = 1:10)
  expect_equal(dim(batch), c(10L, 2L))
  expect_equal(nrow(batch), 10L)
})

test_that("record_batch() handles Array", {
  batch <- record_batch(x = 1:10, y = Array$create(1:10))
  expect_equal(batch$schema, schema(x = int32(), y = int32()))
})

test_that("record_batch() handles data frame columns", {
  tib <- tibble::tibble(x = 1:10, y = 1:10)
  # because tib is named here, this becomes a struct array
  batch <- record_batch(a = 1:10, b = tib)
  expect_equivalent(
    batch$schema,
    schema(
      a = int32(),
      b = struct(x = int32(), y = int32())
    )
  )
  out <- as.data.frame(batch)
  expect_equivalent(out, tibble::tibble(a = 1:10, b = tib))

  # if not named, columns from tib are auto spliced
  batch2 <- record_batch(a = 1:10, tib)
  expect_equal(
    batch2$schema,
    schema(a = int32(), x = int32(), y = int32())
  )
  out <- as.data.frame(batch2)
  expect_equivalent(out, tibble::tibble(a = 1:10, !!!tib))
})

test_that("record_batch() handles data frame columns with schema spec", {
  tib <- tibble::tibble(x = 1:10, y = 1:10)
  tib_float <- tib
  tib_float$y <- as.numeric(tib_float$y)
  schema <- schema(a = int32(), b = struct(x = int16(), y = float64()))
  batch <- record_batch(a = 1:10, b = tib, schema = schema)
  expect_equivalent(batch$schema, schema)
  out <- as.data.frame(batch)
  expect_equivalent(out, tibble::tibble(a = 1:10, b = tib_float))

  schema <- schema(a = int32(), b = struct(x = int16(), y = utf8()))
  expect_error(record_batch(a = 1:10, b = tib, schema = schema))
})

test_that("record_batch() auto splices (ARROW-5718)", {
  df <- tibble::tibble(x = 1:10, y = letters[1:10])
  batch1 <- record_batch(df)
  batch2 <- record_batch(!!!df)
  expect_equal(batch1, batch2)
  expect_equal(batch1$schema, schema(x = int32(), y = utf8()))
  expect_data_frame(batch1, df)

  batch3 <- record_batch(df, z = 1:10)
  batch4 <- record_batch(!!!df, z = 1:10)
  expect_equal(batch3, batch4)
  expect_equal(batch3$schema, schema(x = int32(), y = utf8(), z = int32()))
  expect_equivalent(as.data.frame(batch3), cbind(df, data.frame(z = 1:10)))

  s <- schema(x = float64(), y = utf8())
  batch5 <- record_batch(df, schema = s)
  batch6 <- record_batch(!!!df, schema = s)
  expect_equal(batch5, batch6)
  expect_equal(batch5$schema, s)
  expect_equivalent(as.data.frame(batch5), df)

  s2 <- schema(x = float64(), y = utf8(), z = int16())
  batch7 <- record_batch(df, z = 1:10, schema = s2)
  batch8 <- record_batch(!!!df, z = 1:10, schema = s2)
  expect_equal(batch7, batch8)
  expect_equal(batch7$schema, s2)
  expect_equivalent(as.data.frame(batch7), cbind(df, data.frame(z = 1:10)))
})

test_that("record_batch() only auto splice data frames", {
  expect_error(
    record_batch(1:10),
    regexp = "only data frames are allowed as unnamed arguments to be auto spliced"
  )
})

test_that("record_batch() handles null type (ARROW-7064)", {
  batch <- record_batch(a = 1:10, n = vctrs::unspecified(10))
  expect_equivalent(batch$schema,  schema(a = int32(), n = null()))
})

test_that("RecordBatch$Equals", {
  df <- tibble::tibble(x = 1:10, y = letters[1:10])
  a <- record_batch(df)
  b <- record_batch(df)
  expect_equal(a, b)
  expect_true(a$Equals(b))
  expect_false(a$Equals(df))
})

test_that("RecordBatch$Equals(check_metadata)", {
  df <- tibble::tibble(x = 1:2, y = c("a", "b"))
  rb1 <- record_batch(df)
  rb2 <- record_batch(df, schema = rb1$schema$WithMetadata(list(some="metadata")))

  expect_is(rb1, "RecordBatch")
  expect_is(rb2, "RecordBatch")
  expect_false(rb1$schema$HasMetadata)
  expect_true(rb2$schema$HasMetadata)
  expect_identical(rb2$schema$metadata, list(some = "metadata"))

  expect_true(rb1 == rb2)
  expect_true(rb1$Equals(rb2))
  expect_false(rb1$Equals(rb2, check_metadata = TRUE))

  expect_failure(expect_equal(rb1, rb2))  # expect_equal has check_metadata=TRUE
  expect_equivalent(rb1, rb2)  # expect_equivalent has check_metadata=FALSE

  expect_false(rb1$Equals(24)) # Not a RecordBatch
})
