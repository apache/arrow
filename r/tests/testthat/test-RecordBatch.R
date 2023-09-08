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

# Note that we're reusing `tbl` and `batch` throughout the tests in this file
tbl <- tibble::tibble(
  int = 1:10,
  dbl = as.numeric(1:10),
  lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
  chr = letters[1:10],
  fct = factor(letters[1:10])
)
batch <- record_batch(tbl)

test_that("RecordBatch", {
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
  expect_error(batch$column_name(1:2))
  expect_error(batch$column_name("one"))

  col_int <- batch$column(0)
  expect_true(inherits(col_int, "Array"))
  expect_equal(col_int$as_vector(), tbl$int)
  expect_equal(col_int$type, int32())

  col_dbl <- batch$column(1)
  expect_true(inherits(col_dbl, "Array"))
  expect_equal(col_dbl$as_vector(), tbl$dbl)
  expect_equal(col_dbl$type, float64())

  col_lgl <- batch$column(2)
  expect_true(inherits(col_dbl, "Array"))
  expect_equal(col_lgl$as_vector(), tbl$lgl)
  expect_equal(col_lgl$type, boolean())

  col_chr <- batch$column(3)
  expect_true(inherits(col_chr, "Array"))
  expect_equal(col_chr$as_vector(), tbl$chr)
  expect_equal(col_chr$type, utf8())

  col_fct <- batch$column(4)
  expect_true(inherits(col_fct, "Array"))
  expect_equal(col_fct$as_vector(), tbl$fct)
  expect_equal(col_fct$type, dictionary(int8(), utf8()))

  # input validation
  expect_error(batch$column(NA), "'i' cannot be NA")
  expect_error(batch$column(-1), "subscript out of bounds")
  expect_error(batch$column(1000), "subscript out of bounds")
  expect_error(batch$column(1:2))
  expect_error(batch$column("one"))

  batch2 <- batch$RemoveColumn(0)
  expect_equal(
    batch2$schema,
    schema(dbl = float64(), lgl = boolean(), chr = utf8(), fct = dictionary(int8(), utf8()))
  )
  expect_equal(batch2$column(0), batch$column(1))
  expect_equal_data_frame(batch2, tbl[, -1])

  # input validation
  expect_error(batch$RemoveColumn(NA), "'i' cannot be NA")
  expect_error(batch$RemoveColumn(-1), "subscript out of bounds")
  expect_error(batch$RemoveColumn(1000), "subscript out of bounds")
  expect_error(batch$RemoveColumn(1:2))
  expect_error(batch$RemoveColumn("one"))
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
  expect_equal_data_frame(batch3, tbl[6:10, ])

  batch4 <- batch$Slice(5, 2)
  expect_equal_data_frame(batch4, tbl[6:7, ])

  # Input validation
  expect_error(batch$Slice("ten"))
  expect_error(batch$Slice(NA_integer_), "Slice 'offset' cannot be NA")
  expect_error(batch$Slice(NA), "Slice 'offset' cannot be NA")
  expect_error(batch$Slice(10, "ten"))
  expect_error(batch$Slice(10, NA_integer_), "Slice 'length' cannot be NA")
  expect_error(batch$Slice(NA_integer_, NA_integer_), "Slice 'offset' cannot be NA")
  expect_error(batch$Slice(c(10, 10)))
  expect_error(batch$Slice(10, c(10, 10)))
  expect_error(batch$Slice(1000), "Slice 'offset' greater than array length")
  expect_error(batch$Slice(-1), "Slice 'offset' cannot be negative")
  expect_error(batch4$Slice(10, 10), "Slice 'offset' greater than array length")
  expect_error(batch$Slice(10, -1), "Slice 'length' cannot be negative")
  expect_error(batch$Slice(-1, 10), "Slice 'offset' cannot be negative")
})

test_that("[ on RecordBatch", {
  expect_equal_data_frame(batch[6:7, ], tbl[6:7, ])
  expect_equal_data_frame(batch[c(6, 7), ], tbl[6:7, ])
  expect_equal_data_frame(batch[6:7, 2:4], tbl[6:7, 2:4])
  expect_equal_data_frame(batch[, c("dbl", "fct")], tbl[, c(2, 5)])
  expect_identical(as.vector(batch[, "chr", drop = TRUE]), tbl$chr)
  expect_equal_data_frame(batch[c(7, 3, 5), 2:4], tbl[c(7, 3, 5), 2:4])
  expect_equal_data_frame(
    batch[rep(c(FALSE, TRUE), 5), ],
    tbl[c(2, 4, 6, 8, 10), ]
  )
  # bool Array
  expect_equal_data_frame(batch[batch$lgl, ], tbl[tbl$lgl, ])
  # int Array
  expect_equal_data_frame(batch[Array$create(5:6), 2:4], tbl[6:7, 2:4])

  # input validation
  expect_error(batch[, c("dbl", "NOTACOLUMN")], 'Column not found: "NOTACOLUMN"')
  expect_error(batch[, c(6, NA)], "Column indices cannot be NA")
  expect_error(batch[, c(2, -2)], "Invalid column index")
})

test_that("[[ and $ on RecordBatch", {
  expect_as_vector(batch[["int"]], tbl$int)
  expect_as_vector(batch$int, tbl$int)
  expect_as_vector(batch[[4]], tbl$chr)
  expect_null(batch$qwerty)
  expect_null(batch[["asdf"]])
  expect_error(batch[[c(4, 3)]])
  expect_error(batch[[NA]], "'i' must be character or numeric, not logical")
  expect_error(batch[[NULL]], "'i' must be character or numeric, not NULL")
  expect_error(batch[[c("asdf", "jkl;")]], "name is not a string", fixed = TRUE)
})

test_that("[[<- assignment", {
  tbl <- tibble::tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10],
    fct = factor(letters[1:10])
  )
  batch <- RecordBatch$create(tbl)

  # can remove a column
  batch[["chr"]] <- NULL
  expect_equal_data_frame(batch, tbl[-4])

  # can remove a column by index
  batch[[4]] <- NULL
  expect_equal_data_frame(batch, tbl[1:3])

  # can add a named column
  batch[["new"]] <- letters[10:1]
  expect_equal_data_frame(batch, dplyr::bind_cols(tbl[1:3], new = letters[10:1]))

  # can replace a column by index
  batch[[2]] <- as.numeric(10:1)
  expect_as_vector(batch[[2]], as.numeric(10:1))

  # can add a column by index
  batch[[5]] <- as.numeric(10:1)
  expect_as_vector(batch[[5]], as.numeric(10:1))
  expect_as_vector(batch[["5"]], as.numeric(10:1))

  # can replace a column
  batch[["int"]] <- 10:1
  expect_as_vector(batch[["int"]], 10:1)

  # can use $
  batch$new <- NULL
  expect_null(as.vector(batch$new))
  expect_identical(dim(batch), c(10L, 4L))

  batch$int <- 1:10
  expect_as_vector(batch$int, 1:10)

  # recycling
  batch[["atom"]] <- 1L
  expect_as_vector(batch[["atom"]], rep(1L, 10))

  expect_error(
    batch[["atom"]] <- 1:6,
    "Can't recycle input of size 6 to size 10."
  )

  # assign Arrow array
  array <- Array$create(c(10:1))
  batch$array <- array
  expect_as_vector(batch$array, 10:1)

  # nonsense indexes
  expect_error(batch[[NA]] <- letters[10:1], "'i' must be character or numeric, not logical")
  expect_error(batch[[NULL]] <- letters[10:1], "'i' must be character or numeric, not NULL")
  expect_error(batch[[NA_integer_]] <- letters[10:1], "!is.na(i) is not TRUE", fixed = TRUE)
  expect_error(batch[[NA_real_]] <- letters[10:1], "!is.na(i) is not TRUE", fixed = TRUE)
  expect_error(batch[[NA_character_]] <- letters[10:1], "!is.na(i) is not TRUE", fixed = TRUE)
  expect_error(batch[[c(1, 4)]] <- letters[10:1], "length(i) not equal to 1", fixed = TRUE)
})

test_that("head and tail on RecordBatch", {
  tbl <- tibble::tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10],
    fct = factor(letters[1:10])
  )
  batch <- RecordBatch$create(tbl)
  expect_equal_data_frame(head(batch), head(tbl))
  expect_equal_data_frame(head(batch, 4), head(tbl, 4))
  expect_equal_data_frame(head(batch, 40), head(tbl, 40))
  expect_equal_data_frame(head(batch, -4), head(tbl, -4))
  expect_equal_data_frame(head(batch, -40), head(tbl, -40))
  expect_equal_data_frame(tail(batch), tail(tbl))
  expect_equal_data_frame(tail(batch, 4), tail(tbl, 4))
  expect_equal_data_frame(tail(batch, 40), tail(tbl, 40))
  expect_equal_data_frame(tail(batch, -4), tail(tbl, -4))
  expect_equal_data_frame(tail(batch, -40), tail(tbl, -40))
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
  batch <- record_batch(x = 1:10, y = 1:10)
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
  expect_equal(
    batch$schema,
    schema(
      a = int32(),
      b = struct(x = int32(), y = int32())
    )
  )

  expect_equal_data_frame(batch, tibble::tibble(a = 1:10, b = tib))

  # if not named, columns from tib are auto spliced
  batch2 <- record_batch(a = 1:10, tib)
  expect_equal(
    batch2$schema,
    schema(a = int32(), x = int32(), y = int32())
  )

  expect_equal_data_frame(batch2, tibble::tibble(a = 1:10, !!!tib))
})

test_that("record_batch() handles data frame columns with schema spec", {
  tib <- tibble::tibble(x = 1:10, y = 1:10)
  tib_float <- tib
  tib_float$y <- as.numeric(tib_float$y)
  schema <- schema(a = int32(), b = struct(x = int16(), y = float64()))
  batch <- record_batch(a = 1:10, b = tib, schema = schema)
  expect_equal(batch$schema, schema)
  expect_equal_data_frame(batch, tibble::tibble(a = 1:10, b = tib_float))

  schema <- schema(a = int32(), b = struct(x = int16(), y = utf8()))
  expect_error(record_batch(a = 1:10, b = tib, schema = schema))
})

test_that("record_batch() auto splices (ARROW-5718)", {
  df <- tibble::tibble(x = 1:10, y = letters[1:10])
  batch1 <- record_batch(df)
  batch2 <- record_batch(!!!df)
  expect_equal(batch1, batch2)
  expect_equal(batch1$schema, schema(x = int32(), y = utf8()))
  expect_equal_data_frame(batch1, df)

  batch3 <- record_batch(df, z = 1:10)
  batch4 <- record_batch(!!!df, z = 1:10)
  expect_equal(batch3, batch4)
  expect_equal(batch3$schema, schema(x = int32(), y = utf8(), z = int32()))
  expect_equal_data_frame(
    batch3,
    cbind(df, data.frame(z = 1:10))
  )

  s <- schema(x = float64(), y = utf8())
  batch5 <- record_batch(df, schema = s)
  batch6 <- record_batch(!!!df, schema = s)
  expect_equal(batch5, batch6)
  expect_equal(batch5$schema, s)
  expect_equal_data_frame(batch5, df)

  s2 <- schema(x = float64(), y = utf8(), z = int16())
  batch7 <- record_batch(df, z = 1:10, schema = s2)
  batch8 <- record_batch(!!!df, z = 1:10, schema = s2)
  expect_equal(batch7, batch8)
  expect_equal(batch7$schema, s2)
  expect_equal_data_frame(
    batch7,
    cbind(df, data.frame(z = 1:10))
  )
})

test_that("record_batch() only auto splice data frames", {
  expect_error(
    record_batch(1:10),
    regexp = "only data frames are allowed as unnamed arguments to be auto spliced"
  )
})

test_that("record_batch() handles null type (ARROW-7064)", {
  batch <- record_batch(a = 1:10, n = vctrs::unspecified(10))
  expect_equal(
    batch$schema,
    schema(a = int32(), n = null()),
    ignore_attr = TRUE
  )
})

test_that("record_batch() scalar recycling with vectors", {
  expect_equal_data_frame(
    record_batch(a = 1:10, b = 5),
    tibble::tibble(a = 1:10, b = 5)
  )
})

test_that("record_batch() scalar recycling with Scalars, Arrays, and ChunkedArrays", {
  expect_equal_data_frame(
    record_batch(a = Array$create(1:10), b = Scalar$create(5)),
    tibble::tibble(a = 1:10, b = 5)
  )

  expect_equal_data_frame(
    record_batch(a = Array$create(1:10), b = Array$create(5)),
    tibble::tibble(a = 1:10, b = 5)
  )

  expect_equal_data_frame(
    record_batch(a = Array$create(1:10), b = ChunkedArray$create(5)),
    tibble::tibble(a = 1:10, b = 5)
  )
})

test_that("record_batch() no recycling with tibbles", {
  expect_error(
    record_batch(
      tibble::tibble(a = 1:10),
      tibble::tibble(a = 1, b = 5)
    ),
    regexp = "All input tibbles or data.frames must have the same number of rows"
  )

  expect_error(
    record_batch(
      tibble::tibble(a = 1:10),
      tibble::tibble(a = 1)
    ),
    regexp = "All input tibbles or data.frames must have the same number of rows"
  )
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
  rb2 <- record_batch(df, schema = rb1$schema$WithMetadata(list(some = "metadata")))

  expect_r6_class(rb1, "RecordBatch")
  expect_r6_class(rb2, "RecordBatch")
  expect_false(rb1$schema$HasMetadata)
  expect_true(rb2$schema$HasMetadata)
  expect_identical(rb2$schema$metadata, list(some = "metadata"))

  expect_true(rb1 == rb2)
  expect_true(rb1$Equals(rb2))
  expect_false(rb1$Equals(rb2, check_metadata = TRUE))

  expect_failure(expect_equal(rb1, rb2)) # expect_equal has check_metadata=TRUE
  expect_equal(rb1, rb2, ignore_attr = TRUE) # this passes check_metadata=FALSE

  expect_false(rb1$Equals(24)) # Not a RecordBatch
})

test_that("RecordBatch name assignment", {
  rb <- record_batch(x = 1:10, y = 1:10)
  expect_identical(names(rb), c("x", "y"))
  names(rb) <- c("a", "b")
  expect_identical(names(rb), c("a", "b"))
  expect_error(names(rb) <- "f")
  expect_error(names(rb) <- letters)
  expect_error(names(rb) <- character(0))
  expect_error(names(rb) <- NULL)
  expect_error(names(rb) <- c(TRUE, FALSE))
})

test_that("record_batch() with different length arrays", {
  msg <- "All arrays must have the same length"
  expect_error(record_batch(a = 1:5, b = 1:6), msg)
})

test_that("RecordBatch doesn't support rbind", {
  expect_snapshot_error(
    rbind(
      record_batch(a = 1:10),
      record_batch(a = 2:4)
    )
  )
})

test_that("RecordBatch supports cbind", {
  expect_snapshot_error(
    cbind(
      record_batch(a = 1:10),
      record_batch(a = c("a", "b"))
    )
  )
  expect_error(
    cbind(record_batch(a = 1:10), record_batch(b = character(0))),
    regexp = "Non-scalar inputs must have an equal number of rows"
  )

  actual <- cbind(
    record_batch(a = c(1, 2), b = c("a", "b")),
    record_batch(a = c("d", "c")),
    record_batch(c = c(2, 3))
  )
  expected <- record_batch(
    a = c(1, 2),
    b = c("a", "b"),
    a = c("d", "c"),
    c = c(2, 3)
  )
  expect_equal(actual, expected)

  # cbind() with one argument returns identical table
  expected <- record_batch(a = 1:10)
  expect_equal(expected, cbind(expected))

  # Handles arrays
  expect_equal(
    cbind(record_batch(a = 1:2), b = Array$create(4:5)),
    record_batch(a = 1:2, b = 4:5)
  )

  # Handles data.frames on R 4.0 or greater
  if (getRversion() >= "4.0.0") {
    # Prior to R 4.0, cbind would short-circuit to the data.frame implementation
    # if **any** of the arguments are a data.frame.
    expect_equal(
      cbind(record_batch(a = 1:2), data.frame(b = 4:5)),
      record_batch(a = 1:2, b = 4:5)
    )
  }

  # Handles base factors
  expect_equal(
    cbind(record_batch(a = 1:2), b = factor(c("a", "b"))),
    record_batch(a = 1:2, b = factor(c("a", "b")))
  )

  # Handles base scalars
  expect_equal(
    cbind(record_batch(a = 1:2), b = 1L),
    record_batch(a = 1:2, b = rep(1L, 2))
  )

  # Handles zero rows
  expect_equal(
    cbind(record_batch(a = character(0)), b = Array$create(numeric(0)), c = integer(0)),
    record_batch(a = character(0), b = numeric(0), c = integer(0)),
  )

  # Rejects unnamed arrays, even in cases where no named arguments are passed
  expect_error(
    cbind(record_batch(a = 1:2), b = 3:4, 5:6),
    regexp = "Vector and array arguments must have names"
  )
  expect_error(
    cbind(record_batch(a = 1:2), 3:4, 5:6),
    regexp = "Vector and array arguments must have names"
  )

  # Rejects Table and ChunkedArray arguments
  if (getRversion() >= "4.0.0") {
    # R 3.6 cbind dispatch rules cause cbind to fall back to default impl if
    # there are multiple arguments with distinct cbind implementations
    expect_error(
      cbind(record_batch(a = 1:2), arrow_table(b = 3:4)),
      regexp = "Cannot cbind a RecordBatch with Tables or ChunkedArrays"
    )
  }
  expect_error(
    cbind(record_batch(a = 1:2), b = chunked_array(1, 2)),
    regexp = "Cannot cbind a RecordBatch with Tables or ChunkedArrays"
  )
})

test_that("Handling string data with embedded nuls", {
  raws <- Array$create(structure(list(
    as.raw(c(0x70, 0x65, 0x72, 0x73, 0x6f, 0x6e)),
    as.raw(c(0x77, 0x6f, 0x6d, 0x61, 0x6e)),
    as.raw(c(0x6d, 0x61, 0x00, 0x6e)), # <-- there's your nul, 0x00
    as.raw(c(0x63, 0x61, 0x6d, 0x65, 0x72, 0x61)),
    as.raw(c(0x74, 0x76))
  ),
  class = c("arrow_binary", "vctrs_vctr", "list")
  ))
  batch_with_nul <- record_batch(a = 1:5, b = raws)
  batch_with_nul$b <- batch_with_nul$b$cast(utf8())

  # The behavior of the warnings/errors is slightly different with and without
  # altrep. Without it (i.e. 3.5.0 and below, the error would trigger immediately
  # on `as.vector()` where as with it, the error only happens on materialization)
  skip_on_r_older_than("3.6")
  df <- as.data.frame(batch_with_nul)

  expect_error(
    df$b[],
    paste0(
      "embedded nul in string: 'ma\\0n'; to strip nuls when converting from Arrow to R, ",
      "set options(arrow.skip_nul = TRUE)"
    ),
    fixed = TRUE
  )

  batch_with_nul <- record_batch(a = 1:5, b = raws)
  batch_with_nul$b <- batch_with_nul$b$cast(utf8())

  withr::with_options(list(arrow.skip_nul = TRUE), {
    # Because expect_equal() may call identical(x, y) more than once,
    # the string with a nul may be created more than once and multiple
    # warnings may be issued.
    suppressWarnings(
      expect_warning(
        expect_equal(
          as.data.frame(batch_with_nul)$b,
          c("person", "woman", "man", "camera", "tv"),
          ignore_attr = TRUE
        ),
        "Stripping '\\0' (nul) from character vector",
        fixed = TRUE
      )
    )
  })
})

test_that("ARROW-11769/ARROW-13860/ARROW-17085 - grouping preserved in record batch creation", {
  skip_if_not_available("dataset")
  library(dplyr, warn.conflicts = FALSE)

  tbl <- tibble::tibble(
    int = 1:10,
    fct = factor(rep(c("A", "B"), 5)),
    fct2 = factor(rep(c("C", "D"), each = 5)),
  )

  expect_r6_class(
    tbl %>%
      group_by(fct, fct2) %>%
      record_batch(),
    "RecordBatch"
  )
  expect_identical(
    tbl %>%
      record_batch() %>%
      group_vars(),
    group_vars(tbl)
  )
  expect_identical(
    tbl %>%
      group_by(fct, fct2) %>%
      record_batch() %>%
      group_vars(),
    c("fct", "fct2")
  )
  expect_identical(
    tbl %>%
      group_by(fct, fct2) %>%
      record_batch() %>%
      ungroup() %>%
      group_vars(),
    character()
  )
  expect_identical(
    tbl %>%
      group_by(fct, fct2) %>%
      record_batch() %>%
      select(-int) %>%
      group_vars(),
    c("fct", "fct2")
  )
})

test_that("ARROW-12729 - length returns number of columns in RecordBatch", {
  tbl <- tibble::tibble(
    int = 1:10,
    fct = factor(rep(c("A", "B"), 5)),
    fct2 = factor(rep(c("C", "D"), each = 5)),
  )

  rb <- record_batch(!!!tbl)

  expect_identical(length(rb), 3L)
})

test_that("RecordBatchReader to C-interface", {
  skip_if_not_available("dataset")

  tab <- Table$create(example_data)

  # export the RecordBatchReader via the C-interface
  stream_ptr <- allocate_arrow_array_stream()
  scan <- Scanner$create(tab)
  reader <- scan$ToRecordBatchReader()
  reader$export_to_c(stream_ptr)

  # then import it and check that the roundtripped value is the same
  circle <- RecordBatchStreamReader$import_from_c(stream_ptr)
  tab_from_c_new <- circle$read_table()
  expect_equal(tab, tab_from_c_new)

  # must clean up the pointer or we leak
  delete_arrow_array_stream(stream_ptr)

  # export the RecordBatchStreamReader via the C-interface
  stream_ptr_new <- allocate_arrow_array_stream()
  bytes <- write_to_raw(example_data)
  expect_type(bytes, "raw")
  reader_new <- RecordBatchStreamReader$create(bytes)
  reader_new$export_to_c(stream_ptr_new)

  # then import it and check that the roundtripped value is the same
  circle_new <- RecordBatchStreamReader$import_from_c(stream_ptr_new)
  tab_from_c_new <- circle_new$read_table()
  expect_equal(tab, tab_from_c_new)

  # must clean up the pointer or we leak
  delete_arrow_array_stream(stream_ptr_new)
})

test_that("RecordBatch to C-interface", {
  batch <- RecordBatch$create(example_data)

  # export the RecordBatch via the C-interface
  schema_ptr <- allocate_arrow_schema()
  array_ptr <- allocate_arrow_array()
  batch$export_to_c(array_ptr, schema_ptr)

  # then import it and check that the roundtripped value is the same
  circle <- RecordBatch$import_from_c(array_ptr, schema_ptr)
  expect_equal(batch, circle)

  # must clean up the pointers or we leak
  delete_arrow_schema(schema_ptr)
  delete_arrow_array(array_ptr)
})



test_that("RecordBatchReader to C-interface to arrow_dplyr_query", {
  skip_if_not_available("dataset")

  tab <- Table$create(example_data)

  # export the RecordBatchReader via the C-interface
  stream_ptr <- allocate_arrow_array_stream()
  scan <- Scanner$create(tab)
  reader <- scan$ToRecordBatchReader()
  reader$export_to_c(stream_ptr)

  # then import it and check that the roundtripped value is the same
  circle <- RecordBatchStreamReader$import_from_c(stream_ptr)

  # create an arrow_dplyr_query() from the recordbatch reader
  reader_adq <- arrow_dplyr_query(circle)

  tab_from_c_new <- reader_adq %>%
    dplyr::compute()
  expect_equal(tab_from_c_new, tab)

  # must clean up the pointer or we leak
  delete_arrow_array_stream(stream_ptr)
})


test_that("as_record_batch() works for RecordBatch", {
  batch <- record_batch(col1 = 1L, col2 = "two")
  expect_identical(as_record_batch(batch), batch)
  expect_equal(
    as_record_batch(batch, schema = schema(col1 = float64(), col2 = string())),
    record_batch(col1 = Array$create(1, type = float64()), col2 = "two")
  )
})

test_that("as_record_batch() works for Table", {
  batch <- record_batch(col1 = 1L, col2 = "two")
  table <- arrow_table(col1 = 1L, col2 = "two")

  expect_equal(as_record_batch(table), batch)
  expect_equal(
    as_record_batch(table, schema = schema(col1 = float64(), col2 = string())),
    record_batch(col1 = Array$create(1, type = float64()), col2 = "two")
  )

  # also check zero column table and make sure row count is preserved
  table0 <- table[integer()]
  expect_identical(table0$num_columns, 0L)
  expect_identical(table0$num_rows, 1L)

  batch0 <- as_record_batch(table0)
  expect_identical(batch0$num_columns, 0L)
  expect_identical(batch0$num_rows, 1L)
})

test_that("as_record_batch() works for data.frame()", {
  batch <- record_batch(col1 = 1L, col2 = "two")
  tbl <- tibble::tibble(col1 = 1L, col2 = "two")

  expect_equal(as_record_batch(tbl), batch)

  expect_equal(
    as_record_batch(
      tbl,
      schema = schema(col1 = float64(), col2 = string())
    ),
    record_batch(col1 = Array$create(1, type = float64()), col2 = "two")
  )
})
