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

test_that("Table cast (ARROW-3741)", {
  tab <- Table$create(x = 1:10, y = 1:10)

  expect_error(tab$cast(schema(x = int32())))
  expect_error(tab$cast(schema(x = int32(), z = int32())))

  s2 <- schema(x = int16(), y = int64())
  tab2 <- tab$cast(s2)
  expect_equal(tab2$schema, s2)
  expect_equal(tab2$column(0L)$type, int16())
  expect_equal(tab2$column(1L)$type, int64())
})

test_that("Table S3 methods", {
  tab <- Table$create(example_data)
  for (f in c("dim", "nrow", "ncol", "dimnames", "colnames", "row.names", "as.list")) {
    fun <- get(f)
    expect_identical(fun(tab), fun(example_data), info = f)
  }
})

test_that("Table $column and $field", {
  tab <- Table$create(x = 1:10, y = 1:10)

  expect_equal(tab$field(0), field("x", int32()))

  # input validation
  expect_error(tab$column(NA), "'i' cannot be NA")
  expect_error(tab$column(-1), "subscript out of bounds")
  expect_error(tab$column(1000), "subscript out of bounds")
  expect_error(tab$column(1:2))
  expect_error(tab$column("one"))

  expect_error(tab$field(NA), "'i' cannot be NA")
  expect_error(tab$field(-1), "subscript out of bounds")
  expect_error(tab$field(1000), "subscript out of bounds")
  expect_error(tab$field(1:2))
  expect_error(tab$field("one"))
})

# Common fixtures used in some of the following tests
tbl <- tibble::tibble(
  int = 1:10,
  dbl = as.numeric(1:10),
  lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
  chr = letters[1:10],
  fct = factor(letters[1:10])
)
tab <- Table$create(tbl)

test_that("[, [[, $ for Table", {
  expect_identical(names(tab), names(tbl))

  expect_equal_data_frame(tab[6:7, ], tbl[6:7, ])
  expect_equal_data_frame(tab[6:7, 2:4], tbl[6:7, 2:4])
  expect_equal_data_frame(tab[, c("dbl", "fct")], tbl[, c(2, 5)])
  expect_as_vector(tab[, "chr", drop = TRUE], tbl$chr)
  # Take within a single chunk
  expect_equal_data_frame(tab[c(7, 3, 5), 2:4], tbl[c(7, 3, 5), 2:4])
  expect_equal_data_frame(tab[rep(c(FALSE, TRUE), 5), ], tbl[c(2, 4, 6, 8, 10), ])
  # bool ChunkedArray (with one chunk)
  expect_equal_data_frame(tab[tab$lgl, ], tbl[tbl$lgl, ])
  # ChunkedArray with multiple chunks
  c1 <- c(TRUE, FALSE, TRUE, TRUE, FALSE)
  c2 <- c(FALSE, FALSE, TRUE, TRUE, FALSE)
  ca <- ChunkedArray$create(c1, c2)
  expect_equal_data_frame(tab[ca, ], tbl[c(1, 3, 4, 8, 9), ])
  # int Array
  expect_equal_data_frame(tab[Array$create(5:6), 2:4], tbl[6:7, 2:4])
  # ChunkedArray
  expect_equal_data_frame(tab[ChunkedArray$create(5L, 6L), 2:4], tbl[6:7, 2:4])
  # Expression
  expect_equal_data_frame(tab[tab$int > 6, ], tbl[tbl$int > 6, ])

  expect_as_vector(tab[["int"]], tbl$int)
  expect_as_vector(tab$int, tbl$int)
  expect_as_vector(tab[[4]], tbl$chr)
  expect_null(tab$qwerty)
  expect_null(tab[["asdf"]])
  # List-like column slicing
  expect_equal_data_frame(tab[2:4], tbl[2:4])
  expect_equal_data_frame(tab[c(2, 1)], tbl[c(2, 1)])
  expect_equal_data_frame(tab[-3], tbl[-3])

  expect_error(tab[[c(4, 3)]])
  expect_error(tab[[NA]], "'i' must be character or numeric, not logical")
  expect_error(tab[[NULL]], "'i' must be character or numeric, not NULL")
  expect_error(tab[[c("asdf", "jkl;")]], "length(name) not equal to 1", fixed = TRUE)
  expect_error(tab[-3:3], "Invalid column index")
  expect_error(tab[1000], "Invalid column index")
  expect_error(tab[1:1000], "Invalid column index")

  # input validation
  expect_error(tab[, c("dbl", "NOTACOLUMN")], 'Column not found: "NOTACOLUMN"')
  expect_error(tab[, c(6, NA)], "Column indices cannot be NA")

  skip("Table with 0 cols doesn't know how many rows it should have")
  expect_equal_data_frame(tab[0], tbl[0])
})

test_that("[[<- assignment", {
  # can remove a column
  tab[["chr"]] <- NULL
  expect_equal_data_frame(tab, tbl[-4])

  # can remove a column by index
  tab[[4]] <- NULL
  expect_equal_data_frame(tab, tbl[1:3])

  # can add a named column
  tab[["new"]] <- letters[10:1]
  expect_equal_data_frame(tab, dplyr::bind_cols(tbl[1:3], new = letters[10:1]))

  # can replace a column by index
  tab[[2]] <- as.numeric(10:1)
  expect_as_vector(tab[[2]], as.numeric(10:1))

  # can add a column by index
  tab[[5]] <- as.numeric(10:1)
  expect_as_vector(tab[[5]], as.numeric(10:1))
  expect_as_vector(tab[["5"]], as.numeric(10:1))

  # can replace a column
  tab[["int"]] <- 10:1
  expect_as_vector(tab[["int"]], 10:1)

  # can use $
  tab$new <- NULL
  expect_null(as.vector(tab$new))
  expect_identical(dim(tab), c(10L, 4L))

  tab$int <- 1:10
  expect_as_vector(tab$int, 1:10)

  # recycling
  tab[["atom"]] <- 1L
  expect_as_vector(tab[["atom"]], rep(1L, 10))

  expect_error(
    tab[["atom"]] <- 1:6,
    "Can't recycle input of size 6 to size 10."
  )

  # assign Arrow array and chunked_array
  array <- Array$create(c(10:1))
  tab$array <- array
  expect_as_vector(tab$array, 10:1)

  tab$chunked <- chunked_array(1:10)
  expect_as_vector(tab$chunked, 1:10)

  # nonsense indexes
  expect_error(tab[[NA]] <- letters[10:1], "'i' must be character or numeric, not logical")
  expect_error(tab[[NULL]] <- letters[10:1], "'i' must be character or numeric, not NULL")
  expect_error(tab[[NA_integer_]] <- letters[10:1], "!is.na(i) is not TRUE", fixed = TRUE)
  expect_error(tab[[NA_real_]] <- letters[10:1], "!is.na(i) is not TRUE", fixed = TRUE)
  expect_error(tab[[NA_character_]] <- letters[10:1], "!is.na(i) is not TRUE", fixed = TRUE)
  expect_error(tab[[c(1, 4)]] <- letters[10:1], "length(i) not equal to 1", fixed = TRUE)
})

test_that("Table$Slice", {
  tab2 <- tab$Slice(5)
  expect_equal_data_frame(tab2, tbl[6:10, ])

  tab3 <- tab$Slice(5, 2)
  expect_equal_data_frame(tab3, tbl[6:7, ])

  # Input validation
  expect_error(tab$Slice("ten"))
  expect_error(tab$Slice(NA_integer_), "Slice 'offset' cannot be NA")
  expect_error(tab$Slice(NA), "Slice 'offset' cannot be NA")
  expect_error(tab$Slice(10, "ten"))
  expect_error(tab$Slice(10, NA_integer_), "Slice 'length' cannot be NA")
  expect_error(tab$Slice(NA_integer_, NA_integer_), "Slice 'offset' cannot be NA")
  expect_error(tab$Slice(c(10, 10)))
  expect_error(tab$Slice(10, c(10, 10)))
  expect_error(tab$Slice(1000), "Slice 'offset' greater than array length")
  expect_error(tab$Slice(-1), "Slice 'offset' cannot be negative")
  expect_error(tab3$Slice(10, 10), "Slice 'offset' greater than array length")
  expect_error(tab$Slice(10, -1), "Slice 'length' cannot be negative")
  expect_error(tab$Slice(-1, 10), "Slice 'offset' cannot be negative")
})

test_that("head and tail on Table", {
  expect_equal_data_frame(head(tab), head(tbl))
  expect_equal_data_frame(head(tab, 4), head(tbl, 4))
  expect_equal_data_frame(head(tab, 40), head(tbl, 40))
  expect_equal_data_frame(head(tab, -4), head(tbl, -4))
  expect_equal_data_frame(head(tab, -40), head(tbl, -40))
  expect_equal_data_frame(tail(tab), tail(tbl))
  expect_equal_data_frame(tail(tab, 4), tail(tbl, 4))
  expect_equal_data_frame(tail(tab, 40), tail(tbl, 40))
  expect_equal_data_frame(tail(tab, -4), tail(tbl, -4))
  expect_equal_data_frame(tail(tab, -40), tail(tbl, -40))
})

test_that("Table print method", {
  expect_output(
    print(tab),
    paste(
      "Table",
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

test_that("table active bindings", {
  expect_identical(dim(tbl), dim(tab))
  expect_type(tab$columns, "list")
  expect_equal(tab$columns[[1]], tab[[1]])
})

test_that("table() handles record batches with splicing", {
  batch <- record_batch(x = 1:2, y = letters[1:2])
  tab <- Table$create(batch, batch, batch)
  expect_equal(tab$schema, batch$schema)
  expect_equal(tab$num_rows, 6L)
  expect_equal(
    as.data.frame(tab),
    vctrs::vec_rbind(as.data.frame(batch), as.data.frame(batch), as.data.frame(batch))
  )

  batches <- list(batch, batch, batch)
  tab <- Table$create(!!!batches)
  expect_equal(tab$schema, batch$schema)
  expect_equal(tab$num_rows, 6L)
  expect_equal(
    as.data.frame(tab),
    vctrs::vec_rbind(!!!purrr::map(batches, as.data.frame))
  )
})

test_that("table() handles ... of arrays, chunked arrays, vectors", {
  a <- Array$create(1:10)
  ca <- chunked_array(1:5, 6:10)
  v <- rnorm(10)
  tbl <- tibble::tibble(x = 1:10, y = letters[1:10])

  tab <- Table$create(a = a, b = ca, c = v, !!!tbl)
  expect_equal(
    tab$schema,
    schema(a = int32(), b = int32(), c = float64(), x = int32(), y = utf8())
  )

  expect_equal_data_frame(
    tab,
    tibble::tibble(a = 1:10, b = 1:10, c = v, x = 1:10, y = letters[1:10])
  )
})

test_that("table() auto splices (ARROW-5718)", {
  df <- tibble::tibble(x = 1:10, y = letters[1:10])

  tab1 <- Table$create(df)
  tab2 <- Table$create(!!!df)
  expect_equal(tab1, tab2)
  expect_equal(tab1$schema, schema(x = int32(), y = utf8()))
  expect_equal_data_frame(tab1, df)

  s <- schema(x = float64(), y = utf8())
  tab3 <- Table$create(df, schema = s)
  tab4 <- Table$create(!!!df, schema = s)
  expect_equal(tab3, tab4)
  expect_equal(tab3$schema, s)
  expect_equal_data_frame(tab3, df)
})

test_that("Validation when creating table with schema (ARROW-10953)", {
  expect_error(
    Table$create(data.frame(), schema = schema(a = int32())),
    "incompatible. schema has 1 fields, and 0 columns are supplied",
    fixed = TRUE
  )
  expect_error(
    Table$create(data.frame(b = 1), schema = schema(a = int32())),
    "field at index 1 has name 'a' != 'b'",
    fixed = TRUE
  )
  expect_error(
    Table$create(data.frame(b = 2, c = 3), schema = schema(a = int32())),
    "incompatible. schema has 1 fields, and 2 columns are supplied",
    fixed = TRUE
  )
})

test_that("==.Table", {
  tab1 <- Table$create(x = 1:2, y = c("a", "b"))
  tab2 <- Table$create(x = 1:2, y = c("a", "b"))
  tab3 <- Table$create(x = 1:2)
  tab4 <- Table$create(x = 1:2, y = c("a", "b"), z = 3:4)

  expect_true(tab1 == tab2)
  expect_true(tab2 == tab1)

  expect_false(tab1 == tab3)
  expect_false(tab3 == tab1)

  expect_false(tab1 == tab4)
  expect_false(tab4 == tab1)

  expect_true(all.equal(tab1, tab2))
  expect_equal(tab1, tab2)
})

test_that("Table$Equals(check_metadata)", {
  tab1 <- Table$create(x = 1:2, y = c("a", "b"))
  tab2 <- Table$create(
    x = 1:2, y = c("a", "b"),
    schema = tab1$schema$WithMetadata(list(some = "metadata"))
  )

  expect_r6_class(tab1, "Table")
  expect_r6_class(tab2, "Table")
  expect_false(tab1$schema$HasMetadata)
  expect_true(tab2$schema$HasMetadata)
  expect_identical(tab2$schema$metadata, list(some = "metadata"))

  expect_true(tab1 == tab2)
  expect_true(tab1$Equals(tab2))
  expect_false(tab1$Equals(tab2, check_metadata = TRUE))

  expect_failure(expect_equal(tab1, tab2)) # expect_equal has check_metadata=TRUE
  expect_equal(tab1, tab2, ignore_attr = TRUE) # this sets check_metadata=FALSE

  expect_false(tab1$Equals(24)) # Not a Table
})

test_that("Table handles null type (ARROW-7064)", {
  tab <- Table$create(a = 1:10, n = vctrs::unspecified(10))
  expect_equal(tab$schema, schema(a = int32(), n = null()), ignore_attr = TRUE)
})

test_that("Can create table with specific dictionary types", {
  fact <- example_data[, "fct"]
  int_types <- c(int8(), int16(), int32(), int64())
  # TODO: test uint types when format allows
  # uint_types <- c(uint8(), uint16(), uint32(), uint64()) # nolint
  for (i in int_types) {
    sch <- schema(fct = dictionary(i, utf8()))
    tab <- Table$create(fact, schema = sch)
    expect_equal(sch, tab$schema)
    if (i != int64()) {
      # TODO: same downcast to int32 as we do for int64() type elsewhere
      expect_equal_data_frame(tab, fact)
    }
  }
})

test_that("Table unifies dictionary on conversion back to R (ARROW-8374)", {
  b1 <- record_batch(f = factor(c("a"), levels = c("a", "b")))
  b2 <- record_batch(f = factor(c("c"), levels = c("c", "d")))
  b3 <- record_batch(f = factor(NA, levels = "a"))
  b4 <- record_batch(f = factor())

  res <- tibble::tibble(f = factor(c("a", "c", NA), levels = c("a", "b", "c", "d")))
  tab <- Table$create(b1, b2, b3, b4)

  expect_equal_data_frame(tab, res)
})

test_that("Table$SelectColumns()", {
  tab <- Table$create(x = 1:10, y = 1:10)

  expect_equal(tab$SelectColumns(0L), Table$create(x = 1:10))

  expect_error(tab$SelectColumns(2:4))
  expect_error(tab$SelectColumns(""))
})

test_that("Table name assignment", {
  tab <- Table$create(x = 1:10, y = 1:10)
  expect_identical(names(tab), c("x", "y"))
  names(tab) <- c("a", "b")
  expect_identical(names(tab), c("a", "b"))
  expect_error(names(tab) <- "f")
  expect_error(names(tab) <- letters)
  expect_error(names(tab) <- character(0))
  expect_error(names(tab) <- NULL)
  expect_error(names(tab) <- c(TRUE, FALSE))
})

test_that("Table$create() with different length columns", {
  msg <- "All columns must have the same length"
  expect_error(Table$create(a = 1:5, b = 1:6), msg)
})

test_that("Table$create() scalar recycling with vectors", {
  expect_equal_data_frame(
    Table$create(a = 1:10, b = 5),
    tibble::tibble(a = 1:10, b = 5)
  )
})

test_that("Table$create() scalar recycling with Scalars, Arrays, and ChunkedArrays", {
  expect_equal_data_frame(
    Table$create(a = Array$create(1:10), b = Scalar$create(5)),
    tibble::tibble(a = 1:10, b = 5)
  )

  expect_equal_data_frame(
    Table$create(a = Array$create(1:10), b = Array$create(5)),
    tibble::tibble(a = 1:10, b = 5)
  )

  expect_equal_data_frame(
    Table$create(a = Array$create(1:10), b = ChunkedArray$create(5)),
    tibble::tibble(a = 1:10, b = 5)
  )
})

test_that("Table$create() no recycling with tibbles", {
  expect_error(
    Table$create(
      tibble::tibble(a = 1:10, b = 5),
      tibble::tibble(a = 1, b = 5)
    ),
    regexp = "All input tibbles or data.frames must have the same number of rows"
  )

  expect_error(
    Table$create(
      tibble::tibble(a = 1:10, b = 5),
      tibble::tibble(a = 1)
    ),
    regexp = "All input tibbles or data.frames must have the same number of rows"
  )
})

test_that("Tables can be combined with concat_tables()", {
  expect_error(
    concat_tables(arrow_table(a = 1:10), arrow_table(a = c("a", "b")), unify_schemas = FALSE),
    regexp = "Schema at index 2 does not match the first schema"
  )

  expect_error(
    concat_tables(arrow_table(a = 1:10), arrow_table(a = c("a", "b")), unify_schemas = TRUE),
    regexp = "Unable to merge: Field a has incompatible types: int32 vs string"
  )
  expect_error(
    concat_tables(),
    regexp = "Must pass at least one Table"
  )

  expect_equal(
    concat_tables(
      arrow_table(a = 1:5),
      arrow_table(a = 6:7, b = c("d", "e"))
    ),
    arrow_table(a = 1:7, b = c(rep(NA, 5), "d", "e"))
  )

  # concat_tables() with one argument returns identical table
  expected <- arrow_table(a = 1:10)
  expect_equal(expected, concat_tables(expected))
})

test_that("Table supports rbind", {
  expect_error(
    rbind(arrow_table(a = 1:10), arrow_table(a = c("a", "b"))),
    regexp = "Schema at index 2 does not match the first schema"
  )

  tables <- list(
    arrow_table(a = 1:10, b = Scalar$create("x")),
    arrow_table(a = 2:42, b = Scalar$create("y")),
    arrow_table(a = 8:10, b = Scalar$create("z"))
  )
  expected <- Table$create(do.call(rbind, lapply(tables, as.data.frame)))
  actual <- do.call(rbind, tables)
  expect_equal(actual, expected, ignore_attr = TRUE)

  # rbind with empty table produces identical table
  expected <- arrow_table(a = 1:10, b = Scalar$create("x"))
  expect_equal(
    rbind(expected, arrow_table(a = integer(0), b = character(0))),
    expected
  )
  # rbind() with one argument returns identical table
  expect_equal(rbind(expected), expected)
})

test_that("Table supports cbind", {
  expect_snapshot_error(
    cbind(
      arrow_table(a = 1:10),
      arrow_table(a = c("a", "b"))
    )
  )
  expect_error(
    cbind(arrow_table(a = 1:10), arrow_table(b = character(0))),
    regexp = "Non-scalar inputs must have an equal number of rows"
  )

  actual <- cbind(
    arrow_table(a = 1:10, b = Scalar$create("x")),
    arrow_table(a = 11:20, b = Scalar$create("y")),
    arrow_table(c = 1:10)
  )
  expected <- arrow_table(cbind(
    tibble::tibble(a = 1:10, b = "x"),
    tibble::tibble(a = 11:20, b = "y"),
    tibble::tibble(c = 1:10)
  ))
  expect_equal(actual, expected, ignore_attr = TRUE)

  # cbind() with one argument returns identical table
  expected <- arrow_table(a = 1:10)
  expect_equal(expected, cbind(expected))

  # Handles Arrow arrays and chunked arrays
  expect_equal(
    cbind(arrow_table(a = 1:2), b = Array$create(4:5)),
    arrow_table(a = 1:2, b = 4:5)
  )
  expect_equal(
    cbind(arrow_table(a = 1:2), b = chunked_array(4, 5)),
    arrow_table(a = 1:2, b = chunked_array(4, 5))
  )

  # Handles data.frame
  if (getRversion() >= "4.0.0") {
    # Prior to R 4.0, cbind would short-circuit to the data.frame implementation
    # if **any** of the arguments are a data.frame.
    expect_equal(
      cbind(arrow_table(a = 1:2), data.frame(b = 4:5)),
      arrow_table(a = 1:2, b = 4:5)
    )
  }

  # Handles factors
  expect_equal(
    cbind(arrow_table(a = 1:2), b = factor(c("a", "b"))),
    arrow_table(a = 1:2, b = factor(c("a", "b")))
  )

  # Handles scalar values
  expect_equal(
    cbind(arrow_table(a = 1:2), b = "x"),
    arrow_table(a = 1:2, b = c("x", "x"))
  )

  # Handles zero rows
  expect_equal(
    cbind(arrow_table(a = character(0)), b = Array$create(numeric(0)), c = integer(0)),
    arrow_table(a = character(0), b = numeric(0), c = integer(0)),
  )

  # Rejects unnamed arrays, even in cases where no named arguments are passed
  expect_error(
    cbind(arrow_table(a = 1:2), b = 3:4, 5:6),
    regexp = "Vector and array arguments must have names"
  )
  expect_error(
    cbind(arrow_table(a = 1:2), 3:4, 5:6),
    regexp = "Vector and array arguments must have names"
  )
})

test_that("cbind.Table handles record batches and tables", {
  # R 3.6 cbind dispatch rules cause cbind to fall back to default impl if
  # there are multiple arguments with distinct cbind implementations
  skip_if(getRversion() < "4.0.0", "R 3.6 cbind dispatch rules prevent this behavior")

  expect_equal(
    cbind(arrow_table(a = 1L:2L), record_batch(b = 4:5)),
    arrow_table(a = 1L:2L, b = 4:5)
  )
})

test_that("ARROW-11769/ARROW-17085 - grouping preserved in table creation", {
  skip_if_not_available("dataset")

  tbl <- tibble::tibble(
    int = 1:10,
    fct = factor(rep(c("A", "B"), 5)),
    fct2 = factor(rep(c("C", "D"), each = 5)),
  )

  expect_identical(
    tbl %>%
      Table$create() %>%
      dplyr::group_vars(),
    dplyr::group_vars(tbl)
  )
  expect_identical(
    tbl %>%
      dplyr::group_by(fct, fct2) %>%
      Table$create() %>%
      dplyr::group_vars(),
    c("fct", "fct2")
  )
})

test_that("ARROW-12729 - length returns number of columns in Table", {
  tbl <- tibble::tibble(
    int = 1:10,
    fct = factor(rep(c("A", "B"), 5)),
    fct2 = factor(rep(c("C", "D"), each = 5)),
  )

  tab <- Table$create(!!!tbl)

  expect_identical(length(tab), 3L)
})

test_that("as_arrow_table() works for Table", {
  table <- arrow_table(col1 = 1L, col2 = "two")
  expect_identical(as_arrow_table(table), table)
  expect_equal(
    as_arrow_table(table, schema = schema(col1 = float64(), col2 = string())),
    arrow_table(col1 = Array$create(1, type = float64()), col2 = "two")
  )
})

test_that("as_arrow_table() works for RecordBatch", {
  table <- arrow_table(col1 = 1L, col2 = "two")
  batch <- record_batch(col1 = 1L, col2 = "two")

  expect_equal(as_arrow_table(batch), table)
  expect_equal(
    as_arrow_table(batch, schema = schema(col1 = float64(), col2 = string())),
    arrow_table(col1 = Array$create(1, type = float64()), col2 = "two")
  )
})

test_that("as_arrow_table() works for data.frame()", {
  table <- arrow_table(col1 = 1L, col2 = "two")
  tbl <- tibble::tibble(col1 = 1L, col2 = "two")

  expect_equal(as_arrow_table(tbl), table)

  expect_equal(
    as_arrow_table(
      tbl,
      schema = schema(col1 = float64(), col2 = string())
    ),
    arrow_table(col1 = Array$create(1, type = float64()), col2 = "two")
  )
})

test_that("as_arrow_table() errors for invalid input", {
  expect_error(
    as_arrow_table("no as_arrow_table() method"),
    class = "arrow_no_method_as_arrow_table"
  )
})

test_that("num_rows method not susceptible to integer overflow", {
  skip_if_not_running_large_memory_tests()

  small_array <- Array$create(raw(1))
  big_array <- Array$create(raw(.Machine$integer.max))
  big_chunked_array <- chunked_array(big_array, small_array)
  # LargeString array with data buffer > MAX_INT32
  big_string_array <- Array$create(make_big_string())

  small_table <- Table$create(col = small_array)
  big_table <- Table$create(col = big_chunked_array)

  expect_type(big_array$nbytes(), "integer")
  expect_type(big_chunked_array$nbytes(), "double")

  expect_type(length(big_array), "integer")
  expect_type(length(big_chunked_array), "double")

  expect_type(small_table$num_rows, "integer")
  expect_type(big_table$num_rows, "double")

  expect_identical(big_string_array$data()$buffers[[3]]$size, 2148007936)
})

test_that("can create empty table from schema", {
  schema <- schema(
    col1 = float64(),
    col2 = string(),
    col3 = vctrs_extension_type(integer())
  )
  out <- Table$create(schema = schema)
  expect_r6_class(out, "Table")
  expect_equal(nrow(out), 0)
  expect_equal(out$schema, schema)
})

test_that("as_arrow_table() errors on data.frame with NULL names", {
  df <- data.frame(a = 1, b = "two")
  names(df) <- NULL
  expect_error(as_arrow_table(df), "Input data frame columns must be named")
})

test_that("# GH-35038 - passing in multiple arguments doesn't affect return type", {

  df <- data.frame(x = 1)
  out1 <- as.data.frame(arrow_table(df, name = "1"))
  out2 <- as.data.frame(arrow_table(name = "1", df))

  expect_s3_class(out1, c("data.frame"), exact = TRUE)
  expect_s3_class(out2, c("data.frame"), exact = TRUE)
})

test_that("as.data.frame() on ArrowTabular objects returns a base R data.frame regardless of input type", {
  df <- data.frame(x = 1)
  out1 <- as.data.frame(arrow_table(df))
  expect_s3_class(out1, "data.frame", exact = TRUE)

  tib <- tibble::tibble(x = 1)
  out2 <- as.data.frame(arrow_table(tib))
  expect_s3_class(out2, "data.frame", exact = TRUE)
})

test_that("collect() on ArrowTabular objects returns a tibble regardless of input type", {
  df <- data.frame(x = 1)
  out1 <- dplyr::collect(arrow_table(df))
  expect_s3_class(out1, c("tbl_df", "tbl", "data.frame"), exact = TRUE)

  tib <- tibble::tibble(x = 1)
  out2 <- dplyr::collect(arrow_table(tib))
  expect_s3_class(out2, c("tbl_df", "tbl", "data.frame"), exact = TRUE)
})
