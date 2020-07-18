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

context("Table")

test_that("read_table handles various input streams (ARROW-3450, ARROW-3505)", {
  tbl <- tibble::tibble(
    int = 1:10, dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10]
  )
  tab <- Table$create(!!!tbl)

  tf <- tempfile()
  on.exit(unlink(tf))
  expect_deprecated(
    write_arrow(tab, tf),
    "write_feather"
  )

  tab1 <- read_feather(tf, as_data_frame = FALSE)
  tab2 <- read_feather(normalizePath(tf), as_data_frame = FALSE)

  readable_file <- ReadableFile$create(tf)
  expect_deprecated(
    tab3 <- read_arrow(readable_file, as_data_frame = FALSE),
    "read_feather"
  )
  readable_file$close()

  mmap_file <- mmap_open(tf)
  mmap_file$close()

  expect_equal(tab, tab1)
  expect_equal(tab, tab2)
  expect_equal(tab, tab3)
})

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
  tab <- Table$create(x = 1:10, y  = 1:10)

  expect_equal(tab$field(0), field("x", int32()))

  # input validation
  expect_error(tab$column(NA), "'i' cannot be NA")
  expect_error(tab$column(-1), "subscript out of bounds")
  expect_error(tab$column(1000), "subscript out of bounds")
  expect_error(tab$column(1:2), class = "Rcpp::not_compatible")
  expect_error(tab$column("one"), class = "Rcpp::not_compatible")

  expect_error(tab$field(NA), "'i' cannot be NA")
  expect_error(tab$field(-1), "subscript out of bounds")
  expect_error(tab$field(1000), "subscript out of bounds")
  expect_error(tab$field(1:2), class = "Rcpp::not_compatible")
  expect_error(tab$field("one"), class = "Rcpp::not_compatible")
})

test_that("[, [[, $ for Table", {
  tbl <- tibble::tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10],
    fct = factor(letters[1:10])
  )
  tab <- Table$create(tbl)

  expect_identical(names(tab), names(tbl))

  expect_data_frame(tab[6:7,], tbl[6:7,])
  expect_data_frame(tab[6:7, 2:4], tbl[6:7, 2:4])
  expect_data_frame(tab[, c("dbl", "fct")], tbl[, c(2, 5)])
  expect_vector(tab[, "chr", drop = TRUE], tbl$chr)
  # Take within a single chunk
  expect_data_frame(tab[c(7, 3, 5), 2:4], tbl[c(7, 3, 5), 2:4])
  expect_data_frame(tab[rep(c(FALSE, TRUE), 5),], tbl[c(2, 4, 6, 8, 10),])
  # bool ChunkedArray (with one chunk)
  expect_data_frame(tab[tab$lgl,], tbl[tbl$lgl,])
  # ChunkedArray with multiple chunks
  c1 <- c(TRUE, FALSE, TRUE, TRUE, FALSE)
  c2 <- c(FALSE, FALSE, TRUE, TRUE, FALSE)
  ca <- ChunkedArray$create(c1, c2)
  expect_data_frame(tab[ca,], tbl[c(1, 3, 4, 8, 9),])
  # int Array
  expect_data_frame(tab[Array$create(5:6), 2:4], tbl[6:7, 2:4])
  # ChunkedArray
  expect_data_frame(tab[ChunkedArray$create(5L, 6L), 2:4], tbl[6:7, 2:4])
  # Expression
  expect_data_frame(tab[tab$int > 6,], tbl[tbl$int > 6,])

  expect_vector(tab[["int"]], tbl$int)
  expect_vector(tab$int, tbl$int)
  expect_vector(tab[[4]], tbl$chr)
  expect_null(tab$qwerty)
  expect_null(tab[["asdf"]])
  # List-like column slicing
  expect_data_frame(tab[2:4], tbl[2:4])
  expect_data_frame(tab[c(1, 0)], tbl[c(1, 0)])

  expect_error(tab[[c(4, 3)]], class = "Rcpp::not_compatible")
  expect_error(tab[[NA]], "'i' must be character or numeric, not logical")
  expect_error(tab[[NULL]], "'i' must be character or numeric, not NULL")
  expect_error(tab[[c("asdf", "jkl;")]], 'length(name) not equal to 1', fixed = TRUE)
  expect_error(tab[-3], "Selections can't have negative value") # From tidyselect
  expect_error(tab[-3:3], "Selections can't have negative value") # From tidyselect
  expect_error(tab[1000]) # This is caught in vctrs, assert more specifically when it stabilizes
  expect_error(tab[1:1000]) # same as ^

  skip("Table with 0 cols doesn't know how many rows it should have")
  expect_data_frame(tab[0], tbl[0])
})

test_that("Table$Slice", {
  tab2 <- tab$Slice(5)
  expect_data_frame(tab2, tbl[6:10,])

  tab3 <- tab$Slice(5, 2)
  expect_data_frame(tab3, tbl[6:7,])

  # Input validation
  expect_error(tab$Slice("ten"), class = "Rcpp::not_compatible")
  expect_error(tab$Slice(NA_integer_), "Slice 'offset' cannot be NA")
  expect_error(tab$Slice(NA), "Slice 'offset' cannot be NA")
  expect_error(tab$Slice(10, "ten"), class = "Rcpp::not_compatible")
  expect_error(tab$Slice(10, NA_integer_), "Slice 'length' cannot be NA")
  expect_error(tab$Slice(NA_integer_, NA_integer_), "Slice 'offset' cannot be NA")
  expect_error(tab$Slice(c(10, 10)), class = "Rcpp::not_compatible")
  expect_error(tab$Slice(10, c(10, 10)), class = "Rcpp::not_compatible")
  expect_error(tab$Slice(1000), "Slice 'offset' greater than array length")
  expect_error(tab$Slice(-1), "Slice 'offset' cannot be negative")
  expect_error(tab3$Slice(10, 10), "Slice 'offset' greater than array length")
  expect_error(tab$Slice(10, -1), "Slice 'length' cannot be negative")
  expect_error(tab$Slice(-1, 10), "Slice 'offset' cannot be negative")
})

test_that("head and tail on Table", {
  tbl <- tibble::tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10],
    fct = factor(letters[1:10])
  )
  tab <- Table$create(tbl)

  expect_data_frame(head(tab), head(tbl))
  expect_data_frame(head(tab, 4), head(tbl, 4))
  expect_data_frame(head(tab, 40), head(tbl, 40))
  expect_data_frame(head(tab, -4), head(tbl, -4))
  expect_data_frame(head(tab, -40), head(tbl, -40))
  expect_data_frame(tail(tab), tail(tbl))
  expect_data_frame(tail(tab, 4), tail(tbl, 4))
  expect_data_frame(tail(tab, 40), tail(tbl, 40))
  expect_data_frame(tail(tab, -4), tail(tbl, -4))
  expect_data_frame(tail(tab, -40), tail(tbl, -40))
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
  tbl <- tibble::tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10],
    fct = factor(letters[1:10])
  )
  tab <- Table$create(tbl)

  expect_identical(dim(tbl), dim(tab))
  expect_is(tab$columns, "list")
  expect_equal(tab$columns[[1]], tab[[1]])
})

test_that("table() handles record batches with splicing", {
  batch <- record_batch(x = 1:2, y = letters[1:2])
  tab <- Table$create(batch, batch, batch)
  expect_equal(tab$schema, batch$schema)
  expect_equal(tab$num_rows, 6L)
  expect_equivalent(
    as.data.frame(tab),
    vctrs::vec_rbind(as.data.frame(batch), as.data.frame(batch), as.data.frame(batch))
  )

  batches <- list(batch, batch, batch)
  tab <- Table$create(!!!batches)
  expect_equal(tab$schema, batch$schema)
  expect_equal(tab$num_rows, 6L)
  expect_equivalent(
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
  res <- as.data.frame(tab)
  expect_equal(names(res), c("a", "b", "c", "x", "y"))
  expect_equal(res,
    tibble::tibble(a = 1:10, b = 1:10, c = v, x = 1:10, y = letters[1:10])
  )
})

test_that("table() auto splices (ARROW-5718)", {
  df <- tibble::tibble(x = 1:10, y = letters[1:10])

  tab1 <- Table$create(df)
  tab2 <- Table$create(!!!df)
  expect_equal(tab1, tab2)
  expect_equal(tab1$schema, schema(x = int32(), y = utf8()))
  expect_equivalent(as.data.frame(tab1), df)

  s <- schema(x = float64(), y = utf8())
  tab3 <- Table$create(df, schema = s)
  tab4 <- Table$create(!!!df, schema = s)
  expect_equal(tab3, tab4)
  expect_equal(tab3$schema, s)
  expect_equivalent(as.data.frame(tab3), df)
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
  tab2 <- Table$create(x = 1:2, y = c("a", "b"),
                       schema = tab1$schema$WithMetadata(list(some="metadata")))

  expect_is(tab1, "Table")
  expect_is(tab2, "Table")
  expect_false(tab1$schema$HasMetadata)
  expect_true(tab2$schema$HasMetadata)
  expect_identical(tab2$schema$metadata, list(some = "metadata"))

  expect_true(tab1 == tab2)
  expect_true(tab1$Equals(tab2))
  expect_false(tab1$Equals(tab2, check_metadata = TRUE))

  expect_failure(expect_equal(tab1, tab2))  # expect_equal has check_metadata=TRUE
  expect_equivalent(tab1, tab2)  # expect_equivalent has check_metadata=FALSE

  expect_false(tab1$Equals(24)) # Not a Table
})

test_that("Table handles null type (ARROW-7064)", {
  tab <- Table$create(a = 1:10, n = vctrs::unspecified(10))
  expect_equivalent(tab$schema, schema(a = int32(), n = null()))
})

test_that("Can create table with specific dictionary types", {
  fact <- example_data[,"fct"]
  int_types <- c(int8(), int16(), int32(), int64())
  # TODO: test uint types when format allows
  # uint_types <- c(uint8(), uint16(), uint32(), uint64())
  for (i in int_types) {
    sch <- schema(fct = dictionary(i, utf8()))
    tab <- Table$create(fact, schema = sch)
    expect_equal(sch, tab$schema)
    if (i != int64()) {
      # TODO: same downcast to int32 as we do for int64() type elsewhere
      expect_identical(as.data.frame(tab), fact)
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

  expect_identical(as.data.frame(tab), res)
})
