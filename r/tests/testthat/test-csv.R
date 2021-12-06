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

# Not all types round trip via CSV 100% identical by default
tbl <- example_data[, c("dbl", "lgl", "false", "chr")]
tbl_no_dates <- tbl
# Add a date to test its parsing
tbl$date <- Sys.Date() + 1:10

csv_file <- tempfile()

test_that("Can read csv file", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.csv(tbl, tf, row.names = FALSE)

  tab0 <- Table$create(tbl)
  tab1 <- read_csv_arrow(tf, as_data_frame = FALSE)
  expect_equal(tab0, tab1)
  tab2 <- read_csv_arrow(mmap_open(tf), as_data_frame = FALSE)
  expect_equal(tab0, tab2)
  tab3 <- read_csv_arrow(ReadableFile$create(tf), as_data_frame = FALSE)
  expect_equal(tab0, tab3)
})

test_that("read_csv_arrow(as_data_frame=TRUE)", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.csv(tbl, tf, row.names = FALSE)
  tab1 <- read_csv_arrow(tf, as_data_frame = TRUE)
  expect_equal(tbl, tab1)
})

test_that("read_delim_arrow parsing options: delim", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.table(tbl, tf, sep = "\t", row.names = FALSE)
  tab1 <- read_tsv_arrow(tf)
  tab2 <- read_delim_arrow(tf, delim = "\t")
  expect_equal(tab1, tab2)
  expect_equal(tbl, tab1)
})

test_that("read_delim_arrow parsing options: quote", {
  tf <- tempfile()
  on.exit(unlink(tf))

  df <- data.frame(a = c(1, 2), b = c("'abc'", "'def'"))
  write.table(df, sep = ";", tf, row.names = FALSE, quote = FALSE)
  tab1 <- read_delim_arrow(tf, delim = ";", quote = "'")

  # Is this a problem?
  # Component “a”: target is integer64, current is numeric
  tab1$a <- as.numeric(tab1$a)
  expect_equal(
    tab1,
    tibble::tibble(a = c(1, 2), b = c("abc", "def"))
  )
})

test_that("read_csv_arrow parsing options: col_names", {
  tf <- tempfile()
  on.exit(unlink(tf))

  # Writing the CSV without the header
  write.table(tbl, tf, sep = ",", row.names = FALSE, col.names = FALSE)

  # Reading with col_names = FALSE autogenerates names
  no_names <- read_csv_arrow(tf, col_names = FALSE)
  expect_equal(no_names$f0, tbl[[1]])

  tab1 <- read_csv_arrow(tf, col_names = names(tbl))

  expect_identical(names(tab1), names(tbl))
  expect_equal(tbl, tab1)

  # This errors (correctly) because I haven't given enough names
  # but the error message is "Invalid: Empty CSV file", which is not accurate
  expect_error(
    read_csv_arrow(tf, col_names = names(tbl)[1])
  )
  # Same here
  expect_error(
    read_csv_arrow(tf, col_names = c(names(tbl), names(tbl)))
  )
})

test_that("read_csv_arrow parsing options: skip", {
  tf <- tempfile()
  on.exit(unlink(tf))

  # Adding two garbage lines to start the csv
  cat("asdf\nqwer\n", file = tf)
  suppressWarnings(write.table(tbl, tf, sep = ",", row.names = FALSE, append = TRUE))

  tab1 <- read_csv_arrow(tf, skip = 2)

  expect_identical(names(tab1), names(tbl))
  expect_equal(tbl, tab1)
})

test_that("read_csv_arrow parsing options: skip_empty_rows", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.csv(tbl, tf, row.names = FALSE)
  cat("\n\n", file = tf, append = TRUE)

  tab1 <- read_csv_arrow(tf, skip_empty_rows = FALSE)

  expect_equal(nrow(tab1), nrow(tbl) + 2)
  expect_true(is.na(tail(tab1, 1)[[1]]))
})

test_that("read_csv_arrow parsing options: na strings", {
  tf <- tempfile()
  on.exit(unlink(tf))

  df <- data.frame(
    a = c(1.2, NA, NA, 3.4),
    b = c(NA, "B", "C", NA),
    stringsAsFactors = FALSE
  )
  write.csv(df, tf, row.names = FALSE)
  expect_equal(grep("NA", readLines(tf)), 2:5)

  tab1 <- read_csv_arrow(tf)
  expect_equal(is.na(tab1$a), is.na(df$a))
  expect_equal(is.na(tab1$b), is.na(df$b))

  unlink(tf) # Delete and write to the same file name again

  write.csv(df, tf, row.names = FALSE, na = "asdf")
  expect_equal(grep("asdf", readLines(tf)), 2:5)

  tab2 <- read_csv_arrow(tf, na = "asdf")
  expect_equal(is.na(tab2$a), is.na(df$a))
  expect_equal(is.na(tab2$b), is.na(df$b))
})

test_that("read_csv_arrow() respects col_select", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.csv(tbl, tf, row.names = FALSE, quote = FALSE)

  tab <- read_csv_arrow(tf, col_select = ends_with("l"), as_data_frame = FALSE)
  expect_equal(tab, Table$create(example_data[, c("dbl", "lgl")]))

  tib <- read_csv_arrow(tf, col_select = ends_with("l"), as_data_frame = TRUE)
  expect_equal(tib, example_data[, c("dbl", "lgl")])
})

test_that("read_csv_arrow() can detect compression from file name", {
  skip_if_not_available("gzip")
  tf <- tempfile(fileext = ".csv.gz")
  on.exit(unlink(tf))

  write.csv(tbl, gzfile(tf), row.names = FALSE, quote = FALSE)
  tab1 <- read_csv_arrow(tf)
  expect_equal(tbl, tab1)
})

test_that("read_csv_arrow(schema=)", {
  tbl <- example_data[, "int"]
  tf <- tempfile()
  on.exit(unlink(tf))
  write.csv(tbl, tf, row.names = FALSE)

  df <- read_csv_arrow(tf, schema = schema(int = float64()), skip = 1)
  expect_identical(df, tibble::tibble(int = as.numeric(tbl$int)))
})

test_that("read_csv_arrow(col_types = <Schema>)", {
  tbl <- example_data[, "int"]
  tf <- tempfile()
  on.exit(unlink(tf))
  write.csv(tbl, tf, row.names = FALSE)

  df <- read_csv_arrow(tf, col_types = schema(int = float64()))
  expect_identical(df, tibble::tibble(int = as.numeric(tbl$int)))
})

test_that("read_csv_arrow(col_types=string, col_names)", {
  tbl <- example_data[, "int"]
  tf <- tempfile()
  on.exit(unlink(tf))
  write.csv(tbl, tf, row.names = FALSE)

  df <- read_csv_arrow(tf, col_names = "int", col_types = "d", skip = 1)
  expect_identical(df, tibble::tibble(int = as.numeric(tbl$int)))

  expect_error(read_csv_arrow(tf, col_types = c("i", "d")))
  expect_error(read_csv_arrow(tf, col_types = "d"))
  expect_error(read_csv_arrow(tf, col_types = "i", col_names = c("a", "b")))
  expect_error(read_csv_arrow(tf, col_types = "y", col_names = "a"))
})

test_that("read_csv_arrow() can read timestamps", {
  tbl <- tibble::tibble(time = as.POSIXct("2020-07-20 16:20", tz = "UTC"))
  tf <- tempfile()
  on.exit(unlink(tf))
  write.csv(tbl, tf, row.names = FALSE)

  df <- read_csv_arrow(tf, col_types = schema(time = timestamp()))
  # time zones are being read in as time zone-naive, hence ignore_attr = "tzone"
  expect_equal(tbl, df, ignore_attr = "tzone")

  df <- read_csv_arrow(tf, col_types = "T", col_names = "time", skip = 1)
  expect_equal(tbl, df, ignore_attr = "tzone")
})

test_that("read_csv_arrow(timestamp_parsers=)", {
  tf <- tempfile()
  on.exit(unlink(tf))
  tbl <- tibble::tibble(time = "23/09/2020")
  write.csv(tbl, tf, row.names = FALSE)

  df <- read_csv_arrow(
    tf,
    col_types = schema(time = timestamp()),
    timestamp_parsers = "%d/%m/%Y"
  )
  # time zones are being read in as time zone-naive, hence ignore_attr = "tzone"
  expected <- as.POSIXct(tbl$time, format = "%d/%m/%Y", tz = "UTC")
  expect_equal(df$time, expected, ignore_attr = "tzone")
})

test_that("Skipping columns with null()", {
  tf <- tempfile()
  on.exit(unlink(tf))
  cols <- c("dbl", "lgl", "false", "chr")
  tbl <- example_data[, cols]
  write.csv(tbl, tf, row.names = FALSE)

  df <- read_csv_arrow(tf, col_types = "d-_c", col_names = cols, skip = 1)
  expect_identical(df, tbl[, c("dbl", "chr")])
})

test_that("Mix of guessing and declaring types", {
  tf <- tempfile()
  on.exit(unlink(tf))
  cols <- c("dbl", "lgl", "false", "chr")
  tbl <- example_data[, cols]
  write.csv(tbl, tf, row.names = FALSE)

  tab <- read_csv_arrow(tf, col_types = schema(dbl = float32()), as_data_frame = FALSE)
  expect_equal(tab$schema, schema(dbl = float32(), lgl = bool(), false = bool(), chr = utf8()))

  df <- read_csv_arrow(tf, col_types = "d-?c", col_names = cols, skip = 1)
  expect_identical(df, tbl[, c("dbl", "false", "chr")])
})

test_that("more informative error when reading a CSV with headers and schema", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.csv(example_data, tf, row.names = FALSE)

  share_schema <- schema(
    int = int32(),
    dbl = float64(),
    dbl2 = float64(),
    lgl = boolean(),
    false = boolean(),
    chr = utf8(),
    fct = utf8()
  )

  expect_error(
    read_csv_arrow(tf, schema = share_schema),
    "header row"
  )
})

test_that("Write a CSV file with header", {
  tbl_out <- write_csv_arrow(tbl_no_dates, csv_file)
  expect_true(file.exists(csv_file))
  expect_identical(tbl_out, tbl_no_dates)

  tbl_in <- read_csv_arrow(csv_file)
  expect_identical(tbl_in, tbl_no_dates)

  tbl_out <- write_csv_arrow(tbl, csv_file)
  expect_true(file.exists(csv_file))
  expect_identical(tbl_out, tbl)

  tbl_in <- read_csv_arrow(csv_file)
  expect_identical(tbl_in, tbl)
})


test_that("Write a CSV file with no header", {
  tbl_out <- write_csv_arrow(tbl_no_dates, csv_file, include_header = FALSE)
  expect_true(file.exists(csv_file))
  expect_identical(tbl_out, tbl_no_dates)
  tbl_in <- read_csv_arrow(csv_file, col_names = FALSE)

  tbl_expected <- tbl_no_dates
  names(tbl_expected) <- c("f0", "f1", "f2", "f3")

  expect_identical(tbl_in, tbl_expected)
})

test_that("Write a CSV file with different batch sizes", {
  tbl_out1 <- write_csv_arrow(tbl_no_dates, csv_file, batch_size = 1)
  expect_true(file.exists(csv_file))
  expect_identical(tbl_out1, tbl_no_dates)
  tbl_in1 <- read_csv_arrow(csv_file)
  expect_identical(tbl_in1, tbl_no_dates)

  tbl_out2 <- write_csv_arrow(tbl_no_dates, csv_file, batch_size = 2)
  expect_true(file.exists(csv_file))
  expect_identical(tbl_out2, tbl_no_dates)
  tbl_in2 <- read_csv_arrow(csv_file)
  expect_identical(tbl_in2, tbl_no_dates)

  tbl_out3 <- write_csv_arrow(tbl_no_dates, csv_file, batch_size = 12)
  expect_true(file.exists(csv_file))
  expect_identical(tbl_out3, tbl_no_dates)
  tbl_in3 <- read_csv_arrow(csv_file)
  expect_identical(tbl_in3, tbl_no_dates)
})

test_that("Write a CSV file with invalid input type", {
  bad_input <- Array$create(1:5)
  expect_error(
    write_csv_arrow(bad_input, csv_file),
    regexp = "x must be an object of class 'data.frame', 'RecordBatch', or 'Table', not 'Array'."
  )
})

test_that("Write a CSV file with invalid batch size", {
  expect_error(
    write_csv_arrow(tbl_no_dates, csv_file, batch_size = -1),
    regexp = "batch_size not greater than 0"
  )
})

test_that("time mapping work as expected (ARROW-13624)", {
  tbl <- tibble::tibble(
    dt = as.POSIXct(c("2020-07-20 16:20", NA), tz = "UTC"),
    time = c(hms::as_hms("16:20:00"), NA)
  )
  tf <- tempfile()
  on.exit(unlink(tf))
  write.csv(tbl, tf, row.names = FALSE)

  df <- read_csv_arrow(tf,
    col_names = c("dt", "time"),
    col_types = "Tt",
    skip = 1
  )

  expect_error(
    read_csv_arrow(tf,
      col_names = c("dt", "time"),
      col_types = "tT", skip = 1
    )
  )

  expect_equal(df, tbl, ignore_attr = "tzone")
})

test_that("Writing a CSV errors when unsupported (yet) readr args are used", {
  expect_error(
    write_csv_arrow(tbl, csv_file, append = FALSE),
    "The following argument is not yet supported in Arrow: \"append\""
  )
  expect_error(
    write_csv_arrow(tbl, csv_file, quote = "all"),
    "The following argument is not yet supported in Arrow: \"quote\""
  )
  expect_error(
    write_csv_arrow(tbl, csv_file, escape = "double"),
    "The following argument is not yet supported in Arrow: \"escape\""
  )
  expect_error(
    write_csv_arrow(tbl, csv_file, eol = "\n"),
    "The following argument is not yet supported in Arrow: \"eol\""
  )
  expect_error(
    write_csv_arrow(tbl, csv_file, num_threads = 8),
    "The following argument is not yet supported in Arrow: \"num_threads\""
  )
  expect_error(
    write_csv_arrow(tbl, csv_file, progress = FALSE),
    "The following argument is not yet supported in Arrow: \"progress\""
  )
  expect_error(
    write_csv_arrow(tbl, csv_file, append = FALSE, eol = "\n"),
    "The following arguments are not yet supported in Arrow: \"append\" and \"eol\""
  )
  expect_error(
    write_csv_arrow(
      tbl,
      csv_file,
      append = FALSE,
      quote = "all",
      escape = "double",
      eol = "\n", ),
    paste("The following arguments are not yet supported in Arrow: \"append\",",
          "\"quote\", \"escape\", and \"eol\"")
  )
})

test_that("write_csv_arrow deals with duplication in sink/file", {
  # errors when both file and sink are supplied
  expect_error(
    write_csv_arrow(tbl, file = csv_file, sink = csv_file),
    paste("You have supplied both \"file\" and \"sink\" arguments. Please",
          "supply only one of them")
  )
})

test_that("write_csv_arrow deals with duplication in include_headers/col_names", {
  expect_error(
    write_csv_arrow(
      tbl,
      file = csv_file,
      include_header = TRUE,
      col_names = TRUE
    ),
    paste("You have supplied both \"col_names\" and \"include_header\"",
          "arguments. Please supply only one of them")
  )

  written_tbl <- suppressMessages(
    write_csv_arrow(tbl_no_dates, file = csv_file, col_names = FALSE)
  )
  expect_true(file.exists(csv_file))
  expect_identical(tbl_no_dates, written_tbl)

})
