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

library(tibble)

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

  # work with schema to specify timestamp with time zone type
  tbl <- tibble::tibble(time = "1970-01-01T12:00:00+12:00")
  write.csv(tbl, tf, row.names = FALSE)
  df <- read_csv_arrow(tf, col_types = schema(time = timestamp(unit = "us", timezone = "UTC")))
  expect_equal(df, tibble::tibble(time = as.POSIXct("1970-01-01 00:00:00", tz = "UTC")))
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

test_that("read_csv_arrow() and write_csv_arrow() accept connection objects", {
  skip_if_not(CanRunWithCapturedR())

  tf <- tempfile()
  on.exit(unlink(tf))

  # make this big enough that we might expose concurrency problems,
  # but not so big that it slows down the tests
  test_tbl <- tibble::tibble(
    x = 1:1e4,
    y = vapply(x, rlang::hash, character(1), USE.NAMES = FALSE),
    z = vapply(y, rlang::hash, character(1), USE.NAMES = FALSE)
  )

  write_csv_arrow(test_tbl, file(tf))
  expect_identical(read_csv_arrow(tf), test_tbl)
  expect_identical(read_csv_arrow(file(tf)), read_csv_arrow(tf))
})

test_that("CSV reader works on files with non-UTF-8 encoding", {
  strings <- c("a", "\u00e9", "\U0001f4a9")
  file_string <- paste0(
    "col1,col2\n",
    paste(strings, 1:30, sep = ",", collapse = "\n")
  )
  file_bytes_utf16 <- iconv(
    file_string,
    from = Encoding(file_string),
    to = "UTF-16LE",
    toRaw = TRUE
  )[[1]]

  tf <- tempfile()
  on.exit(unlink(tf))
  con <- file(tf, open = "wb")
  writeBin(file_bytes_utf16, con)
  close(con)

  fs <- LocalFileSystem$create()
  reader <- CsvTableReader$create(
    fs$OpenInputStream(tf),
    read_options = CsvReadOptions$create(encoding = "UTF-16LE")
  )

  table <- reader$Read()

  # check that the CSV reader didn't create a binary column because of
  # invalid bytes
  expect_true(table$col1$type == string())

  # check that the bytes are correct
  expect_identical(
    lapply(as.vector(table$col1$cast(binary())), as.raw),
    rep(
      list(as.raw(0x61), as.raw(c(0xc3, 0xa9)), as.raw(c(0xf0, 0x9f, 0x92, 0xa9))),
      10
    )
  )

  # check that the strings are correct
  expect_identical(as.vector(table$col1), rep(strings, 10))
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
    regexp = "x must be an object of class .* not 'Array'."
  )
})

test_that("Write a CSV file with invalid batch size", {
  expect_error(
    write_csv_arrow(tbl_no_dates, csv_file, batch_size = -1),
    regexp = "batch_size not greater than 0"
  )
})

test_that("Write a CSV with custom NA value", {
  tbl_out1 <- write_csv_arrow(tbl_no_dates, csv_file, na = "NULL_VALUE")
  expect_true(file.exists(csv_file))
  expect_identical(tbl_out1, tbl_no_dates)

  csv_contents <- readLines(csv_file)
  expect_true(any(grepl("NULL_VALUE", csv_contents)))

  tbl_in1 <- read_csv_arrow(csv_file, na = "NULL_VALUE")
  expect_identical(tbl_in1, tbl_no_dates)

  # Also can use null_value in CsvWriteOptions
  tbl_out1 <- write_csv_arrow(tbl_no_dates, csv_file,
    write_options = CsvWriteOptions$create(null_string = "another_null")
  )
  csv_contents <- readLines(csv_file)
  expect_true(any(grepl("another_null", csv_contents)))

  tbl_in1 <- read_csv_arrow(csv_file, na = "another_null")
  expect_identical(tbl_in1, tbl_no_dates)

  # Also can use empty string
  write_csv_arrow(tbl_no_dates, csv_file, na = "")
  expect_true(file.exists(csv_file))

  csv_contents <- readLines(csv_file)
  expect_true(any(grepl(",,", csv_contents)))

  tbl_in1 <- read_csv_arrow(csv_file)
  expect_identical(tbl_in1, tbl_no_dates)
})

test_that("Write a CSV file with invalid null value", {
  expect_error(
    write_csv_arrow(tbl_no_dates, csv_file, na = "MY\"VAL"),
    regexp = "must not contain quote characters"
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
      eol = "\n"
    ),
    paste(
      "The following arguments are not yet supported in Arrow: \"append\",",
      "\"quote\", \"escape\", and \"eol\""
    )
  )
})

test_that("write_csv_arrow deals with duplication in sink/file", {
  # errors when both file and sink are supplied
  expect_error(
    write_csv_arrow(tbl, file = csv_file, sink = csv_file),
    paste(
      "You have supplied both \"file\" and \"sink\" arguments. Please",
      "supply only one of them"
    )
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
    paste(
      "You have supplied both \"col_names\" and \"include_header\"",
      "arguments. Please supply only one of them"
    )
  )

  written_tbl <- suppressMessages(
    write_csv_arrow(tbl_no_dates, file = csv_file, col_names = FALSE)
  )
  expect_true(file.exists(csv_file))
  expect_identical(tbl_no_dates, written_tbl)
})

test_that("read_csv_arrow() deals with BOMs (byte-order-marks) correctly", {
  writeLines("\xef\xbb\xbfa,b\n1,2\n", con = csv_file)

  expect_equal(
    read_csv_arrow(csv_file),
    tibble(a = 1, b = 2)
  )
})

test_that("write_csv_arrow can write from Dataset objects", {
  skip_if_not_available("dataset")
  data_dir <- make_temp_dir()
  write_dataset(tbl_no_dates, data_dir, partitioning = "lgl")
  data_in <- open_dataset(data_dir)

  csv_file <- tempfile()
  tbl_out <- write_csv_arrow(data_in, csv_file)
  expect_true(file.exists(csv_file))

  tbl_in <- read_csv_arrow(csv_file)
  expect_named(tbl_in, c("dbl", "false", "chr", "lgl"))
  expect_equal(nrow(tbl_in), 10)
})

test_that("write_csv_arrow can write from RecordBatchReader objects", {
  skip_if_not_available("dataset")
  library(dplyr, warn.conflicts = FALSE)

  query_obj <- arrow_table(tbl_no_dates) %>%
    filter(lgl == TRUE)

  csv_file <- tempfile()
  on.exit(unlink(csv_file))
  tbl_out <- write_csv_arrow(query_obj, csv_file)
  expect_true(file.exists(csv_file))

  tbl_in <- read_csv_arrow(csv_file)
  expect_named(tbl_in, c("dbl", "lgl", "false", "chr"))
  expect_equal(nrow(tbl_in), 3)
})

test_that("read/write compressed file successfully", {
  skip_if_not_available("gzip")
  tfgz <- tempfile(fileext = ".csv.gz")
  tf <- tempfile(fileext = ".csv")

  write_csv_arrow(tbl, tf)
  write_csv_arrow(tbl, tfgz)
  expect_lt(file.size(tfgz), file.size(tf))

  expect_identical(
    read_csv_arrow(tfgz),
    tbl
  )
  skip_if_not_available("lz4")
  tflz4 <- tempfile(fileext = ".csv.lz4")
  write_csv_arrow(tbl, tflz4)
  expect_false(file.size(tfgz) == file.size(tflz4))
  expect_identical(
    read_csv_arrow(tflz4),
    tbl
  )
})

test_that("read/write compressed filesystem path", {
  skip_if_not_available("zstd")
  tfzst <- tempfile(fileext = ".csv.zst")
  fs <- LocalFileSystem$create()$path(tfzst)
  write_csv_arrow(tbl, fs)

  tf <- tempfile(fileext = ".csv")
  write_csv_arrow(tbl, tf)
  expect_lt(file.size(tfzst), file.size(tf))
  expect_identical(
    read_csv_arrow(fs),
    tbl
  )
})

test_that("read_csv_arrow() can read sub-second timestamps with col_types T setting (ARROW-15599)", {
  tbl <- tibble::tibble(time = c("2018-10-07 19:04:05.000", "2018-10-07 19:04:05.001"))
  tf <- tempfile()
  on.exit(unlink(tf))
  write.csv(tbl, tf, row.names = FALSE)

  df <- read_csv_arrow(tf, col_types = "T", col_names = "time", skip = 1)
  expected <- as.POSIXct(tbl$time, tz = "UTC")
  expect_equal(df$time, expected, ignore_attr = "tzone")
})

test_that("Shows an error message when trying to read a timestamp with time zone with col_types = T (ARROW-17429)", {
  tbl <- tibble::tibble(time = c("1970-01-01T12:00:00+12:00"))
  csv_file <- tempfile()
  on.exit(unlink(csv_file))
  write.csv(tbl, csv_file, row.names = FALSE)

  expect_error(
    read_csv_arrow(csv_file, col_types = "T", col_names = "time", skip = 1),
    "CSV conversion error to timestamp\\[ns\\]: expected no zone offset in"
  )
})

test_that("CSV reading/parsing/convert options can be passed in as lists", {
  tf <- tempfile()
  on.exit(unlink(tf))

  writeLines('"x"\nNA\nNA\n"NULL"\n\n"foo"\n', tf)

  tab1 <- read_csv_arrow(
    tf,
    convert_options = list(null_values = c("NA", "NULL"), strings_can_be_null = TRUE),
    parse_options = list(ignore_empty_lines = FALSE),
    read_options = list(skip_rows = 1L)
  )

  tab2 <- read_csv_arrow(
    tf,
    convert_options = CsvConvertOptions$create(null_values = c(NA, "NA", "NULL"), strings_can_be_null = TRUE),
    parse_options = CsvParseOptions$create(ignore_empty_lines = FALSE),
    read_options = CsvReadOptions$create(skip_rows = 1L)
  )

  expect_equal(tab1, tab2)
})

test_that("Read literal data directly", {
  expected <- tibble::tibble(x = c(1L, 3L), y = c(2L, 4L))

  expect_identical(read_csv_arrow(I("x,y\n1,2\n3,4")), expected)
  expect_identical(read_csv_arrow(I("x,y\r1,2\r3,4")), expected)
  expect_identical(read_csv_arrow(I("x,y\n\r1,2\n\r3,4")), expected)
  expect_identical(read_csv_arrow(charToRaw("x,y\n1,2\n3,4")), expected)
  expect_identical(read_csv_arrow(I(charToRaw("x,y\n1,2\n3,4"))), expected)
  expect_identical(read_csv_arrow(I(c("x,y", "1,2", "3,4"))), expected)
})

test_that("skip_rows and skip_rows_after_names option", {
  txt_raw <- charToRaw(paste0(c("a", 1:4), collapse = "\n"))

  expect_identical(
    read_csv_arrow(
      txt_raw,
      read_options = list(skip_rows_after_names = 1)
    ),
    tibble::tibble(a = 2:4)
  )
  expect_identical(
    read_csv_arrow(
      txt_raw,
      read_options = list(skip_rows_after_names = 10)
    ),
    tibble::tibble(a = vctrs::unspecified())
  )
  expect_identical(
    read_csv_arrow(
      txt_raw,
      read_options = list(skip = 1, skip_rows_after_names = 1)
    ),
    tibble::tibble(`1` = 3:4)
  )
})
