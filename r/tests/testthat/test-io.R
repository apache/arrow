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

test_that("RandomAccessFile$ReadMetadata() works for LocalFileSystem", {
  fs <- LocalFileSystem$create()
  tf <- tempfile()
  on.exit(unlink(tf))
  write("abcdefg", tf)

  expect_identical(
    fs$OpenInputFile(tf)$ReadMetadata(),
    list()
  )
})

test_that("reencoding input stream works for windows-1252", {
  string <- "province_name\nQu\u00e9bec"
  bytes_windows1252 <- iconv(
    string,
    from = Encoding(string),
    to = "windows-1252",
    toRaw = TRUE
  )[[1]]

  bytes_utf8 <- iconv(
    string,
    from = Encoding(string),
    to = "UTF-8",
    toRaw = TRUE
  )[[1]]

  temp_windows1252 <- tempfile()
  con <- file(temp_windows1252, open = "wb")
  writeBin(bytes_windows1252, con)
  close(con)

  fs <- LocalFileSystem$create()

  stream <- fs$OpenInputStream(temp_windows1252)
  stream_utf8 <- MakeReencodeInputStream(stream, "windows-1252")
  expect_identical(as.raw(stream_utf8$Read(100)), bytes_utf8)
  stream$close()
  stream_utf8$close()

  unlink(temp_windows1252)
})

test_that("reencoding input stream works for UTF-16", {
  string <- paste0(strrep("a\u00e9\U0001f4a9", 30))
  bytes_utf16 <- iconv(
    string,
    from = Encoding(string),
    to = "UTF-16LE",
    toRaw = TRUE
  )[[1]]

  bytes_utf8 <- iconv(
    string,
    from = Encoding(string),
    to = "UTF-8",
    toRaw = TRUE
  )[[1]]

  temp_utf16 <- tempfile()
  con <- file(temp_utf16, open = "wb")
  writeBin(bytes_utf16, con)
  close(con)

  fs <- LocalFileSystem$create()

  stream <- fs$OpenInputStream(temp_utf16)
  stream_utf8 <- MakeReencodeInputStream(stream, "UTF-16LE")

  expect_identical(
    as.raw(stream_utf8$Read(length(bytes_utf8))),
    bytes_utf8
  )

  stream_utf8$close()
  stream$close()
  unlink(temp_utf16)
})

test_that("reencoding input stream works with pending characters", {
  string <- paste0(strrep("a\u00e9\U0001f4a9", 30))
  bytes_utf8 <- iconv(
    string,
    from = Encoding(string),
    to = "UTF-8",
    toRaw = TRUE
  )[[1]]

  temp_utf8 <- tempfile()
  con <- file(temp_utf8, open = "wb")
  writeBin(bytes_utf8, con)
  close(con)

  fs <- LocalFileSystem$create()

  stream <- fs$OpenInputStream(temp_utf8)
  stream_utf8 <- MakeReencodeInputStream(stream, "UTF-8")

  # these calls all leave some pending characters
  expect_identical(as.raw(stream_utf8$Read(4)), bytes_utf8[1:4])
  expect_identical(as.raw(stream_utf8$Read(5)), bytes_utf8[5:9])
  expect_identical(as.raw(stream_utf8$Read(6)), bytes_utf8[10:15])
  expect_identical(as.raw(stream_utf8$Read(7)), bytes_utf8[16:22])

  # finish the stream
  expect_identical(
    as.raw(stream_utf8$Read(length(bytes_utf8))),
    bytes_utf8[23:length(bytes_utf8)]
  )

  stream$close()
  stream_utf8$close()

  unlink(temp_utf8)
})

test_that("reencoding input stream errors for invalid characters", {
  bytes_utf8 <- rep(as.raw(0xff), 10)

  temp_utf8 <- tempfile()
  con <- file(temp_utf8, open = "wb")
  writeBin(bytes_utf8, con)
  close(con)

  fs <- LocalFileSystem$create()

  stream <- fs$OpenInputStream(temp_utf8)
  stream_utf8 <- MakeReencodeInputStream(stream, "UTF-8")
  expect_error(stream_utf8$Read(100), "Encountered invalid input bytes")

  unlink(temp_utf8)
})
