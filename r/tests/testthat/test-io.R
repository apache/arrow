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

test_that("reencoding input stream works", {
  string <- "province_name\nQu\u00e9bec"
  bytes_windows1252 <- iconv(string, to = "windows-1252", toRaw = TRUE)[[1]]

  temp_windows1252 <- tempfile()
  con <- file(temp_windows1252, open = "wb")
  writeBin(bytes_windows1252, con)
  close(con)

  fs <- LocalFileSystem$create()

  stream <- fs$OpenInputStream(temp_windows1252)
  expect_identical(
    as.raw(stream$Read(length(bytes_windows1252))),
    bytes_windows1252
  )
  stream$close()

  stream <- fs$OpenInputStream(temp_windows1252)
  stream_utf8 <- MakeRencodeInputStream(stream, "windows-1252")
  expect_identical(as.raw(stream_utf8$Read(100)), bytes_utf8)
  stream$close()
  stream_utf8$close()

  unlink(temp_windows1252)
})
