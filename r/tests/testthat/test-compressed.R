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

test_that("codec_is_available", {
  expect_true(codec_is_available("uncompressed")) # Always true
  expect_match_arg_error(codec_is_available("sdfasdf"))
  skip_if_not_available("gzip")
  expect_true(codec_is_available("gzip"))
  expect_true(codec_is_available("GZIP"))
})

test_that("Compression codecs are included in the Windows build", {
  skip_if(tolower(Sys.info()[["sysname"]]) != "windows")
  expect_true(codec_is_available("lz4"))
  expect_true(codec_is_available("zstd"))
  expect_true(codec_is_available("brotli"))
  expect_true(codec_is_available("bz2"))
  expect_true(codec_is_available("snappy"))
})

test_that("Codec attributes", {
  skip_if_not_available("gzip")
  cod <- Codec$create("gzip")
  expect_equal(cod$name, "gzip")
  # TODO: implement $level
  expect_error(cod$level)
})

test_that("Default compression_level for zstd", {
  skip_if_not_available("zstd")
  cod <- Codec$create("zstd")
  expect_equal(cod$name, "zstd")
  # TODO: implement $level
  expect_error(cod$level)
})

test_that("can write Buffer to CompressedOutputStream and read back in CompressedInputStream", {
  skip_if_not_available("gzip")
  buf <- buffer(as.raw(sample(0:255, size = 1024, replace = TRUE)))

  tf1 <- tempfile()
  stream1 <- CompressedOutputStream$create(tf1)
  expect_equal(stream1$tell(), 0)
  stream1$write(buf)
  expect_equal(stream1$tell(), buf$size)
  stream1$close()

  tf2 <- tempfile()
  sink2 <- FileOutputStream$create(tf2)
  stream2 <- CompressedOutputStream$create(sink2)
  expect_equal(stream2$tell(), 0)
  stream2$write(buf)
  expect_equal(stream2$tell(), buf$size)
  stream2$close()
  sink2$close()

  input1 <- CompressedInputStream$create(tf1)
  buf1 <- input1$Read(1024L)

  file2 <- ReadableFile$create(tf2)
  input2 <- CompressedInputStream$create(file2)
  buf2 <- input2$Read(1024L)

  expect_equal(buf, buf1)
  expect_equal(buf, buf2)

  unlink(tf1)
  unlink(tf2)
})
