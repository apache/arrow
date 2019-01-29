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

context("arrow::io::Compressed.*Stream")

test_that("can write Buffer to CompressedOutputStream and read back in CompressedInputStream", {
  buf <- buffer(as.raw(sample(0:255, size = 1024, replace = TRUE)))

  tf1 <- local_tempfile()
  stream1 <- CompressedOutputStream(tf1)
  stream1$write(buf)
  expect_error(stream1$tell())
  stream1$close()

  tf2 <- local_tempfile()
  sink2 <- FileOutputStream(tf2)
  stream2 <- CompressedOutputStream(sink2)
  stream2$write(buf)
  expect_error(stream2$tell())
  stream2$close()
  sink2$close()


  input1 <- CompressedInputStream(tf1)
  buf1 <- input1$Read(1024L)

  file2 <- ReadableFile(tf2)
  input2 <- CompressedInputStream(file2)
  buf2 <- input2$Read(1024L)

  expect_equal(buf, buf1)
  expect_equal(buf, buf2)
})

