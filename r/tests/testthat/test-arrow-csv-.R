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

context("arrow::csv::TableReader")

test_that("Can read csv file", {
  tf <- local_tempfile()
  write.csv(iris, tf, row.names = FALSE, quote = FALSE)

  tab1 <- read_csv_arrow(tf)
  tab2 <- read_csv_arrow(mmap_open(tf))
  tab3 <- read_csv_arrow(ReadableFile(tf))

  iris$Species <- as.character(iris$Species)
  tab0 <- table(iris)
  expect_equal(tab0, tab1)
  expect_equal(tab0, tab2)
  expect_equal(tab0, tab3)
})
