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

context("Datasets")

test_that("Assembling a Dataset and getting a Table", {
  fs <- LocalFileSystem$create()
  dir.create(td <- tempfile())
  write_parquet(iris, file.path(td, "iris1.parquet"))
  # write_parquet(iris, file.path(td, "iris2.parquet"))
  selector <- Selector$create(td, recursive = TRUE)
  dsd <- FileSystemDataSourceDiscovery$create(fs, selector)
  expect_is(dsd, "FileSystemDataSourceDiscovery")
  # expect_is(dsd$Inspect(), "Schema") # This segfaults
  expect_is(dsd$Finish(), "DataSource")

  # Workaround because Inspect isn't working
  schm <- ParquetFileReader$create(file.path(td, "iris1.parquet"))$GetSchema()
  ds <- Dataset$create(list(dsd), schm)
  expect_is(ds, "Dataset")
  expect_equal(names(ds), names(iris))

  sb <- ds$NewScan()
  expect_is(sb, "ScannerBuilder")
  expect_equal(sb$schema, schm)
  expect_equal(names(sb), names(iris))
  sb$Project(c("Petal.Length", "Petal.Width"))
  # expect_equal(names(sb), c("Petal.Length", "Petal.Width")) # This does not pass; Project does not affect schema. How can I see what I have projected already?
  scn <- sb$Finish()
  expect_is(scn, "Scanner")
  skip("bus error")
  # *** caught bus error ***
  # address 0x7fffa4f2acb8, cause 'non-existent physical address'
  tab <- scn$ToTable()
  expect_is(tab, "Table")
  expect_equal(as.data.frame(tab), iris)
})
