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
  schm <- dsd$Inspect()
  expect_is(schm, "Schema")
  expect_equal(
    schm,
    ParquetFileReader$create(file.path(td, "iris1.parquet"))$GetSchema()
  )
  datasource <- dsd$Finish()
  expect_is(datasource, "DataSource")

  ds <- Dataset$create(list(datasource), schm)
  expect_is(ds, "Dataset")
  expect_equal(names(ds), names(iris))

  sb <- ds$NewScan()
  expect_is(sb, "ScannerBuilder")
  expect_equal(sb$schema, schm)
  expect_equal(names(sb), names(iris))
  sb$Project(c("Petal.Length", "Petal.Width"))
  # This does not pass; Project does not affect schema. How can I see what I have projected already?
  # expect_equal(names(sb), c("Petal.Length", "Petal.Width"))
  sb$Filter(FieldExpression$create("Petal.Width") == 1.8)
  scn <- sb$Finish()
  expect_is(scn, "Scanner")
  tab <- scn$ToTable()
  expect_is(tab, "Table")
  expect_equivalent(
    as.data.frame(tab),
    iris[iris$Petal.Width == 1.8, c("Petal.Length", "Petal.Width")]
  )
  # Now in dplyr
  library(dplyr)
  expect_equivalent(
    ds %>%
      select(Petal.Length, Petal.Width) %>%
      filter(Petal.Width == 1.8) %>%
      collect(),
    iris[iris$Petal.Width == 1.8, c("Petal.Length", "Petal.Width")]
  )
})
