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

test_that("tpch_dbgen()", {
  lineitem_rbr <- tpch_dbgen("lineitem", 1)
  lineitem_tab <- lineitem_rbr$read_table()
  expect_identical(ncol(lineitem_tab), 16L)

  # and check a handful of types
  expect_type_equal(lineitem_tab[["L_ORDERKEY"]], int32())
  expect_type_equal(lineitem_tab[["L_RECEIPTDATE"]], date32())

  region_rbr <- tpch_dbgen("region", 1)
  region_tab <- region_rbr$read_table()
  expect_identical(dim(region_tab), c(5L, 3L))

  # and check a handful of types
  expect_type_equal(region_tab[["R_REGIONKEY"]], int32())
  expect_type_equal(region_tab[["R_COMMENT"]], string())

  part_rbr <- tpch_dbgen("part", 1)
  part_tab <- part_rbr$read_table()
  expect_identical(dim(part_tab), c(200000L, 9L))

  # and check a handful of types
  expect_type_equal(part_tab[["P_PARTKEY"]], int32())
  expect_type_equal(part_tab[["P_NAME"]], string())
})

# these three are tested above, but test that we can get tables for all the rest
tpch_tables_up <- setdiff(tpch_tables, c("lineitem", "region", "part"))

for (table_name in tpch_tables_up) {
  test_that(paste0("Generating table: ", table_name), {
    rbr <- tpch_dbgen(table_name, 1)
    tab <- rbr$read_table()
    expect_r6_class(tab, "Table")
  })
}
