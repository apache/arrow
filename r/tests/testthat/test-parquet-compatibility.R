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

# TODO: skip mostly? Or use this for backwards compat?

pq_file <- test_path("parquets/data-arrow2.parquet")

test_that("reading a known Parquet file to dataframe", {
  df <- read_parquet(pq_file)
  expect_data_frame(df, data.frame(col1 = 1:10))
  expect_identical(dim(df), c(10L, 1L))

  tab <- read_parquet(pq_file, as_data_frame = FALSE)
  expect_s3_class(tab, "Table")
  expect_equal(
    # unserialize like .unserialize_arrow_r_metadata does (though we can't call
    # it directly because it's not exported)
    unserialize(charToRaw(tab$metadata$r)),
    list(
      attributes = list(class = "data.frame"),
      columns = list(col1 = NULL)
    )
  )

  # test the workaround
  tab$metadata$r <- NULL
  df <- as.data.frame(tab)

  expect_data_frame(df, data.frame(col1 = 1:10))
  expect_identical(dim(df), c(10L, 1L))
})
