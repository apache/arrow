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

library(dplyr, warn.conflicts = FALSE)

skip_if_not_available("acero")

withr::local_options(list(arrow.summarise.sort = FALSE))

test_that("union_all", {
  compare_dplyr_binding(
    .input %>%
      union_all(example_data) %>%
      collect(),
    example_data
  )

  test_table <- arrow_table(x = 1:10)

  # Union with empty table produces same dataset
  expect_equal(
    test_table %>%
      union_all(test_table$Slice(0, 0)) %>%
      compute(),
    test_table
  )

  expect_error(
    test_table %>%
      union_all(arrow_table(y = 1:10)) %>%
      collect(),
    regex = "input schemas must all match"
  )
})

test_that("union", {
  compare_dplyr_binding(
    .input %>%
      dplyr::union(example_data) %>%
      collect(),
    example_data
  )

  test_table <- arrow_table(x = 1:10)

  # Union with empty table produces same dataset
  expect_equal(
    test_table %>%
      dplyr::union(test_table$Slice(0, 0)) %>%
      compute(),
    test_table
  )

  expect_error(
    test_table %>%
      dplyr::union(arrow_table(y = 1:10)) %>%
      collect(),
    regex = "input schemas must all match"
  )
})
