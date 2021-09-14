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

skip_if_not_available("dataset")

library(dplyr)

left <- example_data
# Error: Invalid: Dictionary type support for join output field is not yet implemented, output field reference: FieldRef.Name(fct) on left side of the join
# (and select(-fct) doesn't solve this somehow)
left$fct <- NULL
left$some_grouping <- rep(c(1, 2), 5)

to_join <- tibble::tibble(
  # Error: Invalid: Output field name collision in join, name: some_grouping
  # (so call it something else)
  the_grouping = c(1, 2),
  capital_letters = c("A", "B"),
  another_column = TRUE
)

test_that("left_join", {
  expect_dplyr_equal(
    input %>%
      left_join(to_join, by = c(some_grouping = "the_grouping")) %>%
      collect(),
    left
  )
})