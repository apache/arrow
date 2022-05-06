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

skip_if(on_old_windows())

library(dplyr, warn.conflicts = FALSE)

test_that("union_all", {
  compare_dplyr_binding(
    .input %>%
      union_all(example_data) %>%
      collect(),
    example_data
  )
})

test_that("union", {
  compare_dplyr_binding(
    .input %>%
      dplyr::union(example_data) %>%
      collect(),
    example_data
  )
})
