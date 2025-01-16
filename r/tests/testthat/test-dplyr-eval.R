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

test_that("various paths in arrow_eval", {
  expect_arrow_eval_error(
    assert_is(1, "character"),
    class = "validation_error"
  )
  expect_arrow_eval_error(
    NoTaVaRiAbLe,
    class = "validation_error"
  )
  expect_arrow_eval_error(
    match.arg("z", c("a", "b")),
    class = "validation_error"
  )
  expect_arrow_eval_error(
    stop("something something NotImplementedError"),
    class = "arrow_not_supported"
  )
})

test_that("try_arrow_dplyr/abandon_ship adds the right message about collect()", {
  tester <- function(.data, arg) {
    try_arrow_dplyr({
      if (arg == 0) {
        # This one just stops and doesn't recommend calling collect()
        validation_error("arg is 0")
      } else if (arg == 1) {
        # This one recommends calling collect()
        arrow_not_supported("arg == 1")
      } else {
        # Because this one has an alternative suggested, it adds "Or, collect()"
        arrow_not_supported(
          "arg greater than 0",
          body = c(">" = "Try setting arg to -1")
        )
      }
    })
  }

  skip_if_not_available("dataset")
  ds <- InMemoryDataset$create(arrow_table(x = 1))
  for (i in 0:2) {
    expect_snapshot(tester(ds, i), error = TRUE)
  }
})
