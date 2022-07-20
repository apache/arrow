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

library(dplyr, warn.conflicts = FALSE)

test_that("do_exec_plan_substrait can evaluate a simple plan", {
  skip_if_not_available("substrait")

  df <- data.frame(i = 1:5, b = rep_len(c(TRUE, FALSE), 5))
  table <- arrow_table(df, schema = schema(i = int64(), b = bool()))

  tf <- tempfile()
  on.exit(unlink(tf))
  write_parquet(table, tf)

  substrait_json <- sprintf('{
    "relations": [
      {"rel": {
        "read": {
          "base_schema": {
            "struct": {
              "types": [ {"i64": {}}, {"bool": {}} ]
            },
            "names": ["i", "b"]
          },
          "local_files": {
            "items": [
              {
                "uri_file": "file://%s",
                "parquet": {}
              }
            ]
          }
        }
      }}
    ]
  }', tf)

  substrait_buffer <- substrait__internal__SubstraitFromJSON(substrait_json)
  expect_r6_class(substrait_buffer, "Buffer")
  substrait_raw <- as.raw(substrait_buffer)

  substrait_json_roundtrip <- substrait__internal__SubstraitToJSON(substrait_buffer)
  expect_match(substrait_json_roundtrip, tf, fixed = TRUE)

  result <- do_exec_plan_substrait(substrait_json)
  expect_identical(
    # TODO(ARROW-15585) The "select(i, b)" should not be needed
    tibble::as_tibble(result) %>% select(i, b),
    tibble::as_tibble(df)
  )
})
