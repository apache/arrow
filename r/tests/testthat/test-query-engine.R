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

skip_if_not_available("acero")

test_that("ExecPlanReader does not start evaluating a query", {
  skip_if_not(CanRunWithCapturedR())

  rbr <- as_record_batch_reader(
    function(x) stop("This query will error if started"),
    schema = schema(a = int32())
  )

  reader <- as_record_batch_reader(as_adq(rbr))
  expect_identical(reader$PlanStatus(), "PLAN_NOT_STARTED")
  expect_error(reader$read_table(), "This query will error if started")
  expect_identical(reader$PlanStatus(), "PLAN_FINISHED")
})

test_that("ExecPlanReader evaluates nested exec plans lazily", {
  reader <- as_record_batch_reader(as_adq(arrow_table(a = 1:10)))
  expect_identical(reader$PlanStatus(), "PLAN_NOT_STARTED")

  head_reader <- head(reader, 4)
  expect_identical(reader$PlanStatus(), "PLAN_NOT_STARTED")

  expect_equal(
    head_reader$read_table(),
    arrow_table(a = 1:4)
  )

  expect_identical(reader$PlanStatus(), "PLAN_FINISHED")
})

test_that("ExecPlanReader evaluates head() lazily", {
  reader <- as_record_batch_reader(as_adq(arrow_table(a = 1:10)))
  expect_identical(reader$PlanStatus(), "PLAN_NOT_STARTED")

  head_reader <- head(reader, 4)
  expect_identical(reader$PlanStatus(), "PLAN_NOT_STARTED")

  expect_equal(
    head_reader$read_table(),
    arrow_table(a = 1:4)
  )

  expect_identical(reader$PlanStatus(), "PLAN_FINISHED")
})

test_that("ExecPlanReader evaluates head() lazily", {
  # Make a rather long RecordBatchReader
  reader <- RecordBatchReader$create(
    batches = rep(
      list(record_batch(line = letters)),
      100L
    )
  )

  # ...But only get 10 rows from it
  query <- head(as_adq(reader), 10)
  expect_identical(as_arrow_table(query)$num_rows, 10L)

  # Depending on exactly how quickly background threads respond to the
  # request to cancel, reader$read_table()$num_rows > 0 may or may not
  # evaluate to TRUE (i.e., the reader may or may not be completely drained).
})

test_that("head() of an ExecPlanReader is an ExecPlanReader", {
  reader <- as_record_batch_reader(as_adq(arrow_table(x = 1:10)))
  expect_r6_class(reader, "ExecPlanReader")
  reader_head <- head(reader, 6)
  expect_r6_class(reader_head, "ExecPlanReader")
  expect_equal(
    as_arrow_table(reader_head),
    arrow_table(x = 1:6)
  )
})

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
