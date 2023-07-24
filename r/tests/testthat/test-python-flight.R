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

# Assumes:
# * We've already done arrow::install_pyarrow()
# * R -e 'arrow::load_flight_server("demo_flight_server")$DemoFlightServer(port = 8089)$serve()'

if (process_is_running("demo_flight_server")) {
  client <- flight_connect(port = 8089)
  flight_obj <- tempfile()

  test_that("flight_path_exists", {
    expect_false(flight_path_exists(client, flight_obj))
    expect_false(flight_obj %in% list_flights(client))
  })

  test_that("flight_put", {
    flight_put(client, example_data, path = flight_obj)
    expect_true(flight_path_exists(client, flight_obj))
    expect_true(flight_obj %in% list_flights(client))
    expect_error(
      flight_put(client, Array$create(c(1:3)), path = flight_obj),
      regexp = 'data must be a "data.frame", "Table", or "RecordBatch"'
    )
  })

  test_that("flight_put with max_chunksize", {
    flight_put(client, example_data, path = flight_obj, max_chunksize = 1)
    expect_true(flight_path_exists(client, flight_obj))
    expect_true(flight_obj %in% list_flights(client))
    expect_warning(
      flight_put(client, record_batch(example_data), path = flight_obj, max_chunksize = 123),
      regexp = "`max_chunksize` is not supported for flight_put with RecordBatch"
    )
    expect_error(
      flight_put(client, Array$create(c(1:3)), path = flight_obj),
      regexp = 'data must be a "data.frame", "Table", or "RecordBatch"'
    )
  })

  test_that("flight_get", {
    expect_equal_data_frame(flight_get(client, flight_obj), example_data)
  })

  test_that("flight_put with RecordBatch", {
    flight_obj2 <- tempfile()
    flight_put(client, RecordBatch$create(example_data), path = flight_obj2)
    expect_equal_data_frame(flight_get(client, flight_obj2), example_data)
  })

  test_that("flight_put with overwrite = FALSE", {
    expect_error(
      flight_put(client, example_with_times, path = flight_obj, overwrite = FALSE),
      "exists"
    )
    # Default is TRUE so this will overwrite
    flight_put(client, example_with_times, path = flight_obj)
    expect_equal_data_frame(flight_get(client, flight_obj), example_with_times)
  })

  test_that("flight_disconnect", {
    flight_disconnect(client)
    # Idempotent
    flight_disconnect(client)
  })
} else {
  # Kinda hacky, let's put a skipped test here, just so we note that the tests
  # didn't run
  test_that("Flight tests", {
    skip("Flight server is not running")
  })
}
