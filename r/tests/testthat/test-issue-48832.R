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

test_that("zero-length POSIXct with empty tzone attribute handled safely", {
  x <- as.POSIXct(character(0))
  attr(x, "tzone") <- character(0)
  
  # Should not crash or error
  expect_error(type(x), NA)
  
  # Should default to no timezone (or empty string which effectively means local/no-tz behavior in arrow)
  # When sys.timezone is picked up it might vary, but we just check it doesn't crash.
  # If it picks up Sys.timezone(), checking exact equality might be flaky across environments if not mocked.
  # So we primarily check for no error.
  
  # Also check write_parquet survival
  tf <- tempfile() 
  on.exit(unlink(tf))
  expect_error(write_parquet(data.frame(x = x), tf), NA)
  expect_true(file.exists(tf))
})
