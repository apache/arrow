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

skip_if_not_available("gcs")

test_that("FileSystem$from_uri with gs://", {
  fs_and_path <- FileSystem$from_uri("gs://my/test/bucket/")
  expect_r6_class(fs_and_path$fs, "GcsFileSystem")
  expect_identical(fs_and_path$path, "my/test/bucket")
})

test_that("GcsFileSystem$create() options", {
  expect_r6_class(GcsFileSystem$create(), "GcsFileSystem")
  expect_r6_class(GcsFileSystem$create(anonymous = TRUE), "GcsFileSystem")

  # Verify default options
  expect_equal(GcsFileSystem$create()$options, list(
    anonymous = FALSE,
    scheme = "https",
    retry_limit_seconds = 15
  ))

  # Verify a more complete set of options round-trips
  options <- list(
    anonymous = TRUE,
    endpoint_override = "localhost:8888",
    scheme = "http",
    default_bucket_location = "here",
    retry_limit_seconds = 30,
    default_metadata = c(a = "list", of = "stuff")
  )

  fs <- do.call(GcsFileSystem$create, options)

  expect_r6_class(
    fs,
    "GcsFileSystem"
  )

  expect_equal(
    fs$options,
    options
  )

  # Expiration round-trips
  options <- list(
    expiration = as.POSIXct("2030-01-01", tz = "UTC"),
    access_token = "MY_TOKEN"
  )
  fs <- do.call(GcsFileSystem$create, options)

  expect_equal(fs$options$expiration, options$expiration)

  # Verify create fails if expiration isn't a POSIXct
  expect_error(
    GcsFileSystem$create(access_token = "", expiration = ""),
    "must be of class POSIXct, not"
  )
})

test_that("GcsFileSystem$create() input validation", {
  expect_error(
    GcsFileSystem$create(anonymous = TRUE, access_token = "something"),
    'Cannot specify "access_token" when anonymous = TRUE'
  )
  expect_error(
    GcsFileSystem$create(expiration = Sys.time()),
    "token auth requires both 'access_token' and 'expiration'"
  )
  expect_error(
    GcsFileSystem$create(json_credentials = "{}", expiration = Sys.time()),
    "Cannot provide access_token with json_credentials"
  )
  expect_error(
    GcsFileSystem$create(role_arn = "something"),
    'Invalid options for GcsFileSystem: "role_arn"'
  )
})

skip_on_cran()
skip_if_not(system('python -c "import testbench"') == 0, message = "googleapis-storage-testbench is not installed.")
library(dplyr)

testbench_port <- Sys.getenv("TESTBENCH_PORT", "9001")

pid_minio <- sys::exec_background("python", c("-m", "testbench", "--port", testbench_port),
  std_out = FALSE,
  std_err = FALSE # TODO: is there a good place to send output?
)
withr::defer(tools::pskill(pid_minio))
Sys.sleep(1) # Wait for startup

fs <- GcsFileSystem$create(
  endpoint_override = sprintf("localhost:%s", testbench_port),
  retry_limit_seconds = 1,
  scheme = "http",
  anonymous = TRUE # Will fail to resolve host name if anonymous isn't TRUE
)

now <- as.character(as.numeric(Sys.time()))
tryCatch(fs$CreateDir(now), error = function(cond) {
  if (grepl("Couldn't connect to server", cond, fixed = TRUE)) {
    abort(
      c(sprintf("Unable to connect to testbench on port %s.", testbench_port),
        i = "You can set a custom port with TESTBENCH_PORT environment variable."
      ),
      parent = cond
    )
  } else {
    stop(cond)
  }
})
# Clean up when we're all done
withr::defer(fs$DeleteDir(now))

gcs_path <- function(...) {
  paste(now, ..., sep = "/")
}
gcs_uri <- function(...) {
  template <- "gs://anonymous@%s?scheme=http&endpoint_override=localhost%s%s&retry_limit_seconds=1"
  sprintf(template, gcs_path(...), "%3A", testbench_port)
}

test_filesystem("gcs", fs, gcs_path, gcs_uri)

withr::deferred_run()
