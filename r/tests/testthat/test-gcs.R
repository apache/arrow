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
  # TODO: expose options as a list so we can confirm they are set?
  expect_r6_class(GcsFileSystem$create(), "GcsFileSystem")
  expect_r6_class(GcsFileSystem$create(anonymous = TRUE), "GcsFileSystem")
  expect_r6_class(
    GcsFileSystem$create(
      anonymous = TRUE,
      scheme = "http",
      endpoint_override = "localhost:8888",
      default_bucket_location = "here",
      retry_limit_seconds = 30,
      default_metadata = c(a = "list", of = "stuff")
    ),
    "GcsFileSystem"
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
