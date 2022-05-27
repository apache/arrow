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

library(testthat)
library(arrow)
library(tibble)

verbose_test_output <- identical(tolower(Sys.getenv("ARROW_R_DEV", "false")), "true") ||
  identical(tolower(Sys.getenv("ARROW_R_VERBOSE_TEST", "false")), "true")

if (verbose_test_output) {
  arrow_reporter <- MultiReporter$new(list(CheckReporter$new(), LocationReporter$new()))
} else {
  arrow_reporter <- check_reporter()
}
test_check("arrow", reporter = arrow_reporter)
