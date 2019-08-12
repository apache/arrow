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

# Wrap testthat::test_that with a check for the C++ library
options(..skip.tests = !arrow::arrow_available())

test_that <- function(what, code) {
  testthat::test_that(what, {
    skip_if(getOption("..skip.tests", TRUE), "arrow C++ library not available")
    code
  })
}

# Wrapper to run tests that only touch R code even when the C++ library isn't
# available (so that at least some tests are run on those platforms)
r_only <- function(code) {
  old <- options(..skip.tests = FALSE)
  on.exit(options(old))
  code
}
