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

test_that("arrow_repos", {
  cran <- "https://cloud.r-project.org/"
  ours <- "https://dl.example.com/ursalabs/fake_repo"
  other <- "https://cran.fiocruz.br/"

  opts <- list(
    repos = c(CRAN = "@CRAN@"), # Restore default
    arrow.dev_repo = ours
  )
  withr::with_options(opts, {
    expect_identical(arrow_repos(), cran)
    expect_identical(arrow_repos(c(cran, ours)), cran)
    expect_identical(arrow_repos(c(ours, other)), other)
    expect_identical(arrow_repos(nightly = TRUE), c(ours, cran))
    expect_identical(arrow_repos(c(cran, ours), nightly = TRUE), c(ours, cran))
    expect_identical(arrow_repos(c(ours, other), nightly = TRUE), c(ours, other))
  })
})
