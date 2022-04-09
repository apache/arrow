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

r_only({
  test_that("arrow_repos", {
    cran <- "https://cloud.r-project.org/"
    ours <- "https://dl.example.com/ursalabs/fake_repo"
    other <- "https://cran.fiocruz.br/"

    opts <- list(
      repos = c(CRAN = "@CRAN@"), # Restore defaul
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

  test_that("bash-like substitution works", {
    vals <- c(ARROW_ZZZ_VERSION = "c.2", ARROW_AAA_VERSION = "v7")
    expect_equal(
      ..install_substitute_like_bash("x ${ARROW_ZZZ_VERSION} _", vals), "x c.2 _"
    )
    expect_equal(
      ..install_substitute_like_bash("https://example.com/${ARROW_AAA_VERSION:1}", vals),
      "https://example.com/7"
    )
    expect_equal(
      ..install_substitute_like_bash("x ${ARROW_ZZZ_VERSION//./_} .", vals), "x c_2 ."
    )
  })

  test_that("able to parse and substitute versions.txt url-filename array", {
    test_array_lines <- c(
      '"ARROW_ZLIB_URL zlib-${ARROW_ZLIB_BUILD_VERSION}.tar.gz https://zlib.net/fossils/zlib-${ARROW_ZLIB_BUILD_VERSION}.tar.gz"'
    )
    test_version_lines <- c(
      "ARROW_ZLIB_BUILD_VERSION=1.2.12",
      "ARROW_ZLIB_BUILD_SHA256_CHECKSUM=91844808532e5ce316b3c010929493c0244f3d37593afd6de04f71821d5136d9"
    )
    expected_paths_unsub <- list(
      filenames = "zlib-${ARROW_ZLIB_BUILD_VERSION}.tar.gz",
      urls = "https://zlib.net/fossils/zlib-${ARROW_ZLIB_BUILD_VERSION}.tar.gz"
    )
    expected_version_info <- c(ARROW_ZLIB_BUILD_VERSION = "1.2.12")
    expected_paths_sub <- list(
      filenames = "zlib-1.2.12.tar.gz",
      urls = "https://zlib.net/fossils/zlib-1.2.12.tar.gz"
    )

    expect_equal(
      ..install_parse_version_lines(test_version_lines),
      expected_version_info
    )
    expect_equal(
      ..install_parse_dependency_array(test_array_lines),
      expected_paths_unsub
    )
    expect_equal(
      ..install_substitute_all(expected_paths_unsub, expected_version_info),
      expected_paths_sub
    )
  })
})
