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

context("install_arrow()")

i_have_arrow_msg <- "It appears you already have Arrow installed successfully: are you trying to install a different version of the library?

Refer to the R package README <https://github.com/apache/arrow/blob/master/r/README.md> for further details.

If you have other trouble, or if you think this message could be improved, please report an issue here: <https://issues.apache.org/jira/projects/ARROW/issues>"

r_only({
  test_that("install_arrow() prints a message", {
    expect_message(install_arrow())
  })

  test_that("Messages get the standard postscript appended", {
    expect_identical(
      install_arrow_msg(has_arrow = TRUE, "0.13.0"),
      i_have_arrow_msg
    )
  })

  test_that("Solaris and Linux dev version get pointed to C++ guide", {
    expect_match(
      install_arrow_msg(FALSE, "0.13.0", os="sunos"),
      "See the Arrow C++ developer guide",
      fixed = TRUE
    )
    expect_match(
      install_arrow_msg(FALSE, "0.13.0.9000", os="linux"),
      "See the Arrow C++ developer guide",
      fixed = TRUE
    )
  })

  test_that("Linux on release version gets pointed to PPA first, then C++", {
    expect_match(
      install_arrow_msg(FALSE, "0.13.0", os="linux"),
      "dependency. Or, see the Arrow C++ developer guide",
      fixed = TRUE
    )
  })

  test_that("Win/mac release version get pointed to CRAN", {
    expect_match(
      install_arrow_msg(FALSE, "0.13.0", os="darwin", from_cran=FALSE),
      "install.packages",
      fixed = TRUE
    )
    expect_match(
      install_arrow_msg(FALSE, "0.13.0", os="windows", from_cran=FALSE),
      "install.packages",
      fixed = TRUE
    )
  })

  test_that("Win/mac dev version get recommendations", {
    expect_match(
      install_arrow_msg(FALSE, "0.13.0.9000", os="darwin", from_cran=FALSE),
      "Homebrew"
    )
    expect_match(
      install_arrow_msg(FALSE, "0.13.0.9000", os="windows", from_cran=FALSE),
      "RWINLIB_LOCAL"
    )
  })
})
