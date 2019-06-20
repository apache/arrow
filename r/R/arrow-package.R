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

#' @importFrom R6 R6Class
#' @importFrom purrr map map_int map2
#' @importFrom assertthat assert_that
#' @importFrom rlang list2 %||% is_false abort dots_n warn
#' @importFrom Rcpp sourceCpp
#' @useDynLib arrow, .registration = TRUE
#' @keywords internal
"_PACKAGE"

#' Is the C++ Arrow library available?
#'
#' You won't generally need to call this function, but it's here in case it
#' helps for development purposes.
#' @return `TRUE` or `FALSE` depending on whether the package was installed
#' with the Arrow C++ library. If `FALSE`, you'll need to install the C++
#' library and then reinstall the R package. See [install_arrow()] for help.
#' @export
arrow_available <- function() {
  .Call(`_arrow_available`)
}

#' Help installing the Arrow C++ library
#'
#' Binary package installations should come with a working Arrow C++ library,
#' but when installing from source, you'll need to obtain the C++ library
#' first. This function offers guidance on how to get the C++ library depending
#' on your operating system and package version.
#' @export
#' @importFrom utils packageVersion
install_arrow <- function() {
  if(arrow_available()) {
    # Respond that you already have it. See README for alternative installation instructions (like if you want a dev version). Please report an issue if you see arrow_available but can't do something you should be able to
  }

  os <- tolower(Sys.info()[["sysname"]])
  # c("windows", "darwin", "linux", "sunos") # win/mac/linux/solaris
  dev_version <- length(packageVersion("arrow")) > 3
  # From CRAN check:
  rep <- installed.packages(fields="Repository")["arrow", "Repository"]
  from_cran <- identical(rep, "CRAN")
  # Is it possible to tell if was a binary install from CRAN vs. source?

  # Cases (excluding where arrow_available):
  if(os == "sunos") {
    # Good luck with that. See C++ dev guide
  } else if(os == "linux") {
    if(dev_version) {
      # Point to compilation instructions on readme
    } else {
      # Suggest arrow.apache.org/install for PPAs, or compilation instructions
    }
  } else if(!dev_version && !from_cran) {
    # Windows or Mac with a released version but not from CRAN
    # Recommend installing released binary package from CRAN
  } else {
    # Windows or Mac, most likely a dev version
    # for each OS, recommend dev installation, refer to readme
    if(os == "windows") {

    } else {
      # macOS
    }
  }
}

option_use_threads <- function() {
  !is_false(getOption("arrow.use_threads"))
}
