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

#' Install or upgrade the Arrow library
#'
#' Use this function to install the latest release of `arrow`, to switch to or
#' from a nightly development version, or on Linux to try reinstalling with
#' all necessary C++ dependencies.
#'
#' @param nightly logical: Should we install a development version of the
#' package, or should we install from CRAN (the default).
#' @param binary On Linux, value to set for the environment variable
#' `LIBARROW_BINARY`, which governs how C++ binaries are used, if at all.
#' The default value, `TRUE`, tells the installation script to detect the
#' Linux distribution and version and find an appropriate C++ library. `FALSE`
#' would tell the script not to retrieve a binary and instead build Arrow C++
#' from source. Other valid values are strings corresponding to a Linux
#' distribution-version, to override the value that would be detected.
#' See `vignette("install", package = "arrow")` for further details.
#' @param use_system logical: Should we use `pkg-config` to look for Arrow
#' system packages? Default is `FALSE`. If `TRUE`, source installation may be
#' faster, but there is a risk of version mismatch.
#' @param minimal logical: If building from source, should we build without
#' optional dependencies (compression libraries, for example)? Default is
#' `FALSE`.
#' @param repos character vector of base URLs of the repositories to install
#' from (passed to `install.packages()`)
#' @param ... Additional arguments passed to `install.packages()`
#' @export
#' @importFrom utils install.packages
#' @seealso [arrow_available()] to see if the package was configured with
#' necessary C++ dependencies. `vignette("install", package = "arrow")` for
#' more ways to tune installation on Linux.
install_arrow <- function(nightly = FALSE,
                          binary = Sys.getenv("LIBARROW_BINARY", TRUE),
                          use_system = Sys.getenv("ARROW_USE_PKG_CONFIG", FALSE),
                          minimal = Sys.getenv("LIBARROW_MINIMAL", FALSE),
                          repos = getOption("repos"),
                          ...) {
  sysname <- tolower(Sys.info()[["sysname"]])
  conda <- isTRUE(grepl("conda", R.Version()$platform))

  if (sysname %in% c("windows", "darwin", "linux")) {
    if (conda && !nightly) {
      system("conda install -y -c conda-forge --strict-channel-priority r-arrow")
    } else {
      Sys.setenv(
        LIBARROW_DOWNLOAD = "true",
        LIBARROW_BINARY = binary,
        LIBARRWOW_MINIMAL = minimal,
        ARROW_USE_PKG_CONFIG = use_system
      )
      if (isTRUE(binary)) {
        # Unless otherwise directed, don't consider newer source packages when
        # options(pkgType) == "both" (default on win/mac)
        opts <- options(
          install.packages.check.source = "no",
          install.packages.compile.from.source = "never"
        )
        on.exit(options(opts))
      }
      install.packages("arrow", repos = arrow_repos(repos, nightly), ...)
    }
    if ("arrow" %in% loadedNamespaces()) {
      # If you've just sourced this file, "arrow" won't be (re)loaded
      reload_arrow()
    }
  } else {
    # Solaris
    message(SEE_README)
  }
}

arrow_repos <- function(repos = getOption("repos"), nightly = FALSE) {
  if (length(repos) == 0 || identical(repos, c(CRAN = "@CRAN@"))) {
    # Set the default/CDN
    repos <- "https://cloud.r-project.org/"
  }
  bintray <- getOption("arrow.dev.repo", "https://dl.bintray.com/ursalabs/arrow-r")
  # Remove it if it's there (so nightly=FALSE won't accidentally pull from it)
  repos <- setdiff(repos, bintray)
  if (nightly) {
    # Add it first
    repos <- c(bintray, repos)
  }
  repos
}

reload_arrow <- function() {
  if (requireNamespace("pkgload", quietly = TRUE)) {
    is_attached <- "package:arrow" %in% search()
    pkgload::unload("arrow")
    if (is_attached) {
      require("arrow", character.only = TRUE, quietly = TRUE)
    } else {
      requireNamespace("arrow", quietly = TRUE)
    }
  } else {
    message("Please restart R to use the 'arrow' package.")
  }
}

SEE_README <- paste(
  "Refer to the R package README",
  "<https://github.com/apache/arrow/blob/master/r/README.md>",
  "and `vignette('install', package = 'arrow')`",
  "for installation guidance."
)
