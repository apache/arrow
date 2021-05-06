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
#' Note that, unlike packages like `tensorflow`, `blogdown`, and others that
#' require external dependencies, you do not need to run `install_arrow()`
#' after a successful `arrow` installation.
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
#' faster, but there is a risk of version mismatch. This sets the
#' `ARROW_USE_PKG_CONFIG` environment variable.
#' @param minimal logical: If building from source, should we build without
#' optional dependencies (compression libraries, for example)? Default is
#' `FALSE`. This sets the `LIBARROW_MINIMAL` environment variable.
#' @param verbose logical: Print more debugging output when installing? Default
#' is `FALSE`. This sets the `ARROW_R_DEV` environment variable.
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
                          verbose = Sys.getenv("ARROW_R_DEV", FALSE),
                          repos = getOption("repos"),
                          ...) {
  sysname <- tolower(Sys.info()[["sysname"]])
  conda <- isTRUE(grepl("conda", R.Version()$platform))

  if (conda) {
    if (nightly) {
      system("conda install -y -c arrow-nightlies -c conda-forge --strict-channel-priority r-arrow")
    } else {
      system("conda install -y -c conda-forge --strict-channel-priority r-arrow")
    }
  } else {
    Sys.setenv(
      LIBARROW_DOWNLOAD = "true",
      LIBARROW_BINARY = binary,
      LIBARROW_MINIMAL = minimal,
      ARROW_R_DEV = verbose,
      ARROW_USE_PKG_CONFIG = use_system
    )
    # On the M1, we can't use the usual autobrew, which pulls Intel dependencies
    apple_m1 <- grepl("arm-apple|aarch64.*darwin", R.Version()$platform)
    # On Rosetta, we have to build without JEMALLOC, so we also can't autobrew
    rosetta <- identical(sysname, "darwin") && identical(system("sysctl -n sysctl.proc_translated", intern = TRUE), "1")
    if (rosetta) {
      Sys.setenv(ARROW_JEMALLOC = "OFF")
    }
    if (apple_m1 || rosetta) {
      Sys.setenv(FORCE_BUNDLED_BUILD = "true")
    }

    opts <- list()
    if (apple_m1 || rosetta) {
      # Skip binaries (esp. for rosetta)
      opts$pkgType <- "source"
    } else if (isTRUE(binary)) {
      # Unless otherwise directed, don't consider newer source packages when
      # options(pkgType) == "both" (default on win/mac)
      opts$install.packages.check.source <- "no"
      opts$install.packages.compile.from.source <- "never"
    }
    if (length(opts)) {
      old <- options(opts)
      on.exit(options(old))
    }
    install.packages("arrow", repos = arrow_repos(repos, nightly), ...)
  }
  if ("arrow" %in% loadedNamespaces()) {
    # If you've just sourced this file, "arrow" won't be (re)loaded
    reload_arrow()
  }
}

arrow_repos <- function(repos = getOption("repos"), nightly = FALSE) {
  if (length(repos) == 0 || identical(repos, c(CRAN = "@CRAN@"))) {
    # Set the default/CDN
    repos <- "https://cloud.r-project.org/"
  }
  dev_repo <- getOption("arrow.dev_repo", "https://arrow-r-nightly.s3.amazonaws.com")
  # Remove it if it's there (so nightly=FALSE won't accidentally pull from it)
  repos <- setdiff(repos, dev_repo)
  if (nightly) {
    # Add it first
    repos <- c(dev_repo, repos)
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
