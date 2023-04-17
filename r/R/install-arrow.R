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
#' distribution-version, to override the value that would be detected. See the
#' \href{https://arrow.apache.org/docs/r/articles/install.html}{install guide}
#' for further details.
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
#' @seealso [arrow_info()] to see if the package was configured with
#' necessary C++ dependencies.
#' \href{https://arrow.apache.org/docs/r/articles/install.html}{install guide}
#' for more ways to tune installation on Linux.
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
  dev_repo <- getOption("arrow.dev_repo", "https://nightlies.apache.org/arrow/r")
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


#' Create a source bundle that includes all thirdparty dependencies
#'
#' @param dest_file File path for the new tar.gz package. Defaults to
#' `arrow_V.V.V_with_deps.tar.gz` in the current directory (`V.V.V` is the version)
#' @param source_file File path for the input tar.gz package. Defaults to
#' downloading the package from CRAN (or whatever you have set as the first in
#' `getOption("repos")`)
#' @return The full path to `dest_file`, invisibly
#'
#' This function is used for setting up an offline build. If it's possible to
#' download at build time, don't use this function. Instead, let `cmake`
#' download the required dependencies for you.
#' These downloaded dependencies are only used in the build if
#' `ARROW_DEPENDENCY_SOURCE` is unset, `BUNDLED`, or `AUTO`.
#' https://arrow.apache.org/docs/developers/cpp/building.html#offline-builds
#'
#' If you're using binary packages you shouldn't need to use this function. You
#' should download the appropriate binary from your package repository, transfer
#' that to the offline computer, and install that. Any OS can create the source
#' bundle, but it cannot be installed on Windows. (Instead, use a standard
#' Windows binary package.)
#'
#' Note if you're using RStudio Package Manager on Linux: If you still want to
#' make a source bundle with this function, make sure to set the first repo in
#' `options("repos")` to be a mirror that contains source packages (that is:
#' something other than the RSPM binary mirror URLs).
#'
#' ## Steps for an offline install with optional dependencies:
#'
#' ### Using a computer with internet access, pre-download the dependencies:
#' * Install the `arrow` package _or_ run
#'   `source("https://raw.githubusercontent.com/apache/arrow/main/r/R/install-arrow.R")`
#' * Run `create_package_with_all_dependencies("my_arrow_pkg.tar.gz")`
#' * Copy the newly created `my_arrow_pkg.tar.gz` to the computer without internet access
#'
#' ### On the computer without internet access, install the prepared package:
#' * Install the `arrow` package from the copied file
#'   * `install.packages("my_arrow_pkg.tar.gz", dependencies = c("Depends", "Imports", "LinkingTo"))`
#'   * This installation will build from source, so `cmake` must be available
#' * Run [arrow_info()] to check installed capabilities
#'
#'
#' @examples
#' \dontrun{
#' new_pkg <- create_package_with_all_dependencies()
#' # Note: this works when run in the same R session, but it's meant to be
#' # copied to a different computer.
#' install.packages(new_pkg, dependencies = c("Depends", "Imports", "LinkingTo"))
#' }
#' @export
create_package_with_all_dependencies <- function(dest_file = NULL, source_file = NULL) {
  if (Sys.which("bash") == "") {
    stop("
    This function requires bash to be installed and available in your PATH.
    If using RTools, it may be useful to run this code as:
    pkgbuild::with_build_tools(create_package_with_all_dependencies())
    ")
  }
  if (is.null(source_file)) {
    pkg_download_dir <- tempfile()
    dir.create(pkg_download_dir)
    on.exit(unlink(pkg_download_dir, recursive = TRUE), add = TRUE)
    message("Downloading Arrow source file")
    downloaded <- utils::download.packages("arrow", destdir = pkg_download_dir, type = "source")
    source_file <- downloaded[1, 2, drop = TRUE]
  }
  if (!file.exists(source_file) || !endsWith(source_file, "tar.gz")) {
    stop("Arrow package .tar.gz file not found")
  }
  if (is.null(dest_file)) {
    # e.g. convert /path/to/arrow_5.0.0.tar.gz to ./arrow_5.0.0_with_deps.tar.gz
    # (add 'with_deps' for clarity if the file was downloaded locally)
    dest_file <- paste0(gsub(".tar.gz$", "", basename(source_file)), "_with_deps.tar.gz")
  }
  untar_dir <- tempfile()
  on.exit(unlink(untar_dir, recursive = TRUE), add = TRUE)
  utils::untar(source_file, exdir = untar_dir)
  tools_dir <- file.path(untar_dir, "arrow/tools")
  download_dependencies_sh <- file.path(tools_dir, "download_dependencies_R.sh")
  # If you change this path, also need to edit nixlibs.R
  download_dir <- file.path(tools_dir, "thirdparty_dependencies")
  dir.create(download_dir)
  download_script <- tempfile(fileext = ".R")
  parse_versions_success <- system2(
    "bash", c(download_dependencies_sh, download_dir),
    stdout = download_script,
    stderr = FALSE
  ) == 0
  if (!parse_versions_success) {
    stop("Failed to parse versions.txt")
  }
  # `source` the download_script to use R to download all the dependency bundles
  source(download_script)

  # Need to change directory to untar_dir so tar() will use relative paths. That
  # means we'll need a full, non-relative path for dest_file. (extra_flags="-C"
  # doesn't work with R's internal tar)
  orig_wd <- getwd()
  on.exit(setwd(orig_wd), add = TRUE)
  # normalizePath() may return the input unchanged if dest_file doesn't exist,
  # so create it first.
  file.create(dest_file)
  dest_file <- normalizePath(dest_file, mustWork = TRUE)
  setwd(untar_dir)

  message("Repacking tar.gz file to ", dest_file)
  tar_successful <- utils::tar(dest_file, compression = "gz") == 0
  if (!tar_successful) {
    stop("Failed to create new tar.gz file")
  }
  invisible(dest_file)
}
