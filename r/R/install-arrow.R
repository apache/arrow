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

# Substitute like the bash shell
#
# @param one_string A length-1 character vector
# @param possible_values A dictionary-ish set of variables that could provide
#   values to substitute in.
# @return `one_string`, with values substituted like bash would.
#
# Only supports a small subset of bash substitution patterns. May have multiple
# bash variables in `one_string`
# Used as a helper to parse versions.txt
..install_substitute_like_bash <- function(one_string, possible_values) {
  stopifnot(
    !is.null(names(possible_values)),
    !any(names(possible_values) == ""),
    anyDuplicated(names(possible_values)) == 0,
    length(one_string) == 1,
    !anyNA(possible_values)
  )
  # Find the name of the version we want, something like
  # ARROW_RAPIDJSON_BUILD_VERSION
  # The `(//./_|:1)` is a special case to handle some bash fanciness.
  version_regex <- "\\$\\{(ARROW_[A-Z0-9_]+?_VERSION)(//./_|:1)?\\}"
  # Extract the matched groups. If the pattern occurs multiple times, we need
  # all (non-overlapping) matches. stringr::str_extract_all() or
  # base::gregexec() would be useful here, but gregexec was only introduced in
  # R 4.1.
  matched_substrings <- regmatches(
    one_string,
    gregexpr(version_regex, one_string, perl = TRUE)
  )[[1]] # Subset [[1]] because one_string has length 1
  # `matched_substrings` is a character vector with length equal to the number
  # of non-overlapping matches of `version_regex` in `one_string`. `match_list`
  # is a list (same length as `matched_substrings`), where each list element is
  # a length-3 character vector. The first element of the vector is the value
  # from `matched_substrings` (e.g. "${ARROW_ZZZ_VERSION//./_}"). The following
  #  two values are the captured groups specified in `version_regex` e.g.
  # "ARROW_ZZZ_VERSION" and "//./_".
  match_list <- regmatches(
    matched_substrings,
    regexec(version_regex, matched_substrings, perl = TRUE)
  )
  # Small helper to take slices of match_list
  extract_chr_by_idx <- function(lst, idx) {
    vapply(lst, function(x) x[idx], FUN.VALUE = character(1L))
  }
  string_to_sub <- extract_chr_by_idx(match_list, 1L)
  version_varnames <- extract_chr_by_idx(match_list, 2L)
  bash_special_cases <- extract_chr_by_idx(match_list, 3L)
  version_values <- possible_values[version_varnames]
  version_values <- ifelse(
    bash_special_cases == "", version_values, ifelse(
      bash_special_cases == ":1", substring(version_values, 2), ifelse(
        bash_special_cases == "//./_", gsub(".", "_", version_values, fixed = TRUE),
        NA_character_ # otherwise
      )
    )
  )
  num_to_sub <- length(string_to_sub)
  stopifnot(
    all(version_varnames %in% names(possible_values)),
    !anyNA(version_values),
    num_to_sub >= 1,
    num_to_sub < 10 # Something has gone wrong if we're doing 10+
  )
  out <- one_string
  for (idx in seq_len(num_to_sub)) {
    # not gsub in case there are duplicates
    out <- sub(string_to_sub[idx], version_values[idx], out, fixed = TRUE)
  }
  out
}

# Substitute all values in the filenames and URLs of versions.txt
#
# @param deps_unsubstituted A list with two elements, `filenames` and `urls`
# @param possible_values A dictionary-ish set of variables that could provide
#   values to substitute in.
# @return A list with two elements, `filenames` and `urls`, with values
#   substituted into the strings like bash would.
#
# Used as a helper to parse versions.txt
..install_substitute_all <- function(deps_unsubstituted, possible_values) {
  file_substituted <- vapply(
    deps_unsubstituted$filenames,
    ..install_substitute_like_bash,
    FUN.VALUE = character(1),
    possible_values = possible_values
  )
  url_substituted <- vapply(
    deps_unsubstituted$urls,
    ..install_substitute_like_bash,
    FUN.VALUE = character(1),
    possible_values = possible_values
  )
  list(
    filenames = unname(file_substituted),
    urls = unname(url_substituted)
  )
}

# Parse the version lines portion of versions.txt
#
# @param version_lines A character vector of lines read from versions.txt
# @return The parsed and bash-substiuted version values
#
# Used as a helper to parse versions.txt
..install_parse_version_lines <- function(version_lines) {
  version_lines <- trimws(version_lines)
  version_regex <- "^(ARROW_[A-Z0-9_]+_)(VERSION|SHA256_CHECKSUM)=([^=]+)$"
  if (!all(grepl(version_regex, version_lines, perl = TRUE))) {
    stop("Failed to parse version lines")
  }
  match_list <- regmatches(
    version_lines,
    regexec(version_regex, version_lines, perl = TRUE)
  )
  # Find the lines where the second regex match group is that are "VERSION" (as
  # opposed to "SHA256_CHECKSUM")
  version_idx <- vapply(
    match_list,
    function(m) m[[3]] == "VERSION",
    FUN.VALUE = logical(1)
  )
  version_matches <- match_list[version_idx]
  # Fancy indexing here is just to pull the first and second regex match out,
  # e.g. "ARROW_RAPIDJSON_BUILD_" and "VERSION"
  version_varnames <- vapply(
    version_matches,
    function(m) paste0(m[[2]], m[[3]]),
    FUN.VALUE = character(1)
  )
  version_values <- vapply(version_matches,
    function(m) m[[4]],
    FUN.VALUE = character(1)
  )
  names(version_values) <- version_varnames
  return(version_values)
}

# Parse the URL + filename array portion of versions.txt
#
# @param array_lines Characer vector of lines from the versions.txt file with
#   the filename and URL array
# @return A list with two character vectors, with names `filenames` and `urls`
#
# The output of this function has split out the filename and URL components,
# but has not yet substituted in the version numbers. The output is next passed
# to `..install_substitute_all()`
#
# Used as a helper to parse versions.txt
..install_parse_dependency_array <- function(array_lines) {
  stopifnot(
    length(array_lines) >= 1,
    is.character(array_lines),
    !anyNA(array_lines)
  )
  array_lines <- trimws(array_lines)

  # Parse the array_lines with a regex. Each line of the array is a different
  # component, e.g.
  # `"ARROW_RAPIDJSON_URL rapidjson-${ARROW_RAPIDJSON_BUILD_VERSION}.tar.gz https://github.com/miloyip/rapidjson/archive/${ARROW_RAPIDJSON_BUILD_VERSION}.tar.gz"`
  # The first element is the variable name of the URL. This matters for cmake,
  # but not here. The second is the filename that will be saved (no directory).
  # The third is the URL, including some version string that's defined earlier
  # in the file.
  # Regex in words:
  # Start with `"ARROW_`, then any capital ASCII letter, number, or underscore.
  # After a space, find anything except a space, colon, or forward slash. (No
  # space is essential, and would be essential to parsing the array in bash.
  # The colon and slash are just basic guards that this is a filename.) Next, a
  # space. Then a URL, starting with https://, and including anything except a
  # space. (This is the URL before substituting in the version sting, so normal
  # URL parsing rules don't apply.)
  dep_array_regex <- '^"(ARROW_[A-Z0-9_]+_URL) ([^ :/"]+) (https://[^ "]+)"$'
  if (!all(grepl(dep_array_regex, array_lines, perl = TRUE))) {
    stop("Cannot parse thirdparty dependency array in expected format.")
  }
  list(
    filenames = gsub(dep_array_regex, "\\2", array_lines, perl = TRUE),
    urls      = gsub(dep_array_regex, "\\3", array_lines, perl = TRUE)
  )
}

# Parse the versions.txt file
#
# @param versions_file Filename pointing to versions.txt
# @return The parsed and ready-to-use values, as a named list of vectors
#
#
# The versions.txt file is included as part of the R tar file, and is here:
# https://github.com/apache/arrow/blob/master/cpp/thirdparty/versions.txt
#
# Used as a helper to parse versions.txt
..install_parse_lines <- function(versions_file) {
  orig_lines <- readLines(versions_file)

  lines <- gsub("#.*", "", orig_lines, perl = TRUE)
  lines <- lines[lines != ""]

  dep_array_start_idx <- grep("^DEPENDENCIES=\\($", lines, perl = TRUE)
  dep_array_lines <- lines[
    seq.int(from = dep_array_start_idx + 1, to = length(lines) - 1, by = 1)
  ]
  version_lines <- lines[seq.int(1, dep_array_start_idx - 1, by = 1)]
  version_info <- ..install_parse_version_lines(version_lines)

  failed_to_parse <- anyNA(orig_lines) ||
    length(orig_lines) > 1000 ||
    length(lines) == 0 ||
    length(dep_array_start_idx) != 1 ||
    dep_array_start_idx <= 1 ||
    dep_array_start_idx >= length(lines) - 3 ||
    lines[length(lines)] != ")" ||
    length(dep_array_lines) == 0 ||
    anyNA(version_info)

  if (failed_to_parse) {
    stop(
      "Failed to parse 3rd party dependency file. It's possible the function ",
      "is not reading the correct file or the file formatting was not what ",
      "was expected.",
      call. = FALSE
    )
  }
  deps_unsubstituted <- ..install_parse_dependency_array(dep_array_lines)
  ..install_substitute_all(deps_unsubstituted, possible_values = version_info)
}

# Download the thirdparty dependencies specified in versions.txt
#
# @param dep_info Dependency info (named list of character vectors) as created
#   by `..install_parse_lines()`
# @param download_dir Directory name for download destination.
#   The directory must already exist.
# @return NULL
..install_download_dependencies <- function(dep_info, download_dir) {
  stopifnot(
    length(dep_info$urls) == length(dep_info$filenames),
    length(dep_info$urls) > 0,
    length(download_dir) == 1
  )
  download_dir <- normalizePath(download_dir, winslash = "/", mustWork = TRUE)
  full_filenames <- file.path(download_dir, dep_info$filenames, fsep = "/")
  # Using libcurl here is well supported in R, but is a different download
  # engine than the wget in download_dependencies.sh
  # libcurl is required for supplying multiple URLs, and is available in all
  # CRAN builds, but isn't guaranteed.
  download_result_code <- download.file(dep_info$urls,
    full_filenames,
    method = "libcurl",
    quiet = TRUE
  )
  if (!isTRUE(download_result_code == 0)) {
    stop("Failed to download thirdparty dependencies")
  }
  invisible(NULL)
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
#'   `source("https://raw.githubusercontent.com/apache/arrow/master/r/R/install-arrow.R")`
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
  versions_file <- file.path(tools_dir, "cpp/thirdparty/versions.txt")
  # If you change this path, also need to edit nixlibs.R
  download_dir <- file.path(tools_dir, "thirdparty_dependencies")
  dir.create(download_dir)
  dependency_info <- ..install_parse_lines(versions_file)

  message("Downloading files to ", download_dir)
  ..install_download_dependencies(dependency_info, download_dir)
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
