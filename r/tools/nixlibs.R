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

#### Functions #### check end of file for main logic
env_is <- function(var, value) identical(tolower(Sys.getenv(var)), value)

# Log messages in the style of the configure script
lg <- function(..., .indent = "***") {
  cat(.indent, " ", sprintf(...), "\n", sep = "")
}

cleanup <- function(path) {
  options(.arrow.cleanup = c(getOption(".arrow.cleanup"), path))
}

# Exit the script after logging with .status=1 instead of throwing an error
exit <- function(..., .status = 1) {
  lg(...)
  q(save = "no", status = .status)
}


# checks the nightly repo for the latest nightly version X.Y.Z.100<dev>
find_latest_nightly <- function(description_version,
                                list_uri = "https://nightlies.apache.org/arrow/r/src/contrib/PACKAGES",
                                hush = quietly) {
  if (!startsWith(arrow_repo, "https://nightlies.apache.org/arrow/r")) {
    lg("Detected non standard dev repo: %s, not checking latest nightly version.", arrow_repo)
    return(description_version)
  }

  res <- try(
    {
      # Binaries are only uploaded if all jobs pass so can just look at the source versions.
      urls <- readLines(list_uri)
      versions <- grep("Version:\\s*.*?", urls, value = TRUE)
      versions <- sort(package_version(sub("Version:\\s*", "\\1", versions)))
      major_versions <- versions$major

      description_version_major <- as.integer(description_version[1, 1])
      matching_major <- major_versions == description_version_major
      if (!any(matching_major)) {
        lg(
          "No nightly binaries were found for version %s: falling back to libarrow build from source",
          description_version
        )

        return(description_version)
      }

      versions <- versions[matching_major]
      max(versions)
    },
    silent = hush
  )

  if (inherits(res, "try-error")) {
    lg("Failed to find latest nightly for %s", description_version)
    latest <- description_version
  } else {
    lg("Latest available nightly for %s: %s", description_version, res)
    latest <- res
  }
  latest
}

try_download <- function(from_url, to_file, hush = quietly) {
  if (!download_ok) {
    # Don't even try
    return(FALSE)
  }
  # We download some fairly large files, so ensure the timeout is set appropriately.
  # This assumes a static library size of 100 MB (generous) and a download speed
  # of .3 MB/s (slow). This is to anticipate slower user connections or load on
  # artifactory servers.
  opts <- options(timeout = max(300, getOption("timeout")))
  on.exit(options(opts))

  status <- try(
    suppressWarnings(
      download.file(from_url, to_file, quiet = hush, mode = "wb")
    ),
    silent = hush
  )
  # Return whether the download was successful
  !inherits(status, "try-error") && status == 0
}

validate_checksum <- function(binary_url, libfile, hush = quietly) {
  # Explicitly setting the env var to "false" will skip checksum validation
  # e.g. in case the included checksums are stale.
  skip_checksum <- env_is("ARROW_R_ENFORCE_CHECKSUM", "false")
  enforce_checksum <- env_is("ARROW_R_ENFORCE_CHECKSUM", "true")
  checksum_path <- Sys.getenv("ARROW_R_CHECKSUM_PATH", "tools/checksums")
  # validate binary checksum for CRAN release only
  if (!skip_checksum && dir.exists(checksum_path) && is_release ||
    enforce_checksum) {
    # Munge the path to the correct sha file which we include during the
    # release process
    checksum_file <- sub(".+/bin/(.+\\.zip)", "\\1\\.sha512", binary_url)
    checksum_file <- file.path(checksum_path, checksum_file)

    # Try `shasum`, and if that doesn't work, fall back to `sha512sum` if not found
    # system2 doesn't generate an R error, so we can't use a tryCatch to
    # move from shasum to sha512sum.
    # The warnings from system2 if it fails pop up later in the log and thus are
    # more confusing than they are helpful (so we suppress them)
    checksum_ok <- suppressWarnings(system2(
      "shasum",
      args = c("--status", "-a", "512", "-c", checksum_file),
      stdout = ifelse(quietly, FALSE, ""),
      stderr = ifelse(quietly, FALSE, "")
    )) == 0

    if (!checksum_ok) {
      checksum_ok <- suppressWarnings(system2(
        "sha512sum",
        args = c("--status", "-c", checksum_file),
        stdout = ifelse(quietly, FALSE, ""),
        stderr = ifelse(quietly, FALSE, "")
      )) == 0
    }

    if (checksum_ok) {
      lg("Checksum validated successfully for libarrow")
    } else {
      lg("Checksum validation failed for libarrow")
      unlink(libfile)
    }
  } else {
    checksum_ok <- TRUE
  }

  # Return whether the checksum was successful
  checksum_ok
}

download_binary <- function(lib) {
  libfile <- paste0("arrow-", VERSION, ".zip")
  binary_url <- paste0(arrow_repo, "bin/", lib, "/arrow-", VERSION, ".zip")
  if (try_download(binary_url, libfile) && validate_checksum(binary_url, libfile)) {
    lg("Successfully retrieved libarrow (%s)", lib)
  } else {
    # If the download or checksum fail, we will set libfile to NULL this will
    # normally result in a source build after this.
    # TODO: should we condense these together and only call them when verbose?
    lg(
      "Unable to retrieve libarrow for version %s (%s)",
      VERSION, lib
    )
    if (!quietly) {
      lg(
        "Attempted to download the libarrow binary from: %s",
        binary_url
      )
    }
    libfile <- NULL
  }

  libfile
}

# Function to figure out which flavor of binary we should download, if at all.
# LIBARROW_BINARY controls the behavior. If unset, it will determine a course
# of action based on the current system. Other values you can set it to:
# * "FALSE" (not case-sensitive), to skip this option altogether
# * "TRUE" (not case-sensitive), to try to discover your current OS, or
# * Some other string: a "linux-openssl-${OPENSSL_VERSION}" that corresponds to
#   a binary that is available, to override what this function may discover by
#   default.
#   Possible values are:
#    * "linux-openssl-1.0" (OpenSSL 1.0)
#    * "linux-openssl-1.1" (OpenSSL 1.1)
#    * "linux-openssl-3.0" (OpenSSL 3.0)
#    * "macos-amd64-openssl-1.1" (OpenSSL 1.1)
#    * "macos-amd64-openssl-3.0" (OpenSSL 3.0)
#    * "macos-arm64-openssl-1.1" (OpenSSL 1.1)
#    * "macos-arm64-openssl-3.0" (OpenSSL 3.0)
#   These string values, along with `NULL`, are the potential return values of
#   this function.
identify_binary <- function(lib = Sys.getenv("LIBARROW_BINARY"), info = distro()) {
  if (on_windows) {
    return("windows")
  }

  lib <- tolower(lib)
  if (identical(lib, "")) {
    # Not specified. Check the allowlist.
    lib <- ifelse(check_allowlist(info$id), "true", "false")
  }

  if (identical(lib, "false")) {
    # Do not download a binary
    lib <- NULL
  } else if (!identical(lib, "true")) {
    # Env var provided an os-version to use, to override our logic.
    # We don't validate that this exists. If it doesn't, the download will fail
    # and the build will fall back to building from source
  } else {
    # See if we can find a suitable binary
    lib <- select_binary()
  }
  lib
}

check_allowlist <- function(os, allowed = "https://raw.githubusercontent.com/apache/arrow/main/r/tools/nixlibs-allowlist.txt") {
  allowlist <- tryCatch(
    # Try a remote allowlist so that we can add/remove without a release
    suppressWarnings(readLines(allowed)),
    # Fallback to default: allowed only on Ubuntu and CentOS/RHEL
    error = function(e) c("ubuntu", "centos", "redhat", "rhel")
  )
  # allowlist should contain valid regular expressions (plain strings ok too)
  any(grepl(paste(allowlist, collapse = "|"), os))
}

select_binary <- function(os = tolower(Sys.info()[["sysname"]]),
                          arch = tolower(Sys.info()[["machine"]]),
                          test_program = test_for_curl_and_openssl) {
  if (identical(os, "darwin") || (identical(os, "linux") && identical(arch, "x86_64"))) {
    # We only host x86 linux binaries and x86 & arm64 macos today
    binary <- tryCatch(
      # Somehow the test program system2 call errors on the sanitizer builds
      # so globally handle the possibility that this could fail
      {
        errs <- compile_test_program(test_program)
        openssl_version <- determine_binary_from_stderr(errs)
        arch <- ifelse(identical(os, "darwin"), paste0("-", arch, "-"), "-")
        if (is.null(openssl_version)) {
          NULL
        } else {
          paste0(os, arch, openssl_version)
        }
      },
      error = function(e) {
        lg("Unable to find libcurl and openssl")
        NULL
      }
    )
  } else {
    # No binary available for arch
    lg("Building on %s %s", os, arch)
    binary <- NULL
  }
  binary
}

# This tests that curl and OpenSSL are present (bc we can include their headers)
# and it checks for other versions/features and raises errors that we grep for
test_for_curl_and_openssl <- "
#ifndef __APPLE__
#include <ciso646>
#ifdef _LIBCPP_VERSION
#error Using libc++
#endif
#endif

#include <curl/curl.h>
#include <openssl/opensslv.h>
#if OPENSSL_VERSION_NUMBER < 0x10002000L
#error OpenSSL version too old
#endif
#if OPENSSL_VERSION_NUMBER < 0x10100000L
#error Using OpenSSL version 1.0
#endif
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
#error Using OpenSSL version 3
#endif
"

compile_test_program <- function(code) {
  openssl_dir <- ""
  if (on_macos) {
    openssl_root_dir <- get_macos_openssl_dir()
    openssl_dir <- paste0("-I", openssl_root_dir, "/include")
  }
  runner <- paste(
    R_CMD_config("CXX17"),
    openssl_dir,
    R_CMD_config("CPPFLAGS"),
    R_CMD_config("CXX17FLAGS"),
    R_CMD_config("CXX17STD"),
    "-E",
    "-xc++"
  )
  suppressWarnings(system2("echo", sprintf('"%s" | %s -', code, runner), stdout = FALSE, stderr = TRUE))
}

get_macos_openssl_dir <- function() {
  openssl_root_dir <- Sys.getenv("OPENSSL_ROOT_DIR", NA)
  header <- "openssl/opensslv.h"
  if (is.na(openssl_root_dir) || !file.exists(file.path(openssl_root_dir, "include", header))) {
    # try to guess default openssl include dir based on CRAN's build script
    # https://github.com/R-macos/recipes/blob/master/build.sh#L35
    if (identical(Sys.info()["machine"], "arm64") && file.exists(file.path("/opt/R/arm64/include", header))) {
      openssl_root_dir <- "/opt/R/arm64"
    } else if (identical(Sys.info()["machine"], "x86_64") && file.exists(file.path("/opt/R/x86_64/include", header))) {
      openssl_root_dir <- "/opt/R/x86_64"
    } else {
      openssl_root_dir <- "/usr/local"
    }
  }
  openssl_root_dir
}

# (built with newer devtoolset but older glibc (2.17) for broader compatibility,# like manylinux2014)
determine_binary_from_stderr <- function(errs) {
  if (is.null(attr(errs, "status"))) {
    # There was no error in compiling: so we found libcurl and OpenSSL >= 1.1,
    # openssl is < 3.0
    lg("Found libcurl and OpenSSL >= 1.1")
    return("openssl-1.1")
    # Else, check for dealbreakers:
  } else if (!on_macos && any(grepl("Using libc++", errs, fixed = TRUE))) {
    # Our linux binaries are all built with GNU stdlib so they fail with libc++
    lg("Linux binaries incompatible with libc++")
    return(NULL)
  } else if (header_not_found("curl/curl", errs)) {
    lg("libcurl not found")
    return(NULL)
  } else if (header_not_found("openssl/opensslv", errs)) {
    lg("OpenSSL not found")
    return(NULL)
  } else if (any(grepl("OpenSSL version too old", errs))) {
    lg("OpenSSL found but version >= 1.0.2 is required for some features")
    return(NULL)
    # Else, determine which other binary will work
  } else if (any(grepl("Using OpenSSL version 1.0", errs))) {
    if (on_macos) {
      lg("OpenSSL 1.0 is not supported on macOS")
      return(NULL)
    }
    lg("Found libcurl and OpenSSL < 1.1")
    return("openssl-1.0")
  } else if (any(grepl("Using OpenSSL version 3", errs))) {
    lg("Found libcurl and OpenSSL >= 3.0.0")
    return("openssl-3.0")
  }
  NULL
}

header_not_found <- function(header, errs) {
  regex <- sprintf("[Ee]rror.*%s\\.h", header)
  any(grepl(regex, errs))
}

#### start distro ####

distro <- function() {
  # This is not part of distro but needed to enable prebuilt binaries on macos
  if (on_macos) {
    return(list(id = "darwin", arch = tolower(Sys.info()[["machine"]])))
  }

  # The code in this script is a (potentially stale) copy of the distro package
  if (requireNamespace("distro", quietly = TRUE)) {
    # Use the version from the package, which may be updated from this
    return(distro::distro())
  }

  out <- lsb_release()
  if (is.null(out)) {
    out <- os_release()
    if (is.null(out)) {
      out <- system_release()
    }
  }
  if (is.null(out)) {
    return(NULL)
  }

  out$id <- tolower(out$id)
  # debian unstable & testing lsb_release `version` don't include numbers but we can map from pretty name
  if (is.null(out$version) || out$version %in% c("testing", "unstable")) {
    if (grepl("bullseye", out$codename)) {
      out$short_version <- "11"
    } else if (grepl("bookworm", out$codename)) {
      out$short_version <- "12"
    }
  } else if (out$id == "ubuntu") {
    # Keep major.minor version
    out$short_version <- sub('^"?([0-9]+\\.[0-9]+).*"?.*$', "\\1", out$version)
  } else {
    # Only major version number
    out$short_version <- sub('^"?([0-9]+).*"?.*$', "\\1", out$version)
  }
  out
}

lsb_release <- function() {
  if (have_lsb_release()) {
    list(
      id = call_lsb("-is"),
      version = call_lsb("-rs"),
      codename = call_lsb("-cs")
    )
  } else {
    NULL
  }
}

have_lsb_release <- function() nzchar(Sys.which("lsb_release"))
call_lsb <- function(args) system(paste("lsb_release", args), intern = TRUE)

os_release <- function() {
  rel_data <- read_os_release()
  if (!is.null(rel_data)) {
    vals <- as.list(sub('^.*="?(.*?)"?$', "\\1", rel_data))
    names(vals) <- sub("^(.*)=.*$", "\\1", rel_data)

    out <- list(
      id = vals[["ID"]],
      version = vals[["VERSION_ID"]]
    )
    if ("VERSION_CODENAME" %in% names(vals)) {
      out$codename <- vals[["VERSION_CODENAME"]]
    } else {
      # This probably isn't right, maybe could extract codename from pretty name?
      out$codename <- vals[["PRETTY_NAME"]]
    }
    out
  } else {
    NULL
  }
}

read_os_release <- function() {
  if (file.exists("/etc/os-release")) {
    readLines("/etc/os-release")
  }
}

system_release <- function() {
  rel_data <- read_system_release()
  if (!is.null(rel_data)) {
    # Something like "CentOS Linux release 7.7.1908 (Core)"
    list(
      id = sub("^([a-zA-Z]+) .* ([0-9.]+).*$", "\\1", rel_data),
      version = sub("^([a-zA-Z]+) .* ([0-9.]+).*$", "\\2", rel_data),
      codename = NA
    )
  } else {
    NULL
  }
}

read_system_release <- function() {
  if (file.exists("/etc/system-release")) {
    readLines("/etc/system-release")[1]
  }
}

#### end distro ####

find_local_source <- function() {
  # We'll take the first of these that exists
  # The first case probably occurs if we're in the arrow git repo
  # The second probably occurs if we're installing the arrow R package
  cpp_dir_options <- c(
    file.path(Sys.getenv("ARROW_SOURCE_HOME", ".."), "cpp"),
    "tools/cpp"
  )
  for (cpp_dir in cpp_dir_options) {
    if (file.exists(file.path(cpp_dir, "src/arrow/api.h"))) {
      lg("Found local C++ source: '%s'", cpp_dir)
      return(cpp_dir)
    }
  }
  NULL
}

env_vars_as_string <- function(env_var_list) {
  # Do some basic checks on env_var_list:
  # Check that env_var_list has names, that those names are valid POSIX
  # environment variables, and that none of the values contain `'`.
  stopifnot(
    length(env_var_list) == length(names(env_var_list)),
    all(grepl("^[^0-9]", names(env_var_list))),
    all(grepl("^[a-zA-Z0-9_]+$", names(env_var_list))),
    !any(grepl("'", env_var_list, fixed = TRUE))
  )
  env_var_string <- paste0(names(env_var_list), "='", env_var_list, "'", collapse = " ")
  if (nchar(env_var_string) > 30000) {
    # This could happen if the full paths in *_SOURCE_URL were *very* long.
    # A more formal check would look at getconf ARG_MAX, but this shouldn't matter
    lg("Warning: Environment variables are very long. This could cause issues on some shells.")
  }
  env_var_string
}

R_CMD_config <- function(var) {
  tools::Rcmd(paste("config", var), stdout = TRUE)
}

build_libarrow <- function(src_dir, dst_dir) {
  # We'll need to compile R bindings with these libs, so delete any .o files
  system("rm src/*.o", ignore.stdout = TRUE, ignore.stderr = TRUE)
  # Set up make for parallel building
  # CRAN policy says not to use more than 2 cores during checks
  # If you have more and want to use more, set MAKEFLAGS or NOT_CRAN
  ncores <- parallel::detectCores()
  if (!not_cran) {
    ncores <- min(ncores, 2)
  }
  makeflags <- Sys.getenv("MAKEFLAGS")
  if (makeflags == "") {
    makeflags <- sprintf("-j%s", ncores)
    Sys.setenv(MAKEFLAGS = makeflags)
  }
  if (!quietly) {
    lg("Building with MAKEFLAGS=%s", makeflags)
  }
  # Check for libarrow build dependencies:
  # * cmake
  cmake <- ensure_cmake()

  # Optionally build somewhere not in tmp so we can dissect the build if it fails
  debug_dir <- Sys.getenv("LIBARROW_DEBUG_DIR")
  if (nzchar(debug_dir)) {
    build_dir <- debug_dir
  } else {
    # But normally we'll just build in a tmp dir
    build_dir <- tempfile()
  }
  cleanup(build_dir)

  env_var_list <- c(
    SOURCE_DIR = src_dir,
    BUILD_DIR = build_dir,
    DEST_DIR = dst_dir,
    CMAKE = cmake,
    # EXTRA_CMAKE_FLAGS will often be "", but it's convenient later to have it defined
    EXTRA_CMAKE_FLAGS = Sys.getenv("EXTRA_CMAKE_FLAGS"),
    # Make sure we build with the same compiler settings that R is using
    # Exception: if you've added ccache to CC and CXX following
    # http://dirk.eddelbuettel.com/blog/2017/11/27/, some libarrow
    # third party dependencies will error on compilation. But don't
    # worry, `ARROW_USE_CCACHE=ON` by default, so if ccache
    # is found, it will be used by the libarrow build, and this does
    # not affect how R compiles the arrow bindings.
    CC = sub("^.*ccache", "", R_CMD_config("CC")),
    CXX = paste(sub("^.*ccache", "", R_CMD_config("CXX17")), R_CMD_config("CXX17STD")),
    # CXXFLAGS = R_CMD_config("CXX17FLAGS"), # We don't want the same debug symbols
    LDFLAGS = R_CMD_config("LDFLAGS"),
    N_JOBS = ncores
  )

  dep_source <- Sys.getenv("ARROW_DEPENDENCY_SOURCE")
  if (dep_source %in% c("", "AUTO") && !nzchar(Sys.which("pkg-config"))) {
    lg("pkg-config not installed, setting ARROW_DEPENDENCY_SOURCE=BUNDLED", .indent = "****")
    env_var_list <- c(env_var_list, ARROW_DEPENDENCY_SOURCE = "BUNDLED")
  }

  # On macOS, if not otherwise set, let's override Boost_SOURCE to be bundled
  # Necessary due to #39590 for CRAN
  if (on_macos) {
    # Using lowercase (e.g. Boost_SOURCE) to match the cmake args we use already.
    deps_to_bundle <- c("Boost", "lz4")
    for (dep_to_bundle in deps_to_bundle) {
      env_var <- paste0(dep_to_bundle, "_SOURCE")
      if (Sys.getenv(env_var) == "") {
        env_var_list <- c(env_var_list, setNames("BUNDLED", env_var))
      }
    }
  }

  env_var_list <- with_cloud_support(env_var_list)

  # turn_off_all_optional_features() needs to happen after
  # with_cloud_support(), since it might turn features ON.
  thirdparty_deps_unavailable <- !download_ok &&
    !dir.exists(thirdparty_dependency_dir) &&
    !env_is("ARROW_DEPENDENCY_SOURCE", "system")
  do_minimal_build <- env_is("LIBARROW_MINIMAL", "true")

  if (do_minimal_build) {
    env_var_list <- turn_off_all_optional_features(env_var_list)
  } else if (thirdparty_deps_unavailable) {
    cat(paste0(
      "*** Building C++ library from source, but downloading thirdparty dependencies\n",
      "    is not possible, so this build will turn off all thirdparty features.\n",
      "    See installation guide for details:\n",
      "    https://arrow.apache.org/docs/r/articles/install.html\n"
    ))
    env_var_list <- turn_off_all_optional_features(env_var_list)
  } else if (dir.exists(thirdparty_dependency_dir)) {
    # Add the *_SOURCE_URL env vars
    env_var_list <- set_thirdparty_urls(env_var_list)
  }
  env_vars <- env_vars_as_string(env_var_list)

  lg("arrow %s", ifelse(quietly, "", paste("with", env_vars)), .indent = "****")

  build_log_path <- tempfile(fileext = ".log")
  status <- suppressWarnings(system2(
    "bash",
    "inst/build_arrow_static.sh",
    env = env_vars,
    stdout = ifelse(quietly, build_log_path, ""),
    stderr = ifelse(quietly, build_log_path, "")
  ))

  if (status != 0) {
    # It failed :(
    lg("Error building Arrow C++.", .indent = "****")
    if (quietly) {
      cat(
        "**** Printing contents of build log because the build failed",
        "while ARROW_R_DEV was set to FALSE\n"
      )
      cat(readLines(build_log_path), sep = "\n")
      cat("**** Complete build log may still be present at", build_log_path, "\n")
    }
  }
  invisible(status)
}

ensure_cmake <- function(cmake_minimum_required = "3.16") {
  cmake <- find_cmake(version_required = cmake_minimum_required)

  if (is.null(cmake)) {
    # If not found, download it
    CMAKE_VERSION <- Sys.getenv("CMAKE_VERSION", "3.26.4")
    if (on_macos) {
      postfix <- "-macos-universal.tar.gz"
    } else if (tolower(Sys.info()[["machine"]]) %in% c("arm64", "aarch64")) {
      postfix <- "-linux-aarch64.tar.gz"
    } else if (tolower(Sys.info()[["machine"]]) == "x86_64") {
      postfix <- "-linux-x86_64.tar.gz"
    } else {
      exit(paste0(
        "*** cmake was not found locally.\n",
        "    Please make sure cmake >= ", cmake_minimum_required,
        " is installed and available on your PATH."
      ))
    }
    cmake_binary_url <- paste0(
      "https://github.com/Kitware/CMake/releases/download/v", CMAKE_VERSION,
      "/cmake-", CMAKE_VERSION, postfix
    )
    cmake_tar <- tempfile()
    cmake_dir <- tempfile()
    download_successful <- try_download(cmake_binary_url, cmake_tar)
    if (!download_successful) {
      exit(paste0(
        "*** cmake was not found locally and download failed.\n",
        "    Make sure cmake >= ", cmake_minimum_required,
        " is installed and available on your PATH,\n",
        "    or download ", cmake_binary_url, "\n",
        "    and define the CMAKE environment variable.\n"
      ))
    }
    untar(cmake_tar, exdir = cmake_dir)
    unlink(cmake_tar)
    cleanup(cmake_dir)
    # the bin dir is slightly different on macos
    if (on_macos) {
      bin_dir <- "CMake.app/Contents/bin"
    } else {
      bin_dir <- "bin"
    }
    cmake <- paste0(
      cmake_dir,
      "/cmake-", CMAKE_VERSION, sub(".tar.gz", "", postfix, fixed = TRUE),
      "/",
      bin_dir,
      "/cmake"
    )
    lg("cmake %s", CMAKE_VERSION, .indent = "****")
  }
  cmake
}

find_cmake <- function(paths = c(
                         Sys.getenv("CMAKE"),
                         Sys.which("cmake"),
                         # CRAN has it here, not on PATH
                         if (on_macos) "/Applications/CMake.app/Contents/bin/cmake",
                         Sys.which("cmake3")
                       ),
                       version_required = "3.16") {
  # Given a list of possible cmake paths, return the first one that exists and is new enough
  # version_required should be a string or packageVersion; numeric version
  # can be misleading (e.g. 3.10 is actually 3.1)
  for (path in paths) {
    if (nzchar(path) && file.exists(path)) {
      # Sys.which() returns a named vector, but that plays badly with c() later
      names(path) <- NULL
      found_version <- cmake_version(path)
      if (found_version >= version_required) {
        # Show which one we found
        lg("cmake %s: %s", found_version, path, .indent = "****")
        # Stop searching here
        return(path)
      } else {
        # Keep trying
        lg("Not using cmake found at %s", path, .indent = "****")
        if (found_version > 0) {
          lg("Version >= %s required; found %s", version_required, found_version, .indent = "*****")
        } else {
          # If cmake_version() couldn't determine version, it returns 0
          lg("Could not determine version; >= %s required", version_required, .indent = "*****")
        }
      }
    }
  }
  # If none found, return NULL
  NULL
}

cmake_version <- function(cmd = "cmake") {
  tryCatch(
    {
      raw_version <- system(paste(cmd, "--version"), intern = TRUE, ignore.stderr = TRUE)
      pat <- ".* ([0-9\\.]+).*?"
      which_line <- grep(pat, raw_version)
      package_version(sub(pat, "\\1", raw_version[which_line]))
    },
    error = function(e) {
      return(0)
    }
  )
}

turn_off_all_optional_features <- function(env_var_list) {
  # Because these are done as environment variables (as opposed to build flags),
  # setting these to "OFF" overrides any previous setting. We don't need to
  # check the existing value.
  # Some features turn on other features (e.g. substrait -> protobuf),
  # So the list of things to turn off is long. See:
  # https://github.com/apache/arrow/blob/main/cpp/cmake_modules/ThirdpartyToolchain.cmake#L275
  turn_off <- c(
    "ARROW_MIMALLOC" = "OFF",
    "ARROW_JEMALLOC" = "OFF",
    "ARROW_JSON" = "OFF",
    "ARROW_PARQUET" = "OFF", # depends on thrift
    "ARROW_DATASET" = "OFF", # depends on parquet
    "ARROW_S3" = "OFF",
    "ARROW_GCS" = "OFF",
    "ARROW_WITH_GOOGLE_CLOUD_CPP" = "OFF",
    "ARROW_WITH_NLOHMANN_JSON" = "OFF",
    "ARROW_SUBSTRAIT" = "OFF",
    "ARROW_WITH_PROTOBUF" = "OFF",
    "ARROW_WITH_BROTLI" = "OFF",
    "ARROW_WITH_BZ2" = "OFF",
    "ARROW_WITH_LZ4" = "OFF",
    "ARROW_WITH_SNAPPY" = "OFF",
    "ARROW_WITH_ZLIB" = "OFF",
    "ARROW_WITH_ZSTD" = "OFF",
    "ARROW_WITH_RE2" = "OFF",
    "ARROW_WITH_UTF8PROC" = "OFF",
    # The syntax to turn off XSIMD is different.
    # Pull existing value of EXTRA_CMAKE_FLAGS first (must be defined)
    "EXTRA_CMAKE_FLAGS" = paste(
      env_var_list[["EXTRA_CMAKE_FLAGS"]],
      "-DARROW_SIMD_LEVEL=NONE -DARROW_RUNTIME_SIMD_LEVEL=NONE"
    )
  )
  # Create a new env_var_list, with the values of turn_off set.
  # replace() also adds new values if they didn't exist before
  replace(env_var_list, names(turn_off), turn_off)
}

get_component_names <- function() {
  if (!isTRUE(Sys.which("bash") != "")) {
    stop("nixlibs.R requires bash to be installed and available in your PATH")
  }
  deps_bash <- "tools/download_dependencies_R.sh"
  csv_tempfile <- tempfile(fileext = ".csv")
  deps_bash_success <- system2("bash", deps_bash, stdout = csv_tempfile) == 0
  if (!deps_bash_success) {
    stop("Failed to run download_dependencies_R.sh")
  }
  deps_df <- read.csv(csv_tempfile,
    stringsAsFactors = FALSE, row.names = NULL, quote = "'"
  )
  stopifnot(
    names(deps_df) == c("env_varname", "filename"),
    nrow(deps_df) > 0
  )
  deps_df
}

set_thirdparty_urls <- function(env_var_list) {
  # This function does *not* check if existing *_SOURCE_URL variables are set.
  # The directory tools/thirdparty_dependencies is created by
  # create_package_with_all_dependencies() and saved in the tar file.
  deps_df <- get_component_names()
  dep_dir <- normalizePath(thirdparty_dependency_dir, mustWork = TRUE)
  deps_df$full_filenames <- file.path(dep_dir, deps_df$filename)
  files_exist <- file.exists(deps_df$full_filenames)
  if (!any(files_exist)) {
    stop("Dependency tar files did not exist in '", dep_dir, "'")
  }
  # Only set env var for files that are in thirdparty_dependency_dir
  # (allows for a user to download a limited set of tar files, if they wanted)
  deps_df <- deps_df[files_exist, ]
  env_var_list <- replace(env_var_list, deps_df$env_varname, deps_df$full_filenames)
  if (!quietly) {
    env_var_list <- replace(env_var_list, "ARROW_VERBOSE_THIRDPARTY_BUILD", "ON")
  }
  env_var_list
}

is_feature_requested <- function(env_varname, default = env_is("LIBARROW_MINIMAL", "false")) {
  env_value <- tolower(Sys.getenv(env_varname))
  if (identical(env_value, "off")) {
    # If e.g. ARROW_MIMALLOC=OFF explicitly, override default
    requested <- FALSE
  } else if (identical(env_value, "on")) {
    requested <- TRUE
  } else {
    requested <- default
  }
  requested
}

with_cloud_support <- function(env_var_list) {
  arrow_s3 <- is_feature_requested("ARROW_S3")
  arrow_gcs <- is_feature_requested("ARROW_GCS")

  if (arrow_s3 || arrow_gcs) {
    # User wants S3 or GCS support.
    # Make sure that we have curl and openssl system libs
    feats <- c(
      if (arrow_s3) "S3",
      if (arrow_gcs) "GCS"
    )
    start_msg <- paste(feats, collapse = "/")
    off_flags <- paste("ARROW_", feats, "=OFF", sep = "", collapse = " and ")
    print_warning <- function(msg) {
      # Utility to assemble warning message in the console
      cat("**** ", start_msg, " support ", msg, "; building with ", off_flags, "\n")
    }

    if (!cmake_find_package("CURL", NULL, env_var_list)) {
      # curl on macos should be installed, so no need to alter this for macos
      # TODO: check for apt/yum/etc. and message the right thing?
      print_warning("requires libcurl-devel (rpm) or libcurl4-openssl-dev (deb)")
      arrow_s3 <- FALSE
      arrow_gcs <- FALSE
    } else if (!cmake_find_package("OpenSSL", "1.0.2", env_var_list)) {
      print_warning("requires version >= 1.0.2 of openssl-devel (rpm), libssl-dev (deb), or openssl (brew)")
      arrow_s3 <- FALSE
      arrow_gcs <- FALSE
    }
  }

  # Update the build flags
  env_var_list <- replace(env_var_list, "ARROW_S3", ifelse(arrow_s3, "ON", "OFF"))
  replace(env_var_list, "ARROW_GCS", ifelse(arrow_gcs, "ON", "OFF"))
}

cmake_find_package <- function(pkg, version = NULL, env_var_list) {
  td <- tempfile()
  dir.create(td)
  cleanup(td)
  find_package <- paste0("find_package(", pkg, " ", version, " REQUIRED)")
  writeLines(find_package, file.path(td, "CMakeLists.txt"))
  env_vars <- env_vars_as_string(env_var_list)
  cmake_cmd <- paste0(
    "export ", env_vars,
    " && cd ", td,
    " && $CMAKE ",
    " -DCMAKE_EXPORT_NO_PACKAGE_REGISTRY=ON",
    " -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    " ."
  )
  system(cmake_cmd, ignore.stdout = TRUE, ignore.stderr = TRUE) == 0
}

############### Main logic #############
args <- commandArgs(TRUE)
VERSION <- args[1]

# TESTING is set in test-nixlibs.R; it won't be set when called from configure
test_mode <- exists("TESTING")

# Prevent error with binary selection during testing.
if (test_mode && is.na(VERSION)) {
  VERSION <- "8.0.0.9000"
}

VERSION <- package_version(VERSION)
dev_version <- VERSION[1, 4]
# Small dev versions are added for R-only changes during CRAN submission
is_release <- is.na(dev_version) || dev_version < "100"

on_macos <- tolower(Sys.info()[["sysname"]]) == "darwin"
on_windows <- tolower(Sys.info()[["sysname"]]) == "windows"

# For local debugging, set ARROW_R_DEV=TRUE to make this script print more
quietly <- !env_is("ARROW_R_DEV", "true")

# To collect dirs to rm on exit, use cleanup() to add dirs
# we reset it to avoid errors on reruns in the same session.
options(.arrow.cleanup = character())
on.exit(unlink(getOption(".arrow.cleanup"), recursive = TRUE), add = TRUE)

not_cran <- env_is("NOT_CRAN", "true")
if (not_cran) {
  # Set more eager defaults
  if (env_is("LIBARROW_BINARY", "")) {
    Sys.setenv(LIBARROW_BINARY = "true")
  }
  if (env_is("LIBARROW_MINIMAL", "")) {
    Sys.setenv(LIBARROW_MINIMAL = "false")
  }
}

# The default will build from source as a fallback if a binary is not found or shouldn't be used
# Set LIBARROW_BUILD=FALSE to ensure that we use a previously built libarrow
# and don't fall back to a full source build
build_ok <- !env_is("LIBARROW_BUILD", "false")

# Check if we're authorized to download
download_ok <- !test_mode && !env_is("ARROW_OFFLINE_BUILD", "true")
if (!download_ok) {
  lg("Dependency downloading disabled. Unset ARROW_OFFLINE_BUILD to enable", .indent = "***")
}
# If not forbidden from downloading, check if we are offline and turn off downloading.
# The default libarrow source build will download its source dependencies and fail
# if they can't be retrieved.
# But, don't do this if the user has requested a binary or a non-minimal build:
# we should error rather than silently succeeding with a minimal build.
if (download_ok && Sys.getenv("LIBARROW_BINARY") %in% c("false", "") && !env_is("LIBARROW_MINIMAL", "false")) {
  download_ok <- try_download("https://apache.jfrog.io/artifactory/arrow/r/", tempfile())
  if (!download_ok) {
    lg("Network connection not available", .indent = "***")
  }
}

download_libarrow_ok <- download_ok && !env_is("LIBARROW_DOWNLOAD", "false")

# Set binary repos
if (is_release) {
  VERSION <- VERSION[1, 1:3]
  arrow_repo <- paste0(getOption("arrow.repo", sprintf("https://apache.jfrog.io/artifactory/arrow/r/%s", VERSION)), "/libarrow/")
} else {
  arrow_repo <- paste0(getOption("arrow.dev_repo", "https://nightlies.apache.org/arrow/r"), "/libarrow/")
}

# If we're on a dev version, look for the most recent libarrow binary version
if (download_libarrow_ok && !is_release && !test_mode) {
  VERSION <- find_latest_nightly(VERSION)
}

# This "tools/thirdparty_dependencies" path, within the tar file, might exist if
# create_package_with_all_dependencies() was run, or if someone has created it
# manually before running make build.
# If you change this path, you also need to edit
# `create_package_with_all_dependencies()` in install-arrow.R
thirdparty_dependency_dir <- Sys.getenv("ARROW_THIRDPARTY_DEPENDENCY_DIR", "tools/thirdparty_dependencies")

arrow_versioned <- paste0("arrow-", VERSION)
# configure.win uses a different libarrow dir and the zip is already nested
if (on_windows) {
  lib_dir <- "windows"
  dst_dir <- lib_dir
} else {
  lib_dir <- "libarrow"
  dst_dir <- file.path(lib_dir, arrow_versioned)
}

api_h <- file.path(lib_dir, arrow_versioned, "include/arrow/api.h")

if (!test_mode && !file.exists(api_h)) {
  # If we're working in a local checkout and have already built the libs, we
  # don't need to do anything. Otherwise,
  # (1) Look for a prebuilt binary for this version
  bin_file <- src_dir <- NULL

  # Keep backwards compatibility with winlibs.R
  bin_zip <- Sys.getenv("ARROW_DOWNLOADED_BINARIES", Sys.getenv("RWINLIB_LOCAL", NA))

  if (!is.na(bin_zip)) {
    lg("Using pre-downloaded zip for libarrow binaries: %s", bin_zip)
    if (file.exists(bin_zip)) {
      bin_file <- tempfile()
      file.copy(bin_zip, bin_file)
    } else {
      lg("File not found: %s ($ARROW_DOWNLOADED_BINARIES)", bin_zip)
      bin_file <- NULL
    }
  } else if (download_libarrow_ok) {
    binary_flavor <- identify_binary()
    if (!is.null(binary_flavor)) {
      # The env vars say we can, and we've determined a lib that should work
      bin_file <- download_binary(binary_flavor)
    }
  }

  if (!is.null(bin_file)) {
    # Extract them
    dir.create(dst_dir, showWarnings = !quietly, recursive = TRUE)
    unzip(bin_file, exdir = dst_dir)
    unlink(bin_file)
  } else if (build_ok && !on_windows) {
    # (2) Find source and build it
    src_dir <- find_local_source()
    if (!is.null(src_dir)) {
      cat(paste0(
        "*** Building libarrow from source\n",
        "    For build options and troubleshooting, see the install guide:\n",
        "    https://arrow.apache.org/docs/r/articles/install.html\n"
      ))
      build_libarrow(src_dir, dst_dir)
    } else {
      exit("Proceeding without libarrow (no local source)")
    }
  } else {
    exit("Proceeding without libarrow (build not authorized)")
  }
}
