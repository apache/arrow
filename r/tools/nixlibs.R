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

args <- commandArgs(TRUE)
VERSION <- args[1]
dst_dir <- paste0("libarrow/arrow-", VERSION)

arrow_repo <- "https://arrow-r-nightly.s3.amazonaws.com/libarrow/"

if (getRversion() < 3.4 && is.null(getOption("download.file.method"))) {
  # default method doesn't work on R 3.3, nor does libcurl
  options(download.file.method = "wget")
}

options(.arrow.cleanup = character()) # To collect dirs to rm on exit
on.exit(unlink(getOption(".arrow.cleanup")))


env_is <- function(var, value) identical(tolower(Sys.getenv(var)), value)

try_download <- function(from_url, to_file) {
  status <- try(
    suppressWarnings(
      download.file(from_url, to_file, quiet = quietly)
    ),
    silent = quietly
  )
  # Return whether the download was successful
  !inherits(status, "try-error") && status == 0
}

build_ok <- !env_is("LIBARROW_BUILD", "false")
# But binary defaults to not OK
binary_ok <- !identical(tolower(Sys.getenv("LIBARROW_BINARY", "false")), "false")
# For local debugging, set ARROW_R_DEV=TRUE to make this script print more

quietly <- !env_is("ARROW_R_DEV", "true") # try_download uses quietly global
# * download_ok, build_ok: Use prebuilt binary, if found, otherwise try to build
# * !download_ok, build_ok: Build with local git checkout, if available, or
#   sources included in r/tools/cpp/. Optional dependencies are not included,
#   and will not be automatically downloaded.
#   cmake will still be downloaded if necessary
#   https://arrow.apache.org/docs/developers/cpp/building.html#offline-builds
# * download_ok, !build_ok: Only use prebuilt binary, if found
# * neither: Get the arrow-without-arrow package
# Download and build are OK unless you say not to (or can't access github)
download_ok <- !env_is("TEST_OFFLINE_BUILD", "true") && try_download("https://github.com", tempfile())


download_binary <- function(os = identify_os()) {
  libfile <- tempfile()
  if (!is.null(os)) {
    # See if we can map this os-version to one we have binaries for
    os <- find_available_binary(os)
    binary_url <- paste0(arrow_repo, "bin/", os, "/arrow-", VERSION, ".zip")
    if (try_download(binary_url, libfile)) {
      cat(sprintf("*** Successfully retrieved C++ binaries for %s\n", os))
      if (!identical(os, "centos-7")) {
        # centos-7 uses gcc 4.8 so the binary doesn't have ARROW_S3=ON but the others do
        # TODO: actually check for system requirements?
        cat("**** Binary package requires libcurl and openssl\n")
        cat("**** If installation fails, retry after installing those system requirements\n")
      }
    } else {
      cat(sprintf("*** No C++ binaries found for %s\n", os))
      libfile <- NULL
    }
  } else {
    libfile <- NULL
  }
  libfile
}

# Function to figure out which flavor of binary we should download, if at all.
# By default (unset or "FALSE"), it will not download a precompiled library,
# but you can override this by setting the env var LIBARROW_BINARY to:
# * `TRUE` (not case-sensitive), to try to discover your current OS, or
# * some other string, presumably a related "distro-version" that has binaries
#   built that work for your OS
identify_os <- function(os = Sys.getenv("LIBARROW_BINARY", Sys.getenv("TEST_OFFLINE_BUILD"))) {
  if (tolower(os) %in% c("", "false")) {
    # Env var says not to download a binary
    return(NULL)
  } else if (!identical(tolower(os), "true")) {
    # Env var provided an os-version to use--maybe you're on Ubuntu 18.10 but
    # we only build for 18.04 and that's fine--so use what the user set
    return(os)
  }

  linux <- distro()
  if (is.null(linux)) {
    cat("*** Unable to identify current OS/version\n")
    return(NULL)
  }
  paste(linux$id, linux$short_version, sep = "-")
}

#### start distro ####

distro <- function() {
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
  if (grepl("bullseye", out$codename)) {
    # debian unstable doesn't include a number but we can map from pretty name
    out$short_version <- "11"
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

read_system_release <- function() utils::head(readLines("/etc/system-release"), 1)

is_solaris <- function() {
  tolower(Sys.info()[["sysname"]]) %in% "sunos"
}

#### end distro ####

find_available_binary <- function(os) {
  # Download a csv that maps one to the other, columns "actual" and "use_this"
  u <- "https://raw.githubusercontent.com/ursa-labs/arrow-r-nightly/master/linux/distro-map.csv"
  lookup <- try(utils::read.csv(u, stringsAsFactors = FALSE), silent = quietly)
  if (!inherits(lookup, "try-error") && os %in% lookup$actual) {
    new <- lookup$use_this[lookup$actual == os]
    if (length(new) == 1 && !is.na(new)) { # Just some sanity checking
      cat(sprintf("*** Using %s binary for %s\n", new, os))
      os <- new
    }
  }
  os
}

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
      cat(paste0("*** Found local C++ source: '", cpp_dir, "'\n"))
      return(cpp_dir)
    }
  }
  NULL
}

build_libarrow <- function(src_dir, dst_dir) {
  # We'll need to compile R bindings with these libs, so delete any .o files
  system("rm src/*.o", ignore.stdout = TRUE, ignore.stderr = TRUE)
  # Set up make for parallel building
  makeflags <- Sys.getenv("MAKEFLAGS")
  if (makeflags == "") {
    # CRAN policy says not to use more than 2 cores during checks
    # If you have more and want to use more, set MAKEFLAGS
    ncores <- min(parallel::detectCores(), 2)
    makeflags <- sprintf("-j%s", ncores)
    Sys.setenv(MAKEFLAGS = makeflags)
  }
  if (!quietly) {
    cat("*** Building with MAKEFLAGS=", makeflags, "\n")
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
  options(.arrow.cleanup = c(getOption(".arrow.cleanup"), build_dir))

  R_CMD_config <- function(var) {
    if (getRversion() < 3.4) {
      # var names were called CXX1X instead of CXX11
      var <- sub("^CXX11", "CXX1X", var)
    }
    # tools::Rcmd introduced R 3.3
    tools::Rcmd(paste("config", var), stdout = TRUE)
  }
  env_var_list <- c(
    SOURCE_DIR = src_dir,
    BUILD_DIR = build_dir,
    DEST_DIR = dst_dir,
    CMAKE = cmake,
    # Make sure we build with the same compiler settings that R is using
    CC = R_CMD_config("CC"),
    CXX = paste(R_CMD_config("CXX11"), R_CMD_config("CXX11STD")),
    # CXXFLAGS = R_CMD_config("CXX11FLAGS"), # We don't want the same debug symbols
    LDFLAGS = R_CMD_config("LDFLAGS")
  )
  env_vars <- paste0(names(env_var_list), '="', env_var_list, '"', collapse = " ")
  env_vars <- with_s3_support(env_vars)
  env_vars <- with_mimalloc(env_vars)
  # turn_off_thirdparty_features() needs to happen after with_mimalloc() and
  # with_s3_support(), since those might turn features ON.
  thirdparty_deps_unavailable <- !download_ok &&
    !dir.exists(Sys.getenv("ARROW_THIRDPARTY_DEPENDENCY_DIR")) &&
    !env_is("ARROW_DEPENDENCY_SOURCE", "system")
  if (thirdparty_deps_unavailable || is_solaris()) {
    # Note that JSON support does work on Solaris, but will be turned off with
    # the rest of the thirdparty dependencies (when ARROW-13768 is resolved and
    # JSON can be turned off at all). All other dependencies don't compile
    # (e.g thrift, jemalloc, and xsimd) or do compile but `ar` fails to build
    # libarrow_bundled_dependencies (e.g. re2 and utf8proc).
    env_vars <- turn_off_thirdparty_features(env_vars)
  }
  # If $ARROW_THIRDPARTY_DEPENDENCY_DIR has files, add their *_SOURCE_URL env vars
  env_vars <- set_thirdparty_urls(env_vars)

  cat("**** arrow", ifelse(quietly, "", paste("with", env_vars)), "\n")
  status <- suppressWarnings(system(
    paste(env_vars, "inst/build_arrow_static.sh"),
    ignore.stdout = quietly, ignore.stderr = quietly
  ))
  if (status != 0) {
    # It failed :(
    cat(
      "**** Error building Arrow C++.",
      ifelse(env_is("ARROW_R_DEV", "true"), "", "Re-run with ARROW_R_DEV=true for debug information."),
      "\n"
    )
  }
  invisible(status)
}

ensure_cmake <- function() {
  cmake <- find_cmake(c(
    Sys.getenv("CMAKE"),
    Sys.which("cmake"),
    Sys.which("cmake3")
  ))

  if (is.null(cmake)) {
    # If not found, download it
    cat("**** cmake\n")
    CMAKE_VERSION <- Sys.getenv("CMAKE_VERSION", "3.19.2")
    if (tolower(Sys.info()[["sysname"]]) %in% "darwin") {
      postfix <- "-macos-universal.tar.gz"
    } else {
      postfix <- "-Linux-x86_64.tar.gz"
    }
    cmake_binary_url <- paste0(
      "https://github.com/Kitware/CMake/releases/download/v", CMAKE_VERSION,
      "/cmake-", CMAKE_VERSION, postfix
    )
    cmake_tar <- tempfile()
    cmake_dir <- tempfile()
    download_successful <- try_download(cmake_binary_url, cmake_tar)
    if (!download_successful) {
      stop(
        "cmake was not found locally and download failed.\n",
        "Make sure cmake is installed and available on your PATH\n",
        "(or download '", cmake_binary_url,
        "' and define the CMAKE environment variable)."
      )
    }
    untar(cmake_tar, exdir = cmake_dir)
    unlink(cmake_tar)
    options(.arrow.cleanup = c(getOption(".arrow.cleanup"), cmake_dir))
    cmake <- paste0(
      cmake_dir,
      "/cmake-", CMAKE_VERSION, sub(".tar.gz", "", postfix, fixed = TRUE),
      "/bin/cmake"
    )
  }
  cmake
}

find_cmake <- function(paths, version_required = 3.10) {
  # Given a list of possible cmake paths, return the first one that exists and is new enough
  for (path in paths) {
    if (nzchar(path) && cmake_version(path) >= version_required) {
      # Sys.which() returns a named vector, but that plays badly with c() later
      names(path) <- NULL
      return(path)
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

turn_off_thirdparty_features <- function(env_vars) {
  # Because these are done as environment variables (as opposed to build flags),
  # setting these to "OFF" overrides any previous setting. We don't need to
  # check the existing value.
  turn_off <- c(
    "ARROW_MIMALLOC=OFF",
    "ARROW_JEMALLOC=OFF",
    "ARROW_PARQUET=OFF", # depends on thrift
    "ARROW_DATASET=OFF", # depends on parquet
    "ARROW_S3=OFF",
    "ARROW_WITH_BROTLI=OFF",
    "ARROW_WITH_BZ2=OFF",
    "ARROW_WITH_LZ4=OFF",
    "ARROW_WITH_SNAPPY=OFF",
    "ARROW_WITH_ZLIB=OFF",
    "ARROW_WITH_ZSTD=OFF",
    "ARROW_WITH_RE2=OFF",
    "ARROW_WITH_UTF8PROC=OFF",
    # NOTE: this code sets the environment variable ARROW_JSON to "OFF", but
    # that setting is will *not* be honored by build_arrow_static.sh until
    # ARROW-13768 is resolved.
    "ARROW_JSON=OFF",
    # The syntax to turn off XSIMD is different.
    'EXTRA_CMAKE_FLAGS="-DARROW_SIMD_LEVEL=NONE"'
  )
  if (Sys.getenv("EXTRA_CMAKE_FLAGS") != "") {
    # Error rather than overwriting EXTRA_CMAKE_FLAGS
    # (Correctly inserting the flag into an existing quoted string is tricky)
    stop("Sorry, setting EXTRA_CMAKE_FLAGS is not supported at this time.")
  }
  paste(env_vars, paste(turn_off, collapse = " "))
}

set_thirdparty_urls <- function(env_vars) {
  deps_dir <- Sys.getenv("ARROW_THIRDPARTY_DEPENDENCY_DIR")
  files <- list.files(deps_dir, full.names = FALSE)
  if (length(files) == 0) {
    # This will be true if the variable is unset, if it's set but the directory
    # doesn't exist, or if it exists but is empty.
    return(env_vars)
  }
  dep_names <- c(
    "absl", # not used; seems to be a dependency of gRPC
    "aws-sdk-cpp",
    "aws-checksums",
    "aws-c-common",
    "aws-c-event-stream",
    "boost",
    "brotli",
    "bzip2",
    "cares", # not used; "a dependency of gRPC"
    "gbenchmark", # not used; "Google benchmark, for testing"
    "gflags", # not used; "for command line utilities (formerly Googleflags)"
    "glog", # not used; "for logging"
    "grpc", # not used; "for remote procedure calls"
    "gtest", # not used; "Googletest, for testing"
    "jemalloc",
    "lz4",
    "mimalloc",
    "orc", # not used; "for Apache ORC format support"
    "protobuf", # not used; "Google Protocol Buffers, for data serialization"
    "rapidjson",
    "re2",
    "snappy",
    "thrift",
    "utf8proc",
    "xsimd",
    "zlib",
    "zstd"
  )
  dep_regex <- paste0("^(", paste(dep_names, collapse = "|"), ").*")
  # If there were extra files in the folder (not matching our regex) drop them.
  files <- files[grepl(dep_regex, files, perl = TRUE)]
  # Convert e.g. "thrift-0.13.0.tar.gz" to ARROW_THRIFT_URL
  # Note that if there's no file called thrift*, we won't add
  # ARROW_THRIFT_URL to env_vars.
  url_env_varname <- sub(dep_regex, "ARROW_\\1_URL", files, perl = TRUE)
  url_env_varname <- toupper(gsub("-", "_", url_env_varname, fixed = TRUE))
  # Special case: ARROW_AWSSDK_URL for aws-sdk-cpp-<version>.tar.gz
  url_env_varname <- sub("ARROW_AWS_SDK_CPP_URL", "ARROW_AWSSDK_URL", url_env_varname, fixed = TRUE)
  if (anyDuplicated(url_env_varname)) {
    warning("Unexpected files in ", deps_dir,
      "\nDo you have multiple copies of a dependency?",
      .call = FALSE
    )
    return(env_vars)
  }
  full_filenames <- file.path(normalizePath(deps_dir), files)
  url_env_vars <- paste(url_env_varname, full_filenames, sep = "=", collapse = " ")
  paste(env_vars, url_env_vars)
}

with_mimalloc <- function(env_vars) {
  arrow_mimalloc <- env_is("ARROW_MIMALLOC", "on") || env_is("LIBARROW_MINIMAL", "false")
  if (arrow_mimalloc) {
    # User wants mimalloc. If they're using gcc, let's make sure the version is >= 4.9
    if (isTRUE(cmake_gcc_version(env_vars) < "4.9")) {
      cat("**** mimalloc support not available for gcc < 4.9; building with ARROW_MIMALLOC=OFF\n")
      arrow_mimalloc <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_mimalloc, "ARROW_MIMALLOC=ON", "ARROW_MIMALLOC=OFF"))
}

with_s3_support <- function(env_vars) {
  arrow_s3 <- env_is("ARROW_S3", "on") || env_is("LIBARROW_MINIMAL", "false")
  # but if ARROW_S3=OFF explicitly, we are definitely off, so override
  if (env_is("ARROW_S3", "off")) {
    arrow_s3 <- FALSE
  }
  if (arrow_s3) {
    # User wants S3 support. If they're using gcc, let's make sure the version is >= 4.9
    # and make sure that we have curl and openssl system libs
    if (isTRUE(cmake_gcc_version(env_vars) < "4.9")) {
      cat("**** S3 support not available for gcc < 4.9; building with ARROW_S3=OFF\n")
      arrow_s3 <- FALSE
    } else if (!cmake_find_package("CURL", NULL, env_vars)) {
      # curl on macos should be installed, so no need to alter this for macos
      cat("**** S3 support requires libcurl-devel (rpm) or libcurl4-openssl-dev (deb); building with ARROW_S3=OFF\n")
      arrow_s3 <- FALSE
    } else if (!cmake_find_package("OpenSSL", "1.0.2", env_vars)) {
      cat("**** S3 support requires version >= 1.0.2 of openssl-devel (rpm), libssl-dev (deb), or openssl (brew); building with ARROW_S3=OFF\n")
      arrow_s3 <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_s3, "ARROW_S3=ON", "ARROW_S3=OFF"))
}

cmake_gcc_version <- function(env_vars) {
  # This function returns NA if using a non-gcc compiler
  # Always enclose calls to it in isTRUE() or isFALSE()
  vals <- cmake_cxx_compiler_vars(env_vars)
  if (!identical(vals[["CMAKE_CXX_COMPILER_ID"]], "GNU")) {
    return(NA)
  }
  package_version(vals[["CMAKE_CXX_COMPILER_VERSION"]])
}

cmake_cxx_compiler_vars <- function(env_vars) {
  info <- system(paste("export", env_vars, "&& $CMAKE --system-information"), intern = TRUE)
  info <- grep("^[A-Z_]* .*$", info, value = TRUE)
  vals <- as.list(sub('^.*? "?(.*?)"?$', "\\1", info))
  names(vals) <- sub("^(.*?) .*$", "\\1", info)
  vals[grepl("^CMAKE_CXX_COMPILER_?", names(vals))]
}

cmake_find_package <- function(pkg, version = NULL, env_vars) {
  td <- tempfile()
  dir.create(td)
  options(.arrow.cleanup = c(getOption(".arrow.cleanup"), td))
  find_package <- paste0("find_package(", pkg, " ", version, " REQUIRED)")
  writeLines(find_package, file.path(td, "CMakeLists.txt"))
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

#####

if (!file.exists(paste0(dst_dir, "/include/arrow/api.h"))) {
  # If we're working in a local checkout and have already built the libs, we
  # don't need to do anything. Otherwise,
  # (1) Look for a prebuilt binary for this version
  bin_file <- src_dir <- NULL
  if (download_ok && binary_ok) {
    bin_file <- download_binary()
  }
  if (!is.null(bin_file)) {
    # Extract them
    dir.create(dst_dir, showWarnings = !quietly, recursive = TRUE)
    unzip(bin_file, exdir = dst_dir)
    unlink(bin_file)
  } else if (build_ok) {
    # (2) Find source and build it
    src_dir <- find_local_source()
    if (is.null(src_dir) && download_ok) {
      src_dir <- download_source()
    }
    if (!is.null(src_dir)) {
      cat("*** Building C++ libraries\n")
      build_libarrow(src_dir, dst_dir)
    } else {
      cat("*** Proceeding without C++ dependencies\n")
    }
  } else {
    cat("*** Proceeding without C++ dependencies\n")
  }
}
