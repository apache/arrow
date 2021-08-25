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
    Sys.getenv("ARROW_SOURCE_HOME", ".."),
    "tools/cpp"
  )
  valid_cpp_dir <- file.exists(file.path(cpp_dir_options, "src/arrow/api.h"))
  if (!any(valid_cpp_dir)) {
    return(NULL)
  }
  cpp_dir <- cpp_dir_options[valid_cpp_dir][1]
  cat(paste0("*** Found local C++ source:\n    '", cpp_dir, "'\n"))
  cpp_dir
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
  # Add env variables like ARROW_S3=ON. Order doesn't matter. Depends on `download_ok`
  env_vars <- with_s3_support(env_vars)
  env_vars <- with_mimalloc(env_vars)
  env_vars <- with_jemalloc(env_vars)
  env_vars <- with_parquet(env_vars)
  env_vars <- with_dataset(env_vars)
  env_vars <- with_brotli(env_vars)
  env_vars <- with_bz2(env_vars)
  env_vars <- with_lz4(env_vars)
  env_vars <- with_re2(env_vars)
  env_vars <- with_snappy(env_vars)
  env_vars <- with_utf8proc(env_vars)
  env_vars <- with_zlib(env_vars)
  env_vars <- with_zstd(env_vars)
  env_vars <- with_xsimd(env_vars)

  cat("**** arrow", ifelse(quietly, "", paste("with", env_vars)), "\n")
  status <- suppressWarnings(system(
    paste(env_vars, "inst/build_arrow_static.sh"),
    ignore.stdout = quietly, ignore.stderr = quietly
  ))
  if (status != 0) {
    # It failed :(
    cat("**** Error building Arrow C++. Re-run with ARROW_R_DEV=true for debug information.\n")
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

is_feature_requested <- function(arrow_feature) {
  # Cases:
  # * nothing set: OFF
  # * explicitly enabled: ON
  # * LIBARROW_MINIMAL=false: ON
  # Note that if LIBARROW_MINIMAL is unset, `configure` sets it to "false" when
  # NOT_CRAN or TEST_OFFLINE_BUILD are "true".
  explicitly_set_val <- toupper(Sys.getenv(arrow_feature))
  if (explicitly_set_val == "OFF") {
    feature_on <- FALSE
  } else {
    feature_on <- explicitly_set_val == "ON" || env_is("LIBARROW_MINIMAL", "false")
  }
  feature_on
}

remote_download_unavailable <- function(url_env_vars) {
  # Check the env vars
  # e.g. ARROW_MIMALLOC_URL should point to an existing file if !download_ok
  # Some dependencies require multiple downloads - check that all are available.
  # https://arrow.apache.org/docs/developers/cpp/building.html#offline-builds
  missing_local <- FALSE
  for (v in url_env_vars) {
    local_url <- Sys.getenv(v)
    missing_local <- missing_local || (local_url == "") || (!file.exists(local_url))
  }
  # This check is only relevant when Cmake would try to download things
  # (This check would change if we were using individual dependency resolution.)
  # https://arrow.apache.org/docs/developers/cpp/building.html#individual-dependency-resolution)
  download_required <- missing_local &&
    (toupper(Sys.getenv("ARROW_DEPENDENCY_SOURCE")) %in% c("", "BUNDLED", "AUTO"))
  download_unavailable <- download_required && (!download_ok)
  download_unavailable
}

# Memory alloc features: mimalloc, jemalloc
with_mimalloc <- function(env_vars) {
  # Note that the logic here is different than in build_arrow_static.sh, which
  # default includes mimalloc even when LIBARROW_MINIMAL=true
  arrow_mimalloc <- is_feature_requested("ARROW_MIMALLOC")

  if (arrow_mimalloc) {
    # User wants mimalloc. If they're using gcc, let's make sure the version is >= 4.9
    if (isTRUE(cmake_gcc_version(env_vars) < "4.9")) {
      cat("**** mimalloc support not available for gcc < 4.9; building with ARROW_MIMALLOC=OFF\n")
      arrow_mimalloc <- FALSE
    }
    download_unavailable <- remote_download_unavailable("ARROW_MIMALLOC_URL")
    if (download_unavailable) {
      cat(paste(
        "**** mimalloc needs to be downloaded, but can't be.",
        "See ?arrow::download_optional_dependencies.",
        "Building with ARROW_MIMALLOC=OFF\n"
      ))
      arrow_mimalloc <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_mimalloc, "ARROW_MIMALLOC=ON", "ARROW_MIMALLOC=OFF"))
}

with_jemalloc <- function(env_vars) {
  arrow_jemalloc <- is_feature_requested("ARROW_JEMALLOC") && !is_solaris()
  # jemalloc doesn't seem to build on Solaris
  if (arrow_jemalloc) {
    download_unavailable <- remote_download_unavailable("ARROW_JEMALLOC_URL")
    if (download_unavailable) {
      cat("**** jemalloc requested but cannot be downloaded. Setting ARROW_JEMALLOC=OFF\n")
      arrow_jemalloc <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_jemalloc, "ARROW_JEMALLOC=ON", "ARROW_JEMALLOC=OFF"))
}

# File access features: parquet, dataset, S3
with_parquet <- function(env_vars) {
  # We try to build parquet unless it's explicitly turned off, even if
  # LIBARROW_MINIMAL=true.
  # Parquet is built-in, but depends on Thrift, which is thirdparty
  arrow_parquet <- !env_is("ARROW_PARQUET", "off") && !is_solaris()
  # Thrift doesn't compile on solaris, so turn off parquet there.
  if (arrow_parquet) {
    download_unavailable <- remote_download_unavailable("ARROW_THRIFT_URL")
    if (download_unavailable) {
      cat("**** parquet requested but dependencies cannot be downloaded. Setting ARROW_PARQUET=OFF\n")
      arrow_parquet <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_parquet, "ARROW_PARQUET=ON", "ARROW_PARQUET=OFF"))
}

with_dataset <- function(env_vars) {
  # Note: we try to build dataset unless it's explicitly turned off, even if
  # LIBARROW_MINIMAL=true.
  arrow_dataset <- (!env_is("ARROW_DATASET", "off")) &&
    grepl("ARROW_PARQUET=ON", with_parquet(""))
  # arrowExports.cpp requires parquet for dataset (ARROW-11994), so turn dataset
  # off if parquet is off.
  paste(env_vars, ifelse(arrow_dataset, "ARROW_DATASET=ON", "ARROW_DATASET=OFF"))
}

with_s3_support <- function(env_vars) {
  arrow_s3 <- is_feature_requested("ARROW_S3")
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
    download_unavailable <- remote_download_unavailable(c(
      "ARROW_AWSSDK_URL",
      "ARROW_AWS_C_COMMON_URL",
      "ARROW_AWS_CHECKSUMS_URL",
      "ARROW_AWS_C_EVENT_STREAM_URL"
    ))
    if (download_unavailable) {
      cat(paste(
        "**** S3 dependencies need to be downloaded, but can't be.",
        "See ?arrow::download_optional_dependencies.",
        "Building with ARROW_S3=OFF\n"
      ))
      arrow_s3 <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_s3, "ARROW_S3=ON", "ARROW_S3=OFF"))
}

# Compression features: brotli, bz2, lz4, snappy, zlib, zstd
with_brotli <- function(env_vars) {
  arrow_brotli <- is_feature_requested("ARROW_WITH_BROTLI")
  if (arrow_brotli) {
    download_unavailable <- remote_download_unavailable("ARROW_BROTLI_URL")
    if (download_unavailable) {
      cat("**** brotli requested but cannot be downloaded. Setting ARROW_WITH_BROTLI=OFF\n")
      arrow_brotli <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_brotli, "ARROW_WITH_BROTLI=ON", "ARROW_WITH_BROTLI=OFF"))
}

with_bz2 <- function(env_vars) {
  arrow_bz2 <- is_feature_requested("ARROW_WITH_BZ2")
  if (arrow_bz2) {
    download_unavailable <- remote_download_unavailable("ARROW_BZIP2_URL")
    if (download_unavailable) {
      cat("**** bz2 requested but cannot be downloaded. Setting ARROW_WITH_BZ2=OFF\n")
      arrow_bz2 <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_bz2, "ARROW_WITH_BZ2=ON", "ARROW_WITH_BZ2=OFF"))
}

with_lz4 <- function(env_vars) {
  arrow_lz4 <- is_feature_requested("ARROW_WITH_LZ4")
  if (arrow_lz4) {
    download_unavailable <- remote_download_unavailable("ARROW_LZ4_URL")
    if (download_unavailable) {
      cat("**** lz4 requested but cannot be downloaded. Setting ARROW_WITH_LZ4=OFF\n")
      arrow_lz4 <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_lz4, "ARROW_WITH_LZ4=ON", "ARROW_WITH_LZ4=OFF"))
}

with_snappy <- function(env_vars) {
  arrow_snappy <- is_feature_requested("ARROW_WITH_SNAPPY")
  if (arrow_snappy) {
    download_unavailable <- remote_download_unavailable("ARROW_SNAPPY_URL")
    if (download_unavailable) {
      cat("**** snappy requested but cannot be downloaded. Setting ARROW_WITH_SNAPPY=OFF\n")
      arrow_snappy <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_snappy, "ARROW_WITH_SNAPPY=ON", "ARROW_WITH_SNAPPY=OFF"))
}

with_zlib <- function(env_vars) {
  arrow_zlib <- is_feature_requested("ARROW_WITH_ZLIB")
  if (arrow_zlib) {
    download_unavailable <- remote_download_unavailable("ARROW_ZLIB_URL")
    if (download_unavailable) {
      cat("**** zlib requested but cannot be downloaded. Setting ARROW_WITH_ZLIB=OFF\n")
      arrow_zlib <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_zlib, "ARROW_WITH_ZLIB=ON", "ARROW_WITH_ZLIB=OFF"))
}

with_zstd <- function(env_vars) {
  arrow_zstd <- is_feature_requested("ARROW_WITH_ZSTD")
  if (arrow_zstd) {
    download_unavailable <- remote_download_unavailable("ARROW_ZSTD_URL")
    if (download_unavailable) {
      cat("**** zstd requested but cannot be downloaded. Setting ARROW_WITH_ZSTD=OFF\n")
      arrow_zstd <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_zstd, "ARROW_WITH_ZSTD=ON", "ARROW_WITH_ZSTD=OFF"))
}

# Specific computations: json, re2, utf8proc, xsimd
with_json <- function(env_vars) {
  # Note: we try to build json unless it's explicitly turned off, even if
  # LIBARROW_MINIMAL=true.
  arrow_json <- (!env_is("ARROW_JSON", "off")) || (!env_is("ARROW_WITH_RAPIDJSON", "off"))
  if (arrow_json) {
    download_unavailable <- remote_download_unavailable("ARROW_RAPIDJSON_URL")
    if (download_unavailable) {
      cat("**** json requested but cannot be downloaded. Setting ARROW_JSON=OFF\n")
      arrow_json <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_json, "ARROW_WITH_JSON=ON", "ARROW_WITH_JSON=OFF"))
}

with_re2 <- function(env_vars) {
  # Note: we try to build re2 unless it's explicitly turned off, even if
  # LIBARROW_MINIMAL=true.
  arrow_re2 <- !env_is("ARROW_WITH_RE2", "off") && !is_solaris()
  # re2 and utf8proc do compile on Solaris
  # but `ar` fails to build libarrow_bundled_dependencies, so turn them off
  # so that there are no bundled deps
  if (arrow_re2) {
    download_unavailable <- remote_download_unavailable("ARROW_RE2_URL")
    if (download_unavailable) {
      cat("**** re2 requested but cannot be downloaded. Setting ARROW_WITH_RE2=OFF\n")
      arrow_re2 <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_re2, "ARROW_WITH_RE2=ON", "ARROW_WITH_RE2=OFF"))
}

with_utf8proc <- function(env_vars) {
  # Note: we try to build utf8proc unless it's explicitly turned off, even if
  # LIBARROW_MINIMAL=true.
  arrow_utf8proc <- !env_is("ARROW_WITH_UTF8PROC", "off") && !is_solaris()
  # re2 and utf8proc do compile on Solaris
  # but `ar` fails to build libarrow_bundled_dependencies, so turn them off
  # so that there are no bundled deps
  if (arrow_utf8proc) {
    download_unavailable <- remote_download_unavailable("ARROW_UTF8PROC_URL")
    if (download_unavailable) {
      cat("**** utf8proc requested but cannot be downloaded. Setting ARROW_WITH_UTF8PROC=OFF\n")
      arrow_utf8proc <- FALSE
    }
  }
  paste(env_vars, ifelse(arrow_utf8proc, "ARROW_WITH_UTF8PROC=ON", "ARROW_WITH_UTF8PROC=OFF"))
}

with_xsimd <- function(env_vars) {
  # xsimd doesn't compile on solaris, so set SIMD level to NONE to skip it.
  # Use it everywhere else (as long as xsimd is available)
  use_simd <- !is_solaris()
  if (use_simd) {
    download_unavailable <- remote_download_unavailable("ARROW_XSIMD_URL")
    if (download_unavailable) {
      cat("**** xsimd requested but cannot be downloaded. Setting EXTRA_CMAKE_FLAGS=-DARROW_SIMD_LEVEL=NONE\n")
      use_simd <- FALSE
    }
  }
  paste(env_vars, ifelse(use_simd, "", "EXTRA_CMAKE_FLAGS=-DARROW_SIMD_LEVEL=NONE"))
}

# Notes on other downloaded dependencies:
# Boost is required in some cases (Flight, Gandiva, S3, and tests, at least),
# but there's no such thing as ARROW_BOOST=OFF.
# It may be necessary to set BOOST_ROOT or ARROW_BOOST_URL for offline installs.
#
# Other URLs get downloaded, but afaik, are not used in the build.
# - ARROW_ABSL_URL - seems to be a dependency of gRPC
# - ARROW_CARES_URL - "a dependency of gRPC"
# - ARROW_GBENCHMARK_URL - "Google benchmark, for testing"
# - ARROW_GFLAGS_URL - "for command line utilities (formerly Googleflags)"
# - ARROW_GLOG_URL - "for logging"
# - ARROW_GRPC_URL - "for remote procedure calls"
# - ARROW_GTEST_URL - "Googletest, for testing"
# - ARROW_ORC_URL - "for Apache ORC format support"
# - ARROW_PROTOBUF_URL - "Google Protocol Buffers, for data serialization"


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
