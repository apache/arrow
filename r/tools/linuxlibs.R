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

arrow_repo <- "https://dl.bintray.com/ursalabs/arrow-r/libarrow/"

options(.arrow.cleanup = character()) # To collect dirs to rm on exit
on.exit(unlink(getOption(".arrow.cleanup")))

env_is <- function(var, value) identical(tolower(Sys.getenv(var)), value)
# * no download, build_ok: Only build with local git checkout
# * download_ok, no build: Only use prebuilt binary, if found
# * neither: Get the arrow-without-arrow package
# Download and build are OK unless you say not to
download_ok <- !env_is("LIBARROW_DOWNLOAD", "false")
build_ok <- !env_is("LIBARROW_BUILD", "false")
# But binary defaults to not OK
binary_ok <- !identical(tolower(Sys.getenv("LIBARROW_BINARY", "false")), "false")
# For local debugging, set ARROW_R_DEV=TRUE to make this script print more
quietly <- !env_is("ARROW_R_DEV", "true")

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

download_binary <- function(os = identify_os()) {
  libfile <- tempfile()
  if (!is.null(os)) {
    # See if we can map this os-version to one we have binaries for
    os <- find_available_binary(os)
    binary_url <- paste0(arrow_repo, "bin/", os, "/arrow-", VERSION, ".zip")
    if (try_download(binary_url, libfile)) {
      cat(sprintf("*** Successfully retrieved C++ binaries for %s\n", os))
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
identify_os <- function(os = Sys.getenv("LIBARROW_BINARY", Sys.getenv("LIBARROW_DOWNLOAD"))) {
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
      out$codename = vals[["PRETTY_NAME"]]
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

download_source <- function() {
  tf1 <- tempfile()
  src_dir <- tempfile()
  if (bintray_download(tf1)) {
    # First try from bintray
    cat("*** Successfully retrieved C++ source\n")
    unzip(tf1, exdir = src_dir)
    unlink(tf1)
    src_dir <- paste0(src_dir, "/cpp")
  } else if (apache_download(tf1)) {
    # If that fails, try for an official release
    cat("*** Successfully retrieved C++ source\n")
    untar(tf1, exdir = src_dir)
    unlink(tf1)
    src_dir <- paste0(src_dir, "/apache-arrow-", VERSION, "/cpp")
  }

  if (dir.exists(src_dir)) {
    options(.arrow.cleanup = c(getOption(".arrow.cleanup"), src_dir))
    # These scripts need to be executable
    system(
      sprintf("chmod 755 %s/build-support/*.sh", src_dir),
      ignore.stdout = quietly, ignore.stderr = quietly
    )
    return(src_dir)
  } else {
    return(NULL)
  }
}

bintray_download <- function(destfile) {
  source_url <- paste0(arrow_repo, "src/arrow-", VERSION, ".zip")
  try_download(source_url, destfile)
}

apache_download <- function(destfile, n_mirrors = 3) {
  apache_path <- paste0("arrow/arrow-", VERSION, "/apache-arrow-", VERSION, ".tar.gz")
  apache_urls <- c(
    # This returns a different mirror each time
    rep("https://www.apache.org/dyn/closer.lua?action=download&filename=", n_mirrors),
    "https://downloads.apache.org/" # The backup
  )
  downloaded <- FALSE
  for (u in apache_urls) {
    downloaded <- try_download(paste0(u, apache_path), destfile)
    if (downloaded) {
      break
    }
  }
  downloaded
}

find_local_source <- function(arrow_home = Sys.getenv("ARROW_HOME", "..")) {
  if (file.exists(paste0(arrow_home, "/cpp/src/arrow/api.h"))) {
    # We're in a git checkout of arrow, so we can build it
    cat("*** Found local C++ source\n")
    return(paste0(arrow_home, "/cpp"))
  } else {
    return(NULL)
  }
}

build_libarrow <- function(src_dir, dst_dir) {
  # We'll need to compile R bindings with these libs, so delete any .o files
  system("rm src/*.o", ignore.stdout = quietly, ignore.stderr = quietly)
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
    # cf. tools::Rcmd, introduced R 3.3
    system2(file.path(R.home("bin"), "R"), c("CMD", "config", var), stdout = TRUE)
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
  env_vars <- paste(
    names(env_var_list), dQuote(env_var_list, FALSE),
    sep = "=", collapse = " "
  )
  cat("**** arrow", ifelse(quietly, "", paste("with", env_vars)), "\n")
  status <- system(
    paste(env_vars, "inst/build_arrow_static.sh"),
    ignore.stdout = quietly, ignore.stderr = quietly
  )
  if (status != 0) {
    # It failed :(
    cat("**** Error building Arrow C++. Re-run with ARROW_R_DEV=true for debug information.\n")
  }
  invisible(status)
}

ensure_cmake <- function() {
  cmake <- Sys.which("cmake")
  if (!nzchar(cmake) || cmake_version() < 3.2) {
    # If not found, download it
    cat("**** cmake\n")
    CMAKE_VERSION <- Sys.getenv("CMAKE_VERSION", "3.16.2")
    cmake_binary_url <- paste0(
      "https://github.com/Kitware/CMake/releases/download/v", CMAKE_VERSION,
      "/cmake-", CMAKE_VERSION, "-Linux-x86_64.tar.gz"
    )
    cmake_tar <- tempfile()
    cmake_dir <- tempfile()
    try_download(cmake_binary_url, cmake_tar)
    untar(cmake_tar, exdir = cmake_dir)
    unlink(cmake_tar)
    options(.arrow.cleanup = c(getOption(".arrow.cleanup"), cmake_dir))
    cmake <- paste0(
      cmake_dir,
      "/cmake-", CMAKE_VERSION, "-Linux-x86_64",
      "/bin/cmake"
    )
  } else {
    # Sys.which() returns a named vector, but that plays badly with c() later
    names(cmake) <- NULL
  }
  cmake
}

cmake_version <- function() {
  tryCatch(
    {
      raw_version <- system("cmake --version", intern = TRUE, ignore.stderr = TRUE)
      pat <- ".*?([0-9]+\\.[0-9]+\\.[0-9]+).*"
      which_line <- grep(pat, raw_version)
      package_version(sub(pat, "\\1", raw_version[which_line]))
    },
    error = function(e) return(0)
  )
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
    if (download_ok) {
      src_dir <- download_source()
    }
    if (is.null(src_dir)) {
      src_dir <- find_local_source()
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
