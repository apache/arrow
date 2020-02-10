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
apache_src_url <- paste0(
  "https://www.apache.org/dyn/closer.cgi/arrow/arrow-", VERSION,
  "/apache-arrow-", VERSION, ".tar.gz"
)

options(.arrow.cleanup = character()) # To collect dirs to rm on exit
on.exit(unlink(getOption(".arrow.cleanup")))

env_is <- function(var, value) identical(tolower(Sys.getenv(var)), value)

# This condition identifies when you're installing locally, either by presence
# of NOT_CRAN or by absence of a `check` env var. So even if you don't have
# NOT_CRAN set in your dev environment, this will still pass outside R CMD check
locally_installing <- env_is("NOT_CRAN", "true") || env_is("_R_CHECK_SIZE_OF_TARBALL_", "")
# Combine with explicit vars to turn off downloading and building. I.e. only
# downloads/builds when local, but can turn off either with these env vars.
# * no download, build_ok: Only build with local git checkout
# * download_ok, no build: Only use prebuilt binary, if found
# * neither: Get the arrow-without-arrow package
download_ok <- locally_installing && !env_is("LIBARROW_DOWNLOAD", "false")
build_ok <- locally_installing && !env_is("LIBARROW_BUILD", "false")
# TODO: allow LIBARROW_DOWNLOAD=true to override locally_installing? or just set NOT_CRAN?

# For local debugging, set ARROW_R_DEV=TRUE to make this script print more
quietly <- !env_is("ARROW_R_DEV", "true")

download_binary <- function(os = identify_os()) {
  libfile <- tempfile()
  if (!is.null(os)) {
    binary_url <- paste0(arrow_repo, "bin/", os, "/arrow-", VERSION, ".zip")
    try(
      download.file(binary_url, libfile, quiet = quietly),
      silent = quietly
    )
    if (file.exists(libfile)) {
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
# By default ("FALSE"), it will not download a precompiled library,
# but you can override this by setting the env var LIBARROW_BINARY_DISTRO to:
# * `TRUE` (not case-sensitive), to try to discover your current OS, or
# * some other string, presumably a related "distro-version" that has binaries
#   built that work for your OS
identify_os <- function(os = Sys.getenv("LIBARROW_BINARY_DISTRO", "false")) {
    if (identical(tolower(os), "false")) {
      # Env var says not to download a binary
      return(NULL)
    } else if (!identical(tolower(os), "true")) {
      # Env var provided an os-version to use--maybe you're on Ubuntu 18.10 but
      # we only build for 18.04 and that's fine--so use what the user set
      return(os)
    }

  if (nzchar(Sys.which("lsb_release"))) {
    distro <- tolower(system("lsb_release -is", intern = TRUE))
    os_version <- system("lsb_release -rs", intern = TRUE)
    # In the future, we may be able to do some mapping of distro-versions to
    # versions we built for, since there's no way we'll build for everything.
    os <- paste0(distro, "-", os_version)
  } else if (file.exists("/etc/os-release")) {
    os_release <- readLines("/etc/os-release")
    vals <- sub("^.*=(.*)$", "\\1", os_release)
    names(vals) <- sub("^(.*)=.*$", "\\1", os_release)
    distro <- gsub('"', '', vals["ID"])
    if (distro == "ubuntu") {
      # Keep major.minor version
      version_regex <- '^"?([0-9]+\\.[0-9]+).*"?.*$'
    } else {
      # Only major version number
      version_regex <- '^"?([0-9]+).*"?.*$'
    }
    os_version <- sub(version_regex, "\\1", vals["VERSION_ID"])
    os <- paste0(distro, "-", os_version)
  } else if (file.exists("/etc/system-release")) {
    # Something like "CentOS Linux release 7.7.1908 (Core)"
    system_release <- tolower(utils::head(readLines("/etc/system-release"), 1))
    # Extract from that the distro and the major version number
    os <- sub("^([a-z]+) .* ([0-9]+).*$", "\\1-\\2", system_release)
  } else {
    cat("*** Unable to identify current OS/version\n")
    os <- NULL
  }

  # Now look to see if we can map this os-version to one we have binaries for
  os <- find_available_binary(os)
  os
}

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
  src_dir <- NULL
  source_url <- paste0(arrow_repo, "src/arrow-", VERSION, ".zip")
  try(
    download.file(source_url, tf1, quiet = quietly),
    silent = quietly
  )
  if (!file.exists(tf1)) {
    # Try for an official release
    try(
      download.file(apache_src_url, tf1, quiet = quietly),
      silent = quietly
    )
  }
  if (file.exists(tf1)) {
    cat("*** Successfully retrieved C++ source\n")
    src_dir <- tempfile()
    unzip(tf1, exdir = src_dir)
    unlink(tf1)
    # These scripts need to be executable
    system(sprintf("chmod 755 %s/cpp/build-support/*.sh", src_dir))
    options(.arrow.cleanup = c(getOption(".arrow.cleanup"), src_dir))
    # The actual src is in cpp
    src_dir <- paste0(src_dir, "/cpp")
  }
  src_dir
}

find_local_source <- function() {
  if (file.exists("../cpp/src/arrow/api.h")) {
    # We're in a git checkout of arrow, so we can build it
    cat("*** Found local C++ source\n")
    return("../cpp")
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
    makeflags <- sprintf("-j%s", parallel::detectCores())
    Sys.setenv(MAKEFLAGS = makeflags)
  }
  if (!quietly) {
    cat("*** Building with MAKEFLAGS=", makeflags, "\n")
  }
  # Check for libarrow build dependencies:
  # * cmake
  # * flex and bison (for building thrift)
  # * m4 (for building flex and bison)
  cmake <- ensure_cmake()
  m4 <- ensure_m4()
  flex <- ensure_flex(m4)
  bison <- ensure_bison(m4)
  env_vars <- sprintf(
    "SOURCE_DIR=%s BUILD_DIR=libarrow/build DEST_DIR=%s CMAKE=%s",
    src_dir,                                dst_dir,    cmake
  )
  if (!is.null(flex)) {
    system(paste0(flex, "/flex --version"))
    env_vars <- paste0(env_vars, " FLEX_ROOT=", flex)
  }
  if (!is.null(bison)) {
    system(paste0(bison, "/bison --version"))
    env_vars <- sprintf(
      "PATH=%s:$PATH %s BISON_PKGDATADIR=%s/../share/bison",
            bison,   env_vars,           bison
    )
  }
  if (!quietly) {
    cat("*** Building with ", env_vars, "\n")
  }
  system(paste(env_vars, "inst/build_arrow_static.sh"))
}

ensure_cmake <- function() {
  cmake <- Sys.which("cmake")
  if (!nzchar(cmake)) {
    # If not found, download it
    cat("*** Downloading cmake\n")
    CMAKE_VERSION <- Sys.getenv("CMAKE_VERSION", "3.16.2")
    cmake_binary_url <- paste0(
      "https://github.com/Kitware/CMake/releases/download/v", CMAKE_VERSION,
      "/cmake-", CMAKE_VERSION, "-Linux-x86_64.tar.gz"
    )
    cmake_tar <- tempfile()
    cmake_dir <- tempfile()
    try(
      download.file(cmake_binary_url, cmake_tar, quiet = quietly),
      silent = quietly
    )
    untar(cmake_tar, exdir = cmake_dir)
    unlink(cmake_tar)
    options(.arrow.cleanup = c(getOption(".arrow.cleanup"), cmake_dir))
    cmake <- paste0(
      cmake_dir,
      "/cmake-", CMAKE_VERSION, "-Linux-x86_64",
      "/bin/cmake"
    )
  }
  cmake
}

# TODO: move ensure_flex/bison/m4 to cmake: https://issues.apache.org/jira/browse/ARROW-7501
ensure_flex <- function(m4 = ensure_m4()) {
  if (nzchar(Sys.which("flex"))) {
    # We already have flex.
    # NULL will tell the caller not to append FLEX_ROOT to env vars bc it's not needed
    return(NULL)
  }
  # If not found, download it
  cat("*** Downloading and building flex\n")
  # Flex 2.6.4 (latest release) causes segfaults on some platforms (ubuntu bionic, debian e.g.)
  # See https://github.com/westes/flex/issues/219
  # Allegedly it has been fixed in master but there hasn't been a release since May 2017
  FLEX_VERSION <- Sys.getenv("FLEX_VERSION", "2.6.3")
  flex_source_url <- paste0(
    "https://github.com/westes/flex/releases/download/v", FLEX_VERSION,
    "/flex-", FLEX_VERSION, ".tar.gz"
  )
  flex_tar <- tempfile()
  flex_dir <- tempfile()
  try(
    download.file(flex_source_url, flex_tar, quiet = quietly),
    silent = quietly
  )
  untar(flex_tar, exdir = flex_dir)
  unlink(flex_tar)
  options(.arrow.cleanup = c(getOption(".arrow.cleanup"), flex_dir))
  # flex also needs m4
  if (!is.null(m4)) {
    # If we just built it, put it on PATH for building bison
    path <- sprintf('export PATH="%s:$PATH" && ', m4)
  } else {
    path <- ""
  }
  # Now, build flex
  flex_dir <- paste0(flex_dir, "/flex-", FLEX_VERSION)
  cmd <- sprintf("cd %s && ./configure && make", shQuote(flex_dir))
  system(paste0(path, cmd))
  # The built flex should be in ./src. Return that so we can set as FLEX_ROOT
  paste0(flex_dir, "/src")
}

ensure_bison <- function(m4 = ensure_m4()) {
  if (nzchar(Sys.which("bison"))) {
    # We already have bison.
    # NULL will tell the caller not to append BISON_ROOT to env vars bc it's not needed
    return(NULL)
  }
  # If not found, download it
  cat("*** Downloading and building bison\n")
  BISON_VERSION <- Sys.getenv("BISON_VERSION", "3.5")
  source_url <- paste0("https://ftp.gnu.org/gnu/bison/bison-", BISON_VERSION, ".tar.gz")
  tar_file <- tempfile()
  build_dir <- tempfile()
  install_dir <- tempfile()
  try(
    download.file(source_url, tar_file, quiet = quietly),
    silent = quietly
  )
  untar(tar_file, exdir = build_dir)
  unlink(tar_file)
  on.exit(unlink(build_dir))
  options(.arrow.cleanup = c(getOption(".arrow.cleanup"), install_dir))
  # bison also needs m4, so let's make sure we have that too
  # (we probably don't if we're here)
  if (!is.null(m4)) {
    # If we just built it, put it on PATH for building bison
    path <- sprintf('export PATH="%s:$PATH" && ', m4)
  } else {
    path <- ""
  }
  # Now, build bison
  build_dir <- paste0(build_dir, "/bison-", BISON_VERSION)
  cmd <- sprintf(
    "cd %s && ./configure --prefix=%s && make && make install",
        shQuote(build_dir),          install_dir
  )
  system(paste0(path, cmd))
  # Return the path to the bison binaries
  paste0(install_dir, "/bin")
}

ensure_m4 <- function() {
  if (nzchar(Sys.which("m4"))) {
    # We already have m4.
    return(NULL)
  }
  # If not found, download it
  cat("*** Downloading and building m4\n")
  M4_VERSION <- Sys.getenv("M4_VERSION", "1.4.18")
  source_url <- paste0("https://ftp.gnu.org/gnu/m4/m4-", M4_VERSION, ".tar.gz")
  tar_file <- tempfile()
  dst_dir <- tempfile()
  try(
    download.file(source_url, tar_file, quiet = quietly),
    silent = quietly
  )
  untar(tar_file, exdir = dst_dir)
  unlink(tar_file)
  options(.arrow.cleanup = c(getOption(".arrow.cleanup"), dst_dir))
  # bison also needs m4, so let's make sure we have that too
  # (we probably don't if we're here)

  # Now, build it
  dst_dir <- paste0(dst_dir, "/m4-", M4_VERSION)
  system(sprintf("cd %s && ./configure && make", shQuote(dst_dir)))
  # The built m4 should be in ./src. Return that so we can put that on the PATH
  paste0(dst_dir, "/src")
}

#####

if (!file.exists(paste0(dst_dir, "/include/arrow/api.h"))) {
  # If we're working in a local checkout and have already built the libs, we
  # don't need to do anything. Otherwise,
  # (1) Look for a prebuilt binary for this version
  bin_file <- src_dir <- NULL
  if (download_ok) {
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
    }
  } else {
   cat("*** Proceeding without C++ dependencies\n")
  }
}
