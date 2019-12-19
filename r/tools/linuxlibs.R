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
src_dir <- NULL
download_libs <- identical(tolower(Sys.getenv("NOT_CRAN")), "true") || identical(Sys.getenv("_R_CHECK_SIZE_OF_TARBALL_"), "")
# For local debugging, set ARROW_R_DEV=TRUE to make this script print more
quietly <- !identical(tolower(Sys.getenv("ARROW_R_DEV")), "true")

identify_os <- function(os = Sys.getenv("ARROW_BINARY_DISTRO")) {
  # Function to figure out which flavor of binary we should download.
  # By default, it will try to discover from the OS what distro-version we're on
  # but you can override this by setting the env var ARROW_BINARY_DISTRO to:
  # * `FALSE` (not case-sensitive), which tells the script not to download a binary,
  # * some other string, presumably a related "distro-version" that has binaries
  #   built that work for your OS
  if (nzchar(os)) {
    if (identical(tolower(os), "false")) {
      # Env var says not to download a binary
      return(NULL)
    } else {
      # Env var provided an os-version to use--maybe you're on Ubuntu 18.10 but
      # we only build for 18.04 and that's fine--so use what the user set
      return(os)
    }
  }

  has_lsb <- system("which lsb_release", ignore.stdout = TRUE) == 0
  if (has_lsb) {
    distro <- tolower(system("lsb_release -is", intern = TRUE))
    os_version <- system("lsb_release -rs", intern = TRUE)
    # In the future, we may be able to do some mapping of distro-versions to
    # versions we built for, since there's no way we'll build for everything.
    os <- paste0(distro, "-", os_version)
  } else if (file.exists("/etc/system-release")) {
    # Something like "CentOS Linux release 7.7.1908 (Core)"
    system_release <- tolower(head(readLines("/etc/system-release"), 1))
    # Extract from that the distro and the major version number
    os <- sub("^([a-z]+) .*([0-9]+).*$", "\\1-\\2", system_release)
  } else {
    cat("*** Unable to identify current OS/version\n")
    os <- NULL
  }
  return(os)
}

if (!file.exists(paste0(dst_dir, "/include/arrow/api.h"))) {
  # If we're working in a local checkout and have already built the libs, we
  # don't need to do anything. Otherwise,
  if (length(args) > 1) {
    # Arg 2 would be the path/to/lib.zip
    # TODO: do we need this option?
    localfile <- args[2]
    if (file.exists(localfile)) {
      cat(sprintf("*** Using LOCAL_LIBARROW %s\n", localfile))
      file.copy(localfile, "lib.zip")
    } else {
      cat(sprintf("*** LOCAL_LIBARROW %s does not exist\n", localfile))
    }
  } else if (download_libs) {
    base_url <- "https://dl.bintray.com/ursa-labs/arrow-r/libarrow/"
    # Determine (or take) distro
    os <- identify_os()
    if (!is.null(os)) {
      # Download it, if allowed
      binary_url <- paste0(base_url, os, "/arrow-", VERSION, ".zip")
      try(
        download.file(binary_url, "lib.zip", quiet = quietly),
        silent = quietly
      )
    }
    if (file.exists("lib.zip")) {
      cat(sprintf("*** Successfully retrieved C++ binaries for %s\n", os))
    } else {
      if (!is.null(os)) {
        cat(sprintf("*** No C++ binaries found for %s\n", os))
      }
      # Try to build C++ from source
      tf1 <- tempfile()
      source_url <- paste0(base_url, "src/arrow-", VERSION, ".zip")
      try(
        download.file(source_url, tf1, quiet = quietly),
        silent = quietly
      )
      if (file.exists(tf1)) {
        cat("*** Successfully retrieved C++ source\n")
        src_dir <- tempfile()
        unzip(tf1, exdir = src_dir)
        unlink(tf1)
        on.exit(unlink(src_dir))
      } else if (file.exists("../cpp/src/arrow/api.h")) {
        # We're in a git checkout of arrow, so we can build it
        cat("*** Found local C++ source\n")
        src_dir <- "../cpp"
      }
    }
  } else {
    cat("*** Proceeding without C++ dependencies\n")
  }

  if (file.exists("lib.zip")) {
    # Finish up for the cases where we got a zip file of the libs
    dir.create(dst_dir, showWarnings = !quietly, recursive = TRUE)
    unzip("lib.zip", exdir = dst_dir)
    unlink("lib.zip")
  } else if (!is.null(src_dir)) {
    cat("*** Building C++ libraries\n")
    # We'll need to compile R bindings with these libs, so delete any .o files
    system("rm src/*.o")
    env_vars <- sprintf(
      "SOURCE_DIR=%s BUILD_DIR=libarrow/build DEST_DIR=%s", src_dir, dst_dir
    )
    system(paste(env_vars, "inst/build_arrow_static.sh"))
  }
}
