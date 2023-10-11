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
dev_version <- package_version(VERSION)[1, 4]
# Small dev versions are added for R-only changes during CRAN submission
is_release <- is.na(dev_version) || dev_version < "100"
env_is <- function(var, value) identical(tolower(Sys.getenv(var)), value)
# We want to log the message in the style of the configure script
# not as an R error. Use `return` to exit the script after logging.
lg <- function(...) {
  cat("*** ", sprintf(...), "\n")
}

if (is_release) {
  # This is a release version, so we need to use the major.minor.patch version without
  # the CRAN suffix/dev_version
  VERSION <- package_version(VERSION)[1, 1:3]
  # %1$s uses the first variable for both substitutions
  url_template <- paste0(
    getOption("arrow.repo", "https://apache.jfrog.io/artifactory/arrow/r/%1$s"),
    "/libarrow/bin/windows/arrow-%1$s.zip"
  )
} else {
  url_template <- paste0(
    getOption("arrow.dev_repo", "https://nightlies.apache.org/arrow/r"),
    "/libarrow/bin/windows/arrow-%s.zip"
  )
}

if (file.exists(sprintf("windows/arrow-%s/include/arrow/api.h", VERSION))) {
  lg("Found local Arrow %s!", VERSION)
  return()
}

zip_file <- sprintf("arrow-%s.zip", VERSION)

if (length(args) > 1) {
  # Arg 2 would be the path/to/lib.zip
  localfile <- args[2]
  if (!file.exists(localfile)) {
    lg("RWINLIB_LOCAL '%s' does not exist. Build will fail.", localfile)
    return()
  } else {
    lg("Using RWINLIB_LOCAL %s", localfile)
  }
  file.copy(localfile, zip_file)
} else {
  quietly <- !identical(tolower(Sys.getenv("ARROW_R_DEV")), "true")
  binary_url <- sprintf(url_template, VERSION)
  try(
    suppressWarnings(
      download.file(binary_url, zip_file, quiet = quietly)
    ),
    silent = quietly
  )

  if (!file.exists(zip_file) || file.size(zip_file) == 0) {
    lg("Failed to download libarrow binary from %s. Build will fail.", binary_url)
    return()
  }

  checksum_path <- Sys.getenv("ARROW_R_CHECKSUM_PATH", "tools/checksums")
  # Explicitly setting the env var to "false" will skip checksum validation
  # e.g. in case the included checksums are stale.
  skip_checksum <- env_is("ARROW_R_ENFORCE_CHECKSUM", "false")
  enforce_checksum <- env_is("ARROW_R_ENFORCE_CHECKSUM", "true")
  # validate binary checksum for CRAN release only
  if (!skip_checksum && dir.exists(checksum_path) && is_release ||
    enforce_checksum) {
    checksum_file <- sprintf("%s/windows/arrow-%s.zip.sha512", checksum_path, VERSION)
    # rtools does not have shasum with default config
    checksum_ok <- system2("sha512sum", args = c("--status", "-c", checksum_file))

    if (checksum_ok != 0) {
      lg("Checksum validation failed for libarrow binary: %s", zip_file)
      return()
    }
    lg("Checksum validated successfully for libarrow binary: %s", zip_file)
  }
}

dir.create("windows", showWarnings = FALSE)
unzip(zip_file, exdir = "windows")
unlink(zip_file)
