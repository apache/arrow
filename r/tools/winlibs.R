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
# Small dev versions are added for R-only changes during CRAN submission.
is_release <- is.na(dev_version) || dev_version < "100"
env_is <- function(var, value) identical(tolower(Sys.getenv(var)), value)

if (!file.exists(sprintf("windows/arrow-%s/include/arrow/api.h", VERSION))) {
  if (length(args) > 1) {
    # Arg 2 would be the path/to/lib.zip
    localfile <- args[2]
    cat(sprintf("*** Using RWINLIB_LOCAL %s\n", localfile))
    if (!file.exists(localfile)) {
      cat(sprintf("*** %s does not exist; build will fail\n", localfile))
    }
    zip_file <- "lib.zip"
    file.copy(localfile, zip_file)
  } else {
    # Download static arrow from the apache artifactory
    quietly <- !identical(tolower(Sys.getenv("ARROW_R_DEV")), "true")
    get_file <- function(template, version, dest_file) {
      try(
        suppressWarnings(
          download.file(sprintf(template, version), dest_file, quiet = quietly)
        ),
        silent = quietly
      )
    }

    # URL templates
    nightly <- paste0(
      getOption("arrow.dev_repo", "https://nightlies.apache.org/arrow/r"),
      "/libarrow/bin/windows/arrow-%s.zip"
    )
    # %1$s uses the first variable for both substitutions
    artifactory <- paste0(
      getOption("arrow.repo", "https://apache.jfrog.io/artifactory/arrow/r/%1$s"),
      "/libarrow/bin/windows/arrow-%1$s.zip"
    )

    zip_file <- sprintf("arrow-%s.zip", VERSION)

    if (is_release) {
      VERSION <- package_version(VERSION)[1, 1:3]
      zip_file <- sprintf("arrow-%s.zip", VERSION)

      get_file(artifactory, VERSION, zip_file)
    } else {
      get_file(nightly, VERSION, zip_file)
    }

    checksum_path <- Sys.getenv("ARROW_R_CHECKSUM_PATH", "tools/checksums")
    # validate binary checksum for CRAN release only
    if (dir.exists(checksum_path) && is_release ||
      env_is("ARROW_R_ENFORCE_CHECKSUM", "true")) {
      checksum_file <- sprintf("%s/windows/arrow-%s.zip.sha512", checksum_path, VERSION)
      # rtools does not have shasum with default config
      checksum_ok <- system2("sha512sum", args = c("--status", "-c", checksum_file))

      if (checksum_ok != 0) {
        stop("*** Checksum validation failed for libarrow binary: ", zip_file)
      }
      cat("*** Checksum validated successfully for libarrow binary: ", zip_file, "\n")
    }
  }
  dir.create("windows", showWarnings = FALSE)
  unzip(zip_file, exdir = "windows")
  unlink(zip_file)
}
