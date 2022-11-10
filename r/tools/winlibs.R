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
if (!file.exists(sprintf("windows/arrow-%s/include/arrow/api.h", VERSION))) {
  if (length(args) > 1) {
    # Arg 2 would be the path/to/lib.zip
    localfile <- args[2]
    cat(sprintf("*** Using RWINLIB_LOCAL %s\n", localfile))
    if (!file.exists(localfile)) {
      cat(sprintf("*** %s does not exist; build will fail\n", localfile))
    }
    file.copy(localfile, "lib.zip")
  } else {
    # Download static arrow from the apache artifactory
    quietly <- !identical(tolower(Sys.getenv("ARROW_R_DEV")), "true")
    get_file <- function(template, version) {
      try(
        suppressWarnings(
          download.file(sprintf(template, version), "lib.zip", quiet = quietly)
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
    rwinlib <- "https://github.com/rwinlib/arrow/archive/v%s.zip"

    dev_version <- package_version(VERSION)[1, 4]

    # Small dev versions are added for R-only changes during CRAN submission.
    if (is.na(dev_version) || dev_version < 100) {
      VERSION <- package_version(VERSION)[1, 1:3]
      get_file(rwinlib, VERSION)

      # If not found, fall back to apache artifactory
      if (!file.exists("lib.zip")) {
        get_file(artifactory, VERSION)
      }
    } else {
      get_file(nightly, VERSION)
    }
  }
  dir.create("windows", showWarnings = FALSE)
  unzip("lib.zip", exdir = "windows")
  unlink("lib.zip")
}
