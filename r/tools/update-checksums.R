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

# Run this script AFTER the release was voted and the artifacts
# are moved into the final dir. This script will download the checksum
# files and save them to the tools/checksums directory mirroring the
# artifactory layout. *libs.R uses these files to validated the downloaded
# binaries when installing the package.
#
# Run this script from the r/ directory of the arrow repo with the version
# as the first argument$ Rscript tools/update-checksum.R 14.0.0

args <- commandArgs(TRUE)
VERSION <- args[1]
tools_root <- ""

if (length(args) != 1) {
  stop("Usage: Rscript tools/update-checksums.R <version>")
}

tasks_yml <- "../dev/tasks/tasks.yml"

if (!file.exists(tasks_yml)) {
  stop("Run this script from the r/ directory of the arrow repo")
}

cat("Extracting libarrow binary paths from tasks.yml\n")
# Get the libarrow binary paths from the tasks.yml file
binary_paths <- readLines(tasks_yml) |>
  grep("r-lib__libarrow", x = _, value = TRUE) |>
  sub(".+r-lib__libarrow__bin__(.+\\.zip)", "\\1", x = _) |>
  sub("{no_rc_r_version}", VERSION, fixed = TRUE, x = _) |>
  sub("__", "/", x = _) |>
  sub("\\.zip", ".zip", fixed = TRUE, x = _)

artifactory_root <- "https://apache.jfrog.io/artifactory/arrow/r/%s/libarrow/bin/%s"

# Get the checksum file from the artifactory
for (path in binary_paths) {
  sha_path <- paste0(path, ".sha512")
  file <- file.path("tools/checksums", sha_path)
  dirname(file) |> dir.create(path = _, recursive = TRUE, showWarnings = FALSE)
  
  cat(paste0("Downloading ", sha_path, "\n"))
  url <- sprintf(artifactory_root, VERSION, sha_path)
  download.file(url, file, quiet = TRUE, cacheOK = FALSE)

  if (grepl("windows", path)) {
    cat(paste0("Converting ", path, " to windows style line endings\n"))
    # UNIX style line endings cause errors with mysys2 sha512sum
    sed_status <- system2("sed", args = c("-i", "s/\\\\r//", file))
    if (sed_status != 0) {
      stop("Failed to remove \\r from windows checksum file. Exit code: ", sed_status)
    }
  }
}

cat("Checksums updated successfully!\n")
