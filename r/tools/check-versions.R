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

# TESTING is set in test-check-version.R; it won't be set when called from configure
test_mode <- exists("TESTING")

release_version_supported <- function(r_version, cpp_version) {
  r_version <- package_version(r_version)
  cpp_version <- package_version(cpp_version)
  major <- function(x) as.numeric(x[1, 1])
  minimum_cpp_version <- package_version("13.0.0")

  allow_mismatch <- identical(tolower(Sys.getenv("ARROW_R_ALLOW_CPP_VERSION_MISMATCH", "false")), "true")
  # If we allow a version mismatch we still need to cover the minimum version (13.0.0 for now)
  # we don't allow newer C++ versions as new features without additional feature gates are likely to
  # break the R package
  version_valid <- cpp_version >= minimum_cpp_version && major(cpp_version) <= major(r_version)
  allow_mismatch && version_valid || major(r_version) == major(cpp_version)
}

check_versions <- function(r_version, cpp_version) {
  r_parsed <- package_version(r_version)
  r_dev_version <- r_parsed[1, 4]
  r_is_dev <- !is.na(r_dev_version) && r_dev_version > "100"
  r_is_patch <- !is.na(r_dev_version) && r_dev_version <= "100"
  cpp_is_dev <- grepl("SNAPSHOT$", cpp_version)
  cpp_parsed <- package_version(sub("-SNAPSHOT$", "", cpp_version))

  major <- function(x) as.numeric(x[1, 1])
  # R and C++ denote dev versions differently
  # R is current.release.9000, C++ is next.release-SNAPSHOT
  # So a "match" is if the R major version + 1 = C++ major version
  if (r_is_dev && cpp_is_dev && major(r_parsed) + 1 == major(cpp_parsed)) {
    msg <- c(
      sprintf("*** > Packages are both on development versions (%s, %s)", cpp_version, r_version),
      "*** > If installation fails, rebuild the C++ library to match the R version",
      "*** > or retry with FORCE_BUNDLED_BUILD=true"
    )
    cat(paste0(msg, "\n", collapse = ""))
  } else if (cpp_is_dev || !release_version_supported(r_version, cpp_parsed)) {
    cat(
      sprintf(
        "**** Not using: C++ library version (%s): not supported by R package version %s\n",
        cpp_version,
        r_version
      )
    )
    stop("version mismatch")
    # Add ALLOW_VERSION_MISMATCH env var to override stop()? (Could be useful for debugging)
  } else {
    # OK
    cat(
      sprintf(
        "**** C++ library version %s is supported by R version %s\n",
        cpp_version, r_version
      )
    )
  }
}

if (!test_mode) {
  check_versions(args[1], args[2])
}
