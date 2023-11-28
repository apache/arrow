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
  } else if (r_is_patch && as.character(r_parsed[1, 1:3]) == cpp_version) {
    # Patch releases we do for CRAN feedback get an extra x.y.z.1 version.
    # These should work with the x.y.z C++ library (which never has .1 added)
    cat(
      sprintf(
        "*** > Using C++ library version %s with R package %s\n",
        cpp_version,
        r_version
      )
    )
  } else if (r_version != cpp_version) {
    cat(
      sprintf(
        "**** Not using: C++ library version (%s) does not match R package (%s)\n",
        cpp_version,
        r_version
      )
    )
    stop("version mismatch")
    # Add ALLOW_VERSION_MISMATCH env var to override stop()? (Could be useful for debugging)
  } else {
    # OK
    cat(sprintf("**** C++ and R library versions match: %s\n", cpp_version))
  }
}

if (!test_mode) {
  check_versions(args[1], args[2])
}
