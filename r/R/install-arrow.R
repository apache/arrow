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

#' Help installing the Arrow C++ library
#'
#' Binary package installations should come with a working Arrow C++ library,
#' but when installing from source, you'll need to obtain the C++ library
#' first. This function offers guidance on how to get the C++ library depending
#' on your operating system and package version.
#' @export
#' @importFrom utils packageVersion installed.packages
install_arrow <- function() {
  os <- tolower(Sys.info()[["sysname"]])
  # c("windows", "darwin", "linux", "sunos") # win/mac/linux/solaris
  version <- packageVersion("arrow")
  # From CRAN check:
  rep <- installed.packages(fields="Repository")["arrow", "Repository"]
  from_cran <- identical(rep, "CRAN")
  # Is it possible to tell if was a binary install from CRAN vs. source?

  message(install_arrow_msg(arrow_available(), version, from_cran, os))
}

install_arrow_msg <- function(has_arrow, version, from_cran, os) {
  # TODO: check if there is a newer version on CRAN?

  # install_arrow() sends "version" as a "package_version" class, but for
  # convenience, this also accepts a string like "0.13.0". Calling
  # `package_version` is idempotent so do it again, and then `unclass` to get
  # the integers. Then see how many there are.
  dev_version <- length(unclass(package_version(version))[[1]]) > 3
  # Based on these parameters, assemble a string with installation advice
  if (has_arrow) {
    # Respond that you already have it
    msg <- ALREADY_HAVE
  } else if (os == "sunos") {
    # Good luck with that.
    msg <- c(SEE_DEV_GUIDE, THEN_REINSTALL)
  } else if (os == "linux") {
    if (dev_version) {
      # Point to compilation instructions on readme
      msg <- c(SEE_DEV_GUIDE, THEN_REINSTALL)
    } else {
      # Suggest arrow.apache.org/install for PPAs, or compilation instructions
      msg <- c(paste(SEE_ARROW_INSTALL, OR_SEE_DEV_GUIDE), THEN_REINSTALL)
    }
  } else if (!dev_version && !from_cran) {
    # Windows or Mac with a released version but not from CRAN
    # Recommend installing released binary package from CRAN
    msg <- INSTALL_FROM_CRAN
  } else {
    # Windows or Mac, most likely a dev version
    # for each OS, recommend dev installation, refer to readme
    # TODO: if there is a newer version on CRAN, recommend CRAN
    if (os == "windows") {
      msg <- c(paste(FIND_WIN_BINARY, OR_SEE_DEV_GUIDE), THEN_REINSTALL)
    } else {
      # macOS
      msg <- c(paste(FIND_MAC_BINARY, OR_SEE_DEV_GUIDE), THEN_REINSTALL)
    }
  }
  # Common postscript
  msg <- c(msg, SEE_README, REPORT_ISSUE)
  paste(msg, collapse="\n\n")
}

ALREADY_HAVE <- paste(
  "It appears you already have Arrow installed successfully:",
  "are you trying to install a different version of the library?"
)

SEE_DEV_GUIDE <- paste(
  "See the Arrow C++ developer guide",
  "<https://arrow.apache.org/docs/developers/cpp.html>",
  "for instructions on building the library from source."
)
# Variation of that
OR_SEE_DEV_GUIDE <- paste0(
  "Or, s",
  substr(SEE_DEV_GUIDE, 2, nchar(SEE_DEV_GUIDE))
)

SEE_ARROW_INSTALL <- paste(
  "See the Apache Arrow project installation page",
  "<https://arrow.apache.org/install/>",
  "for how to install the C++ package from a PPA."
)

THEN_REINSTALL <- paste(
  "After you've installed the C++ library,",
  "you'll need to reinstall the R package from source to find it."
)

SEE_README <- paste(
  "Refer to the R package README",
  "<https://github.com/apache/arrow/blob/master/r/README.md>",
  "for further details."
)

REPORT_ISSUE <- paste(
  "If you have other trouble, or if you think this message could be improved,",
  "please report an issue here:",
  "<https://issues.apache.org/jira/projects/ARROW/issues>"
)

INSTALL_FROM_CRAN <- paste(
  'Try installing the package from CRAN:',
  '`install.packages("arrow")`'
)

FIND_WIN_BINARY <- paste(
  "You may be able to download a development version of the C++ binary",
  "from the Apache Arrow project's Appveyor:",
  "<https://ci.appveyor.com/project/ApacheSoftwareFoundation/arrow>.",
  "Select an R job from a recent build,",
  'and download the `build\arrow-*.zip` file from the "Artifacts" tab.',
  "Then, set the RWINLIB_LOCAL environment variable to point to that file."
)

FIND_MAC_BINARY <- paste(
  "You may be able to get a development version of the Arrow C++ library",
  "using Homebrew: `brew install apache-arrow --HEAD`"
)
