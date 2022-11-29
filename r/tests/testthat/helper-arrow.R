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

set.seed(1)

MAX_INT <- 2147483647L

# Make sure this is unset
Sys.setenv(ARROW_PRE_0_15_IPC_FORMAT = "")

# use the C locale for string collation (ARROW-12046)
Sys.setlocale("LC_COLLATE", "C")

# Set English language so that error messages aren't internationalized
# (R CMD check does this, but in case you're running outside of check)
Sys.setenv(LANGUAGE = "en")

# Set this option so that the deprecation warning isn't shown
# (except when we test for it)
options(arrow.pull_as_vector = FALSE)

with_language <- function(lang, expr) {
  old <- Sys.getenv("LANGUAGE")
  # Check what this message is before changing languages; this will
  # trigger caching the transations if the OS does that (some do).
  # If the OS does cache, then we can't test changing languages safely.
  before <- i18ize_error_messages()
  Sys.setenv(LANGUAGE = lang)
  on.exit({
    Sys.setenv(LANGUAGE = old)
    .cache$i18ized_error_pattern <<- NULL
  })
  if (!identical(before, i18ize_error_messages())) {
    skip(paste("This OS either does not support changing languages to", lang, "or it caches translations"))
  }
  force(expr)
}

# backport of 4.0.0 implementation
if (getRversion() < "4.0.0") {
  suppressWarnings <- function(expr, classes = "warning") {
    withCallingHandlers(
      expr,
      warning = function(w) {
        if (inherits(w, classes)) {
          invokeRestart("muffleWarning")
        }
      }
    )
  }
}

make_temp_dir <- function() {
  path <- tempfile()
  dir.create(path)
  normalizePath(path, winslash = "/")
}
