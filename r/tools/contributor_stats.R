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

# Contributor statistics for release announcements.
#
# The `print_new_contributors()` helper below is adapted from Bryce Mecum's gist:
# https://gist.github.com/amoeba/4e26c064d1a0d0227cd8c2260cf0072a
#
# Usage: launch R from the root of the arrow git repo, then:
#
#   source("r/tools/contributor_stats.R")
#   release_contributor_stats("apache-arrow-20.0.0", "apache-arrow-21.0.0")
#
# or, for just the list of first-time contributors to a subdirectory:
#
#   print_new_contributors("apache-arrow-20.0.0", "apache-arrow-21.0.0", "r")

# Run a git command, erroring if it fails. Returns stdout as a character vector.
git <- function(...) {
  out <- suppressWarnings(system2("git", c(...), stdout = TRUE, stderr = TRUE))
  status <- attr(out, "status")
  if (!is.null(status) && status != 0) {
    stop("git ", paste(c(...), collapse = " "), " failed:\n", paste(out, collapse = "\n"))
  }
  as.character(out)
}

stopifnotinrepo <- function() {
  git("rev-parse", "--is-inside-work-tree")
  invisible(TRUE)
}

stopifnotref <- function(ref) {
  stopifnot(is.character(ref), length(ref) == 1, nchar(ref) > 0)
  status <- system2(
    "git",
    c("rev-parse", "--verify", "--quiet", paste0(ref, "^{commit}")),
    stdout = FALSE,
    stderr = FALSE
  )
  if (status != 0) {
    stop(
      "'",
      ref,
      "' is not a git ref in this repo. Release tags look like ",
      "'apache-arrow-",
      sub("^apache-arrow-", "", ref),
      "'."
    )
  }
  invisible(TRUE)
}

# Unique author names for commits in `revision_range` that touched `subdirectory`
git_authors <- function(revision_range, subdirectory) {
  sort(unique(trimws(git("log", "--pretty=format:%an", revision_range, "--", subdirectory))))
}

# Contributors to `subdirectory` between `ref_from` and `ref_to` with no
# commits reachable from `ref_from`, i.e. first-time contributors in that range.
print_new_contributors <- function(ref_from, ref_to, subdirectory = ".") {
  stopifnotinrepo()
  stopifnotref(ref_from)
  stopifnotref(ref_to)
  stopifnot(file.exists(subdirectory))

  prev_out <- git_authors(ref_from, subdirectory)
  new_out <- git_authors(paste0(ref_from, "..", ref_to), subdirectory)

  setdiff(new_out, prev_out)
}

# Contributors (author names) to `subdirectory` between two refs
contributors_between <- function(ref_from, ref_to, subdirectory = ".") {
  git_authors(paste0(ref_from, "..", ref_to), subdirectory)
}

# Summary stats for the release announcement: total contributors, and how
# many touched only C++, only R, or both, plus first-time contributors.
release_contributor_stats <- function(ref_from, ref_to) {
  stopifnotinrepo()
  stopifnotref(ref_from)
  stopifnotref(ref_to)

  all_contribs <- contributors_between(ref_from, ref_to)
  cpp_contribs <- contributors_between(ref_from, ref_to, "cpp")
  r_contribs <- contributors_between(ref_from, ref_to, "r")

  list(
    total = length(all_contribs),
    cpp_only = length(setdiff(cpp_contribs, r_contribs)),
    r_only = length(setdiff(r_contribs, cpp_contribs)),
    both = length(intersect(cpp_contribs, r_contribs)),
    first_timers = print_new_contributors(ref_from, ref_to),
    first_timers_r = print_new_contributors(ref_from, ref_to, "r")
  )
}
