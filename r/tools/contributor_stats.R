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
# The `print_new_contributors()` helper below is copied from Bryce Mecum's gist:
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

#' new_contributors.R
#'
#' Produce a list of names of new contributors between two git refs. The method
#' this uses is to first get the list of unique contrbutors referancable from
#' the first ref, then the second ref, and then compute the set difference and
#' return that.
#'
#' Usage
#'
#'   Launch an R session from the directory containing the git repo you want to
#'   query against.
#'
#'   Run this to get a list of new contributors between the refs
#'   "apache-arrow-13.0.0" and "apache-arrow-14.0.0" for commits that touched
#'   the "r" subdirectory":
#
#'   print_new_contributors(
#'     "apache-arrow-13.0.0",
#'     "apache-arrow-14.0.0",
#'     "r"
#'   )
#'
#'   Note that the third argument, subdirectory, is optional. If omitted, it
#'   will use the root directory.

stopunlesscommand <- function(command, arguments) {
  out <- tryCatch(
    {
      system2("git", arguments, stdout = TRUE)
    },
    warning = function(w) {
      stop(w)
    },
    error = function(e) {
      stop(e)
    }
  )

  TRUE
}

stopfinotinrepo <- function() {
  stopunlesscommand("git", "reflog main")
}

stopifnotref <- function(ref) {
  stopifnot(is.character(ref))
  stopifnot(nchar(ref) > 0)
  stopunlesscommand("git", paste("reflog", ref))
}

make_git_log_prev_args <- function(ref_from, subdirectory) {
  paste0("log --pretty='format: %an' ", ref_from, " ", subdirectory, " | sort | uniq")
}

make_git_log_next_args <- function(ref_from, ref_to, subdirectory) {
  paste0("log --pretty='format: %an' ", ref_from, "..", ref_to, " ", subdirectory, " | sort | uniq")
}

print_new_contributors <- function(ref_from, ref_to, subdirectory = ".") {
  stopfinotinrepo()
  stopifnotref(ref_from)
  stopifnotref(ref_to)
  stopifnot(file.exists(subdirectory))

  prev_out <- trimws(system2("git", make_git_log_prev_args(ref_from, subdirectory), stdout = TRUE))
  new_out <- trimws(system2("git", make_git_log_next_args(ref_from, ref_to, subdirectory), stdout = TRUE))

  setdiff(new_out, prev_out)
}

# Contributors (author names) to `subdirectory` between two refs
contributors_between <- function(ref_from, ref_to, subdirectory = ".") {
  trimws(system2("git", make_git_log_next_args(ref_from, ref_to, subdirectory), stdout = TRUE))
}

# Summary stats for the release announcement: total contributors, and how
# many touched only C++, only R, or both, plus first-time contributors.
release_contributor_stats <- function(ref_from, ref_to) {
  stopfinotinrepo()
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
