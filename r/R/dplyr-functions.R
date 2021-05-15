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


#' @include expression.R
NULL

# This environment is an internal cache for things including data mask functions
# We'll populate it at package load time.
.cache <- NULL
init_env <- function () {
  .cache <<- new.env(hash = TRUE)
}
init_env()

# nse_funcs is a list of functions that operated on (and return) Expressions
# These will be the basis for a data_mask inside dplyr methods
# and will be added to .cache at package load time

# Start with mappings from R function name spellings
nse_funcs <- lapply(set_names(names(.array_function_map)), function(operator) {
  force(operator)
  function(...) build_expr(operator, ...)
})

# Now add functions to that list where the mapping from R to Arrow isn't 1:1
# Each of these functions should have the same signature as the R function
# they're replacing.
#
# When to use `build_expr()` vs. `Expression$create()`?
#
# Use `build_expr()` if you need to
# (1) map R function names to Arrow C++ functions
# (2) wrap R inputs (vectors) as Array/Scalar
#
# `Expression$create()` is lower level. Most of the functions below use it
# because they manage the preparation of the user-provided inputs
# and don't need to wrap scalars

nse_funcs$cast <- function(x, target_type, safe = TRUE, ...) {
  opts <- cast_options(safe, ...)
  opts$to_type <- as_type(target_type)
  Expression$create("cast", x, options = opts)
}

nse_funcs$dictionary_encode <- function(x,
                                        null_encoding_behavior = c("mask", "encode")) {
  behavior <- toupper(match.arg(null_encoding_behavior))
  null_encoding_behavior <- NullEncodingBehavior[[behavior]]
  Expression$create(
    "dictionary_encode",
    x,
    options = list(null_encoding_behavior = null_encoding_behavior)
  )
}

nse_funcs$between <- function(x, left, right) {
  x >= left & x <= right
}

# as.* type casting functions
# as.factor() is mapped in expression.R
nse_funcs$as.character <- function(x) {
  Expression$create("cast", x, options = cast_options(to_type = string()))
}
nse_funcs$as.double <- function(x) {
  Expression$create("cast", x, options = cast_options(to_type = float64()))
}
nse_funcs$as.integer <- function(x) {
  Expression$create(
    "cast",
    x,
    options = cast_options(
      to_type = int32(),
      allow_float_truncate = TRUE,
      allow_decimal_truncate = TRUE
    )
  )
}
nse_funcs$as.integer64 <- function(x) {
  Expression$create(
    "cast",
    x,
    options = cast_options(
      to_type = int64(),
      allow_float_truncate = TRUE,
      allow_decimal_truncate = TRUE
    )
  )
}
nse_funcs$as.logical <- function(x) {
  Expression$create("cast", x, options = cast_options(to_type = boolean()))
}
nse_funcs$as.numeric <- function(x) {
  Expression$create("cast", x, options = cast_options(to_type = float64()))
}

# String functions
nse_funcs$nchar <- function(x, type = "chars", allowNA = FALSE, keepNA = NA) {
  if (allowNA) {
    arrow_not_supported("allowNA = TRUE")
  }
  if (is.na(keepNA)) {
    keepNA <- !identical(type, "width")
  }
  if (!keepNA) {
    # TODO: I think there is a fill_null kernel we could use, set null to 2
    arrow_not_supported("keepNA = TRUE")
  }
  if (identical(type, "bytes")) {
    Expression$create("binary_length", x)
  } else {
    Expression$create("utf8_length", x)
  }
}

nse_funcs$str_trim <- function(string, side = c("both", "left", "right")) {
  side <- match.arg(side)
  trim_fun <- switch(side,
    left = "utf8_ltrim_whitespace",
    right = "utf8_rtrim_whitespace",
    both = "utf8_trim_whitespace"
  )
  Expression$create(trim_fun, string)
}

nse_funcs$grepl <- function(pattern, x, ignore.case = FALSE, fixed = FALSE) {
  arrow_fun <- ifelse(fixed && !ignore.case, "match_substring", "match_substring_regex")
  Expression$create(
    arrow_fun,
    x,
    options = list(pattern = format_string_pattern(pattern, ignore.case, fixed))
  )
}

nse_funcs$str_detect <- function(string, pattern, negate = FALSE) {
  opts <- get_stringr_pattern_options(enexpr(pattern))
  out <- nse_funcs$grepl(
    pattern = opts$pattern,
    x = string,
    ignore.case = opts$ignore_case,
    fixed = opts$fixed
  )
  if (negate) {
    out <- !out
  }
  out
}

# Encapsulate some common logic for sub/gsub/str_replace/str_replace_all
arrow_r_string_replace_function <- function(max_replacements) {
  function(pattern, replacement, x, ignore.case = FALSE, fixed = FALSE) {
    Expression$create(
      ifelse(fixed && !ignore.case, "replace_substring", "replace_substring_regex"),
      x,
      options = list(
        pattern = format_string_pattern(pattern, ignore.case, fixed),
        replacement = format_string_replacement(replacement, ignore.case, fixed),
        max_replacements = max_replacements
      )
    )
  }
}

arrow_stringr_string_replace_function <- function(max_replacements) {
  function(string, pattern, replacement) {
    opts <- get_stringr_pattern_options(enexpr(pattern))
    arrow_r_string_replace_function(max_replacements)(
      pattern = opts$pattern,
      replacement = replacement,
      x = string,
      ignore.case = opts$ignore_case,
      fixed = opts$fixed
    )
  }
}

nse_funcs$sub <- arrow_r_string_replace_function(1L)
nse_funcs$gsub <- arrow_r_string_replace_function(-1L)
nse_funcs$str_replace <- arrow_stringr_string_replace_function(1L)
nse_funcs$str_replace_all <- arrow_stringr_string_replace_function(-1L)

nse_funcs$strsplit <- function(x,
                               split,
                               fixed = FALSE,
                               perl = FALSE,
                               useBytes = FALSE) {
  assert_that(is.string(split))

  # The Arrow C++ library does not support splitting a string by a regular
  # expression pattern (ARROW-12608) but the default behavior of
  # base::strsplit() is to interpret the split pattern as a regex
  # (fixed = FALSE). R users commonly pass non-regex split patterns to
  # strsplit() without bothering to set fixed = TRUE. It would be annoying if
  # that didn't work here. So: if fixed = FALSE, let's check the split pattern
  # to see if it is a regex (if it contains any regex metacharacters). If not,
  # then allow to proceed.
  if (!fixed && contains_regex(split)) {
    arrow_not_supported("Regular expression matching in strsplit()")
  }
  # warn when the user specifies both fixed = TRUE and perl = TRUE, for
  # consistency with the behavior of base::strsplit()
  if (fixed && perl) {
    warning("Argument 'perl = TRUE' will be ignored", call. = FALSE)
  }
  # since split is not a regex, proceed without any warnings or errors
  # regardless of the value of perl, for consistency with the behavior of
  # base::strsplit()
  Expression$create(
    "split_pattern",
    x,
    options = list(pattern = split, reverse = FALSE, max_splits = -1L)
  )
}

nse_funcs$str_split <- function(string, pattern, n = Inf, simplify = FALSE) {
  opts <- get_stringr_pattern_options(enexpr(pattern))
  if (!opts$fixed && contains_regex(opts$pattern)) {
    arrow_not_supported("Regular expression matching in str_split()")
  }
  if (opts$ignore_case) {
    arrow_not_supported("Case-insensitive string splitting")
  }
  if (n == 0) {
    arrow_not_supported("Splitting strings into zero parts")
  }
  if (identical(n, Inf)) {
    n <- 0L
  }
  if (simplify) {
    warning("Argument 'simplify = TRUE' will be ignored", call. = FALSE)
  }
  # The max_splits option in the Arrow C++ library controls the maximum number
  # of places at which the string is split, whereas the argument n to
  # str_split() controls the maximum number of pieces to return. So we must
  # subtract 1 from n to get max_splits.
  Expression$create(
    "split_pattern",
    string,
    options = list(
      pattern =
      opts$pattern,
      reverse = FALSE,
      max_splits = n - 1L
    )
  )
}

# String function helpers

# format `pattern` as needed for case insensitivity and literal matching by RE2
format_string_pattern <- function(pattern, ignore.case, fixed) {
  # Arrow lacks native support for case-insensitive literal string matching and
  # replacement, so we use the regular expression engine (RE2) to do this.
  # https://github.com/google/re2/wiki/Syntax
  if (ignore.case) {
    if (fixed) {
      # Everything between "\Q" and "\E" is treated as literal text.
      # If the search text contains any literal "\E" strings, make them
      # lowercase so they won't signal the end of the literal text:
      pattern <- gsub("\\E", "\\e", pattern, fixed = TRUE)
      pattern <- paste0("\\Q", pattern, "\\E")
    }
    # Prepend "(?i)" for case-insensitive matching
    pattern <- paste0("(?i)", pattern)
  }
  pattern
}

# format `replacement` as needed for literal replacement by RE2
format_string_replacement <- function(replacement, ignore.case, fixed) {
  # Arrow lacks native support for case-insensitive literal string
  # replacement, so we use the regular expression engine (RE2) to do this.
  # https://github.com/google/re2/wiki/Syntax
  if (ignore.case && fixed) {
    # Escape single backslashes in the regex replacement text so they are
    # interpreted as literal backslashes:
    replacement <- gsub("\\", "\\\\", replacement, fixed = TRUE)
  }
  replacement
}

#' Get `stringr` pattern options
#'
#' This function assigns definitions for the `stringr` pattern modifier
#' functions (`fixed()`, `regex()`, etc.) inside itself, and uses them to
#' evaluate the quoted expression `pattern`, returning a list that is used
#' to control pattern matching behavior in internal `arrow` functions.
#'
#' @param pattern Unevaluated expression containing a call to a `stringr`
#' pattern modifier function
#'
#' @return List containing elements `pattern`, `fixed`, and `ignore_case`
#' @keywords internal
get_stringr_pattern_options <- function(pattern) {
  fixed <- function(pattern, ignore_case = FALSE, ...) {
    check_dots(...)
    list(pattern = pattern, fixed = TRUE, ignore_case = ignore_case)
  }
  regex <- function(pattern, ignore_case = FALSE, ...) {
    check_dots(...)
    list(pattern = pattern, fixed = FALSE, ignore_case = ignore_case)
  }
  coll <- function(...) {
    arrow_not_supported("Pattern modifier `coll()`")
  }
  boundary <- function(...) {
    arrow_not_supported("Pattern modifier `boundary()`")
  }
  check_dots <- function(...) {
    dots <- list(...)
    if (length(dots)) {
      warning(
        "Ignoring pattern modifier ",
        ngettext(length(dots), "argument ", "arguments "),
        "not supported in Arrow: ",
        oxford_paste(names(dots)),
        call. = FALSE
      )
    }
  }
  ensure_opts <- function(opts) {
    if (is.character(opts)) {
      opts <- list(pattern = opts, fixed = FALSE, ignore_case = FALSE)
    }
    opts
  }
  ensure_opts(eval(pattern))
}

#' Does this string contain regex metacharacters?
#'
#' @param string String to be tested
#' @keywords internal
#' @return Logical: does `string` contain regex metacharacters?
contains_regex <- function(string) {
  grepl("[.\\|()[{^$*+?]", string)
}
