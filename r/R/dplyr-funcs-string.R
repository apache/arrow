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

# String function helpers

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

# Currently, Arrow does not supports a locale option for string case conversion
# functions, contrast to stringr's API, so the 'locale' argument is only valid
# for stringr's default value ("en"). The following are string functions that
# take a 'locale' option as its second argument:
#   str_to_lower
#   str_to_upper
#   str_to_title
#
# Arrow locale will be supported with ARROW-14126
stop_if_locale_provided <- function(locale) {
  if (!identical(locale, "en")) {
    stop("Providing a value for 'locale' other than the default ('en') is not supported in Arrow. ",
      "To change locale, use 'Sys.setlocale()'",
      call. = FALSE
    )
  }
}


# Split up into several register functions by category to satisfy the linter
register_bindings_string <- function() {
  register_bindings_string_join()
  register_bindings_string_regex()
  register_bindings_string_other()
}

register_bindings_string_join <- function() {
  arrow_string_join_function <- function(null_handling, null_replacement = NULL) {
    # the `binary_join_element_wise` Arrow C++ compute kernel takes the separator
    # as the last argument, so pass `sep` as the last dots arg to this function
    function(...) {
      args <- lapply(list(...), function(arg) {
        # handle scalar literal args, and cast all args to string for
        # consistency with base::paste(), base::paste0(), and stringr::str_c()
        if (!inherits(arg, "Expression")) {
          assert_that(
            length(arg) == 1,
            msg = "Literal vectors of length != 1 not supported in string concatenation"
          )
          Expression$scalar(as.character(arg))
        } else {
          call_binding("as.character", arg)
        }
      })
      Expression$create(
        "binary_join_element_wise",
        args = args,
        options = list(
          null_handling = null_handling,
          null_replacement = null_replacement
        )
      )
    }
  }

  register_binding(
    "base::paste",
    function(..., sep = " ", collapse = NULL, recycle0 = FALSE) {
      assert_that(
        is.null(collapse),
        msg = "paste() with the collapse argument is not yet supported in Arrow"
      )
      if (!inherits(sep, "Expression")) {
        assert_that(!is.na(sep), msg = "Invalid separator")
      }
      arrow_string_join_function(NullHandlingBehavior$REPLACE, "NA")(..., sep)
    },
    notes = "the `collapse` argument is not yet supported"
  )

  register_binding(
    "base::paste0",
    function(..., collapse = NULL, recycle0 = FALSE) {
      assert_that(
        is.null(collapse),
        msg = "paste0() with the collapse argument is not yet supported in Arrow"
      )
      arrow_string_join_function(NullHandlingBehavior$REPLACE, "NA")(..., "")
    },
    notes = "the `collapse` argument is not yet supported"
  )

  register_binding(
    "stringr::str_c",
    function(..., sep = "", collapse = NULL) {
      assert_that(
        is.null(collapse),
        msg = "str_c() with the collapse argument is not yet supported in Arrow"
      )
      if (!inherits(sep, "Expression")) {
        assert_that(!is.na(sep), msg = "`sep` must be a single string, not `NA`.")
      }
      arrow_string_join_function(NullHandlingBehavior$EMIT_NULL)(..., sep)
    },
    notes = "the `collapse` argument is not yet supported"
  )

  register_binding("base::strrep", function(x, times) {
    Expression$create("binary_repeat", x, times)
  })

  register_binding("stringr::str_dup", function(string, times) {
    Expression$create("binary_repeat", string, times)
  })
}

register_bindings_string_regex <- function() {
  create_string_match_expr <- function(arrow_fun, string, pattern, ignore_case) {
    out <- Expression$create(
      arrow_fun,
      string,
      options = list(pattern = pattern, ignore_case = ignore_case)
    )
  }

  register_binding("base::grepl", function(pattern,
                                           x,
                                           ignore.case = FALSE,
                                           fixed = FALSE) {
    arrow_fun <- ifelse(fixed, "match_substring", "match_substring_regex")
    out <- create_string_match_expr(
      arrow_fun,
      string = x,
      pattern = pattern,
      ignore_case = ignore.case
    )
    call_binding("if_else", call_binding("is.na", out), FALSE, out)
  })


  register_binding("stringr::str_detect", function(string, pattern, negate = FALSE) {
    opts <- get_stringr_pattern_options(enexpr(pattern))
    arrow_fun <- ifelse(opts$fixed, "match_substring", "match_substring_regex")
    out <- create_string_match_expr(arrow_fun,
      string = string,
      pattern = opts$pattern,
      ignore_case = opts$ignore_case
    )
    if (negate) {
      out <- !out
    }
    out
  })

  register_binding(
    "stringr::str_like",
    function(string, pattern, ignore_case = TRUE) {
      Expression$create(
        "match_like",
        string,
        options = list(pattern = pattern, ignore_case = ignore_case)
      )
    }
  )

  register_binding(
    "stringr::str_count",
    function(string, pattern) {
      opts <- get_stringr_pattern_options(enexpr(pattern))
      if (!is.string(pattern)) {
        arrow_not_supported("`pattern` must be a length 1 character vector; other values")
      }
      arrow_fun <- ifelse(opts$fixed, "count_substring", "count_substring_regex")
      Expression$create(
        arrow_fun,
        string,
        options = list(pattern = opts$pattern, ignore_case = opts$ignore_case)
      )
    },
    notes = "`pattern` must be a length 1 character vector"
  )

  register_binding("base::startsWith", function(x, prefix) {
    Expression$create(
      "starts_with",
      x,
      options = list(pattern = prefix)
    )
  })

  register_binding("base::endsWith", function(x, suffix) {
    Expression$create(
      "ends_with",
      x,
      options = list(pattern = suffix)
    )
  })

  register_binding("stringr::str_starts", function(string, pattern, negate = FALSE) {
    opts <- get_stringr_pattern_options(enexpr(pattern))
    if (opts$fixed) {
      out <- call_binding("startsWith", x = string, prefix = opts$pattern)
    } else {
      out <- create_string_match_expr(
        arrow_fun = "match_substring_regex",
        string = string,
        pattern = paste0("^", opts$pattern),
        ignore_case = opts$ignore_case
      )
    }
    if (negate) {
      out <- !out
    }
    out
  })

  register_binding("stringr::str_ends", function(string, pattern, negate = FALSE) {
    opts <- get_stringr_pattern_options(enexpr(pattern))
    if (opts$fixed) {
      out <- call_binding("endsWith", x = string, suffix = opts$pattern)
    } else {
      out <- create_string_match_expr(
        arrow_fun = "match_substring_regex",
        string = string,
        pattern = paste0(opts$pattern, "$"),
        ignore_case = opts$ignore_case
      )
    }
    if (negate) {
      out <- !out
    }
    out
  })

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
    force(max_replacements)
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

  arrow_stringr_string_remove_function <- function(max_replacements) {
    force(max_replacements)
    function(string, pattern) {
      opts <- get_stringr_pattern_options(enexpr(pattern))
      arrow_r_string_replace_function(max_replacements)(
        pattern = opts$pattern,
        replacement = "",
        x = string,
        ignore.case = opts$ignore_case,
        fixed = opts$fixed
      )
    }
  }

  register_binding("base::sub", arrow_r_string_replace_function(1L))
  register_binding("base::gsub", arrow_r_string_replace_function(-1L))
  register_binding("stringr::str_replace", arrow_stringr_string_replace_function(1L))
  register_binding("stringr::str_replace_all", arrow_stringr_string_replace_function(-1L))
  register_binding("stringr::str_remove", arrow_stringr_string_remove_function(1L))
  register_binding("stringr::str_remove_all", arrow_stringr_string_remove_function(-1L))

  register_binding("base::strsplit", function(x, split, fixed = FALSE, perl = FALSE,
                                              useBytes = FALSE) {
    assert_that(is.string(split))

    arrow_fun <- ifelse(fixed, "split_pattern", "split_pattern_regex")
    # warn when the user specifies both fixed = TRUE and perl = TRUE, for
    # consistency with the behavior of base::strsplit()
    if (fixed && perl) {
      warning("Argument 'perl = TRUE' will be ignored", call. = FALSE)
    }
    # since split is not a regex, proceed without any warnings or errors regardless
    # of the value of perl, for consistency with the behavior of base::strsplit()
    Expression$create(
      arrow_fun,
      x,
      options = list(pattern = split, reverse = FALSE, max_splits = -1L)
    )
  })

  register_binding(
    "stringr::str_split",
    function(string,
             pattern,
             n = Inf,
             simplify = FALSE) {
      opts <- get_stringr_pattern_options(enexpr(pattern))
      arrow_fun <- ifelse(opts$fixed, "split_pattern", "split_pattern_regex")
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
        arrow_fun,
        string,
        options = list(
          pattern = opts$pattern,
          reverse = FALSE,
          max_splits = n - 1L
        )
      )
    },
    notes = "Case-insensitive string splitting and splitting into 0 parts not supported"
  )
}

register_bindings_string_other <- function() {
  register_binding(
    "base::nchar",
    function(x, type = "chars", allowNA = FALSE, keepNA = NA) {
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
    },
    notes = "`allowNA = TRUE` and `keepNA = TRUE` not supported"
  )

  register_binding("stringr::str_to_lower", function(string, locale = "en") {
    stop_if_locale_provided(locale)
    Expression$create("utf8_lower", string)
  })

  register_binding("stringr::str_to_upper", function(string, locale = "en") {
    stop_if_locale_provided(locale)
    Expression$create("utf8_upper", string)
  })

  register_binding("stringr::str_to_title", function(string, locale = "en") {
    stop_if_locale_provided(locale)
    Expression$create("utf8_title", string)
  })

  register_binding("stringr::str_trim", function(string, side = c("both", "left", "right")) {
    side <- match.arg(side)
    trim_fun <- switch(side,
      left = "utf8_ltrim_whitespace",
      right = "utf8_rtrim_whitespace",
      both = "utf8_trim_whitespace"
    )
    Expression$create(trim_fun, string)
  })

  register_binding(
    "base::substr",
    function(x, start, stop) {
      assert_that(
        length(start) == 1,
        msg = "`start` must be length 1 - other lengths are not supported in Arrow"
      )
      assert_that(
        length(stop) == 1,
        msg = "`stop` must be length 1 - other lengths are not supported in Arrow"
      )

      # substr treats values as if they're on a continous number line, so values
      # 0 are effectively blank characters - set `start` to 1 here so Arrow mimics
      # this behavior
      if (start <= 0) {
        start <- 1
      }

      # if `stop` is lower than `start`, this is invalid, so set `stop` to
      # 0 so that an empty string will be returned (consistent with base::substr())
      if (stop < start) {
        stop <- 0
      }

      # if the input is a string we use "utf8_slice_codeunits"; if the
      # input is binary we use "binary_slice". This does not consider
      # a binary Scalar.
      x_is_binary <- inherits(x, "ArrowObject") &&
        x$type_id() %in% c(Type$BINARY, Type$LARGE_BINARY, Type$FIXED_SIZE_BINARY)
      if (x_is_binary) {
        fun <- "binary_slice"
      } else {
        fun <- "utf8_slice_codeunits"
      }

      Expression$create(
        fun,
        x,
        # we don't need to subtract 1 from `stop` as C++ counts exclusively
        # which effectively cancels out the difference in indexing between R & C++
        options = list(start = start - 1L, stop = stop)
      )
    },
    notes = "`start` and `stop` must be length 1"
  )

  register_binding("base::substring", function(text, first, last) {
    call_binding("substr", x = text, start = first, stop = last)
  })

  register_binding("stringr::str_sub", function(string, start = 1L, end = -1L) {
    assert_that(
      length(start) == 1,
      msg = "`start` must be length 1 - other lengths are not supported in Arrow"
    )
    assert_that(
      length(end) == 1,
      msg = "`end` must be length 1 - other lengths are not supported in Arrow"
    )

    # In stringr::str_sub, an `end` value of -1 means the end of the string, so
    # set it to the maximum integer to match this behavior
    if (end == -1) {
      end <- .Machine$integer.max
    }

    # An end value lower than a start value returns an empty string in
    # stringr::str_sub so set end to 0 here to match this behavior
    if (end < start) {
      end <- 0
    }

    # subtract 1 from `start` because C++ is 0-based and R is 1-based
    # str_sub treats a `start` value of 0 or 1 as the same thing so don't subtract 1 when `start` == 0
    # when `start` < 0, both str_sub and utf8_slice_codeunits count backwards from the end
    if (start > 0) {
      start <- start - 1L
    }

    Expression$create(
      "utf8_slice_codeunits",
      string,
      options = list(start = start, stop = end)
    )
  },
  notes = "`start` and `end` must be length 1"
  )


  register_binding("stringr::str_pad", function(string,
                                                width,
                                                side = c("left", "right", "both"),
                                                pad = " ") {
    assert_that(is_integerish(width))
    side <- match.arg(side)
    assert_that(is.string(pad))

    if (side == "left") {
      pad_func <- "utf8_lpad"
    } else if (side == "right") {
      pad_func <- "utf8_rpad"
    } else if (side == "both") {
      pad_func <- "utf8_center"
    }

    Expression$create(
      pad_func,
      string,
      options = list(width = width, padding = pad)
    )
  })
}
