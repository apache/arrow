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
init_env <- function() {
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

nse_funcs$coalesce <- function(...) {
  args <- list2(...)
  if (length(args) < 1) {
    abort("At least one argument must be supplied to coalesce()")
  }

  # Treat NaN like NA for consistency with dplyr::coalesce(), but if *all*
  # the values are NaN, we should return NaN, not NA, so don't replace
  # NaN with NA in the final (or only) argument
  # TODO: if an option is added to the coalesce kernel to treat NaN as NA,
  # use that to simplify the code here (ARROW-13389)
  attr(args[[length(args)]], "last") <- TRUE
  args <- lapply(args, function(arg) {
    last_arg <- is.null(attr(arg, "last"))
    attr(arg, "last") <- NULL

    if (!inherits(arg, "Expression")) {
      arg <- Expression$scalar(arg)
    }

    # coalesce doesn't yet support factors/dictionaries
    # TODO: remove this after ARROW-13390 is merged
    if (nse_funcs$is.factor(arg)) {
      warning("Dictionaries (in R: factors) are currently converted to strings (characters) in coalesce", call. = FALSE)
    }

    if (last_arg && arg$type_id() %in% TYPES_WITH_NAN) {
      # store the NA_real_ in the same type as arg to avoid avoid casting
      # smaller float types to larger float types
      NA_expr <- Expression$scalar(Scalar$create(NA_real_, type = arg$type()))
      Expression$create("if_else", Expression$create("is_nan", arg), NA_expr, arg)
    } else {
      arg
    }
  })
  Expression$create("coalesce", args = args)
}

nse_funcs$is.na <- function(x) {
  # TODO: if an option is added to the is_null kernel to treat NaN as NA,
  # use that to simplify the code here (ARROW-13367)
  if (is.double(x) || (inherits(x, "Expression") &&
      x$type_id() %in% TYPES_WITH_NAN)) {
    build_expr("is_nan", x) | build_expr("is_null", x)
  } else {
    build_expr("is_null", x)
  }
}

nse_funcs$is.nan <- function(x) {
  if (is.double(x) || (inherits(x, "Expression") &&
      x$type_id() %in% TYPES_WITH_NAN)) {
    # TODO: if an option is added to the is_nan kernel to treat NA as NaN,
    # use that to simplify the code here (ARROW-13366)
    build_expr("is_nan", x) & build_expr("is_valid", x)
  } else {
    Expression$scalar(FALSE)
  }
}

nse_funcs$is <- function(object, class2) {
  if (is.string(class2)) {
    switch(class2,
      # for R data types, pass off to is.*() functions
      character = nse_funcs$is.character(object),
      numeric = nse_funcs$is.numeric(object),
      integer = nse_funcs$is.integer(object),
      integer64 = nse_funcs$is.integer64(object),
      logical = nse_funcs$is.logical(object),
      factor = nse_funcs$is.factor(object),
      list = nse_funcs$is.list(object),
      # for Arrow data types, compare class2 with object$type()$ToString(),
      # but first strip off any parameters to only compare the top-level data
      # type,  and canonicalize class2
      sub("^([^([<]+).*$", "\\1", object$type()$ToString()) ==
        canonical_type_str(class2)
    )
  } else if (inherits(class2, "DataType")) {
    object$type() == as_type(class2)
  } else {
    stop("Second argument to is() is not a string or DataType", call. = FALSE)
  }
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

nse_funcs$is.finite <- function(x) {
  is_fin <- Expression$create("is_finite", x)
  # for compatibility with base::is.finite(), return FALSE for NA_real_
  is_fin & !nse_funcs$is.na(is_fin)
}

nse_funcs$is.infinite <- function(x) {
  is_inf <- Expression$create("is_inf", x)
  # for compatibility with base::is.infinite(), return FALSE for NA_real_
  is_inf & !nse_funcs$is.na(is_inf)
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

# is.* type functions
nse_funcs$is.character <- function(x) {
  is.character(x) || (inherits(x, "Expression") &&
    x$type_id() %in% Type[c("STRING", "LARGE_STRING")])
}
nse_funcs$is.numeric <- function(x) {
  is.numeric(x) || (inherits(x, "Expression") && x$type_id() %in% Type[c(
    "UINT8", "INT8", "UINT16", "INT16", "UINT32", "INT32",
    "UINT64", "INT64", "HALF_FLOAT", "FLOAT", "DOUBLE",
    "DECIMAL", "DECIMAL256"
  )])
}
nse_funcs$is.double <- function(x) {
  is.double(x) || (inherits(x, "Expression") && x$type_id() == Type["DOUBLE"])
}
nse_funcs$is.integer <- function(x) {
  is.integer(x) || (inherits(x, "Expression") && x$type_id() %in% Type[c(
    "UINT8", "INT8", "UINT16", "INT16", "UINT32", "INT32",
    "UINT64", "INT64"
  )])
}
nse_funcs$is.integer64 <- function(x) {
  is.integer64(x) || (inherits(x, "Expression") && x$type_id() == Type["INT64"])
}
nse_funcs$is.logical <- function(x) {
  is.logical(x) || (inherits(x, "Expression") && x$type_id() == Type["BOOL"])
}
nse_funcs$is.factor <- function(x) {
  is.factor(x) || (inherits(x, "Expression") && x$type_id() == Type["DICTIONARY"])
}
nse_funcs$is.list <- function(x) {
  is.list(x) || (inherits(x, "Expression") && x$type_id() %in% Type[c(
    "LIST", "FIXED_SIZE_LIST", "LARGE_LIST"
  )])
}

# rlang::is_* type functions
nse_funcs$is_character <- function(x, n = NULL) {
  assert_that(is.null(n))
  nse_funcs$is.character(x)
}
nse_funcs$is_double <- function(x, n = NULL, finite = NULL) {
  assert_that(is.null(n) && is.null(finite))
  nse_funcs$is.double(x)
}
nse_funcs$is_integer <- function(x, n = NULL) {
  assert_that(is.null(n))
  nse_funcs$is.integer(x)
}
nse_funcs$is_list <- function(x, n = NULL) {
  assert_that(is.null(n))
  nse_funcs$is.list(x)
}
nse_funcs$is_logical <- function(x, n = NULL) {
  assert_that(is.null(n))
  nse_funcs$is.logical(x)
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

nse_funcs$paste <- function(..., sep = " ", collapse = NULL, recycle0 = FALSE) {
  assert_that(
    is.null(collapse),
    msg = "paste() with the collapse argument is not yet supported in Arrow"
  )
  if (!inherits(sep, "Expression")) {
    assert_that(!is.na(sep), msg = "Invalid separator")
  }
  arrow_string_join_function(NullHandlingBehavior$REPLACE, "NA")(..., sep)
}

nse_funcs$paste0 <- function(..., collapse = NULL, recycle0 = FALSE) {
  assert_that(
    is.null(collapse),
    msg = "paste0() with the collapse argument is not yet supported in Arrow"
  )
  arrow_string_join_function(NullHandlingBehavior$REPLACE, "NA")(..., "")
}

nse_funcs$str_c <- function(..., sep = "", collapse = NULL) {
  assert_that(
    is.null(collapse),
    msg = "str_c() with the collapse argument is not yet supported in Arrow"
  )
  arrow_string_join_function(NullHandlingBehavior$EMIT_NULL)(..., sep)
}

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
        nse_funcs$as.character(arg)
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

nse_funcs$str_trim <- function(string, side = c("both", "left", "right")) {
  side <- match.arg(side)
  trim_fun <- switch(side,
    left = "utf8_ltrim_whitespace",
    right = "utf8_rtrim_whitespace",
    both = "utf8_trim_whitespace"
  )
  Expression$create(trim_fun, string)
}

nse_funcs$substr <- function(x, start, stop) {
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

  Expression$create(
    "utf8_slice_codeunits",
    x,
    # we don't need to subtract 1 from `stop` as C++ counts exclusively
    # which effectively cancels out the difference in indexing between R & C++
    options = list(start = start - 1L, stop = stop)
  )
}

nse_funcs$substring <- function(text, first, last){
  nse_funcs$substr(x = text, start = first, stop = last)
}

nse_funcs$str_sub <- function(string, start = 1L, end = -1L) {
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
}

nse_funcs$grepl <- function(pattern, x, ignore.case = FALSE, fixed = FALSE) {
  arrow_fun <- ifelse(fixed, "match_substring", "match_substring_regex")
  Expression$create(
    arrow_fun,
    x,
    options = list(pattern = pattern, ignore_case = ignore.case)
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

nse_funcs$str_like <- function(string, pattern, ignore_case = TRUE) {
  Expression$create(
    "match_like",
    string,
    options = list(pattern = pattern, ignore_case = ignore_case)
  )
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

  arrow_fun <- ifelse(fixed, "split_pattern", "split_pattern_regex")
  # warn when the user specifies both fixed = TRUE and perl = TRUE, for
  # consistency with the behavior of base::strsplit()
  if (fixed && perl) {
    warning("Argument 'perl = TRUE' will be ignored", call. = FALSE)
  }
  # since split is not a regex, proceed without any warnings or errors
  # regardless of the value of perl, for consistency with the behavior of
  # base::strsplit()
  Expression$create(
    arrow_fun,
    x,
    options = list(pattern = split, reverse = FALSE, max_splits = -1L)
  )
}

nse_funcs$str_split <- function(string, pattern, n = Inf, simplify = FALSE) {
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
}

nse_funcs$pmin <- function(..., na.rm = FALSE) {
  build_expr(
    "min_element_wise",
    ...,
    options = list(skip_nulls = na.rm)
  )
}

nse_funcs$pmax <- function(..., na.rm = FALSE) {
  build_expr(
    "max_element_wise",
    ...,
    options = list(skip_nulls = na.rm)
  )
}

nse_funcs$str_pad <- function(string, width, side = c("left", "right", "both"), pad = " ") {
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

nse_funcs$strptime <- function(x, format = "%Y-%m-%d %H:%M:%S", tz = NULL, unit = "ms") {
  # Arrow uses unit for time parsing, strptime() does not.
  # Arrow has no default option for strptime (format, unit),
  # we suggest following format = "%Y-%m-%d %H:%M:%S", unit = MILLI/1L/"ms",
  # (ARROW-12809)

  # ParseTimestampStrptime currently ignores the timezone information (ARROW-12820).
  # Stop if tz is provided.
  if (is.character(tz)) {
    arrow_not_supported("Time zone argument")
  }

  unit <- make_valid_time_unit(unit, c(valid_time64_units, valid_time32_units))

  Expression$create("strptime", x, options = list(format = format, unit = unit))
}

nse_funcs$second <- function(x) {
  Expression$create("add", Expression$create("second", x), Expression$create("subsecond", x))
}

nse_funcs$trunc <- function(x, ...) {
  # accepts and ignores ... for consistency with base::trunc()
  build_expr("trunc", x)
}

nse_funcs$wday <- function(x, label = FALSE, abbr = TRUE, week_start = getOption("lubridate.week.start", 7)) {

  # The "day_of_week" compute function returns numeric days of week and not locale-aware strftime
  # When the ticket below is resolved, we should be able to support the label argument
  # https://issues.apache.org/jira/browse/ARROW-13133
  if (label) {
    arrow_not_supported("Label argument")
  }

  Expression$create("day_of_week", x, options = list(one_based_numbering = TRUE, week_start = week_start))
}

nse_funcs$log <- nse_funcs$logb <- function(x, base = exp(1)) {

  if (base == exp(1)) {
    return(Expression$create("ln_checked", x))
  }

  if (base == 2) {
    return(Expression$create("log2_checked", x))
  }

  if (base == 10) {
    return(Expression$create("log10_checked", x))
  }
  # ARROW-13345
  stop("`base` values other than exp(1), 2 and 10 not supported in Arrow", call. = FALSE)
}

nse_funcs$if_else <- function(condition, true, false, missing = NULL){
  if (!is.null(missing)) {
    return(nse_funcs$if_else(
      nse_funcs$is.na(condition),
      missing,
      nse_funcs$if_else(condition, true, false)
    ))
  }

  # if_else doesn't yet support factors/dictionaries
  # TODO: remove this after ARROW-13358 is merged
  warn_types <- nse_funcs$is.factor(true) | nse_funcs$is.factor(false)
  if (warn_types) {
    warning("Dictionaries (in R: factors) are currently converted to strings (characters) in if_else and ifelse", call. = FALSE)
  }

  build_expr("if_else", condition, true, false)
}

# Although base R ifelse allows `yes` and `no` to be different classes
#
nse_funcs$ifelse <- function(test, yes, no) {
  nse_funcs$if_else(condition = test, true = yes, false = no)
}

nse_funcs$case_when <- function(...) {
  formulas <- list2(...)
  n <- length(formulas)
  if (n == 0) {
    abort("No cases provided in case_when()")
  }
  query <- vector("list", n)
  value <- vector("list", n)
  mask <- caller_env()
  for (i in seq_len(n)) {
    f <- formulas[[i]]
    if (!inherits(f, "formula")) {
      abort("Each argument to case_when() must be a two-sided formula")
    }
    query[[i]] <- arrow_eval(f[[2]], mask)
    value[[i]] <- arrow_eval(f[[3]], mask)
    if (!nse_funcs$is.logical(query[[i]])) {
      abort("Left side of each formula in case_when() must be a logical expression")
    }
  }
  build_expr(
    "case_when",
    args = c(
      build_expr(
        "make_struct",
        args = query,
        options = list(field_names = as.character(seq_along(query)))
      ),
      value
    )
  )
}
