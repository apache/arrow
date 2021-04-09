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
#' @include record-batch.R
#' @include table.R

arrow_dplyr_query <- function(.data) {
  # An arrow_dplyr_query is a container for an Arrow data object (Table,
  # RecordBatch, or Dataset) and the state of the user's dplyr query--things
  # like selected columns, filters, and group vars.

  # For most dplyr methods,
  # method.Table == method.RecordBatch == method.Dataset == method.arrow_dplyr_query
  # This works because the functions all pass .data through arrow_dplyr_query()
  if (inherits(.data, "arrow_dplyr_query")) {
    return(.data)
  }
  structure(
    list(
      .data = .data$clone(),
      # selected_columns is a named list:
      # * contents are references/expressions pointing to the data
      # * names are the names they should be in the end (i.e. this
      #   records any renaming)
      selected_columns = make_field_refs(names(.data), dataset = inherits(.data, "Dataset")),
      # filtered_rows will be an Expression
      filtered_rows = TRUE,
      # group_by_vars is a character vector of columns (as renamed)
      # in the data. They will be kept when data is pulled into R.
      group_by_vars = character(),
      # drop_empty_groups is a logical value indicating whether to drop
      # groups formed by factor levels that don't appear in the data. It
      # should be non-null only when the data is grouped.
      drop_empty_groups = NULL,
      # arrange_vars will be a list of expressions named by their associated
      # column names
      arrange_vars = list(),
      # arrange_desc will be a logical vector indicating the sort order for each
      # expression in arrange_vars (FALSE for ascending, TRUE for descending)
      arrange_desc = logical()
    ),
    class = "arrow_dplyr_query"
  )
}

#' @export
print.arrow_dplyr_query <- function(x, ...) {
  schm <- x$.data$schema
  cols <- get_field_names(x)
  # If cols are expressions, they won't be in the schema and will be "" in cols
  fields <- map_chr(cols, function(name) {
    if (nzchar(name)) {
      schm$GetFieldByName(name)$ToString()
    } else {
      "expr"
    }
  })
  # Strip off the field names as they are in the dataset and add the renamed ones
  fields <- paste(names(cols), sub("^.*?: ", "", fields), sep = ": ", collapse = "\n")
  cat(class(x$.data)[1], " (query)\n", sep = "")
  cat(fields, "\n", sep = "")
  cat("\n")
  if (!isTRUE(x$filtered_rows)) {
    if (query_on_dataset(x)) {
      filter_string <- x$filtered_rows$ToString()
    } else {
      filter_string <- .format_array_expression(x$filtered_rows)
    }
    cat("* Filter: ", filter_string, "\n", sep = "")
  }
  if (length(x$group_by_vars)) {
    cat("* Grouped by ", paste(x$group_by_vars, collapse = ", "), "\n", sep = "")
  }
  if (length(x$arrange_vars)) {
    if (query_on_dataset(x)) {
      arrange_strings <- map_chr(x$arrange_vars, function(x) x$ToString())
    } else {
      arrange_strings <- map_chr(x$arrange_vars, .format_array_expression)
    }
    cat(
      "* Sorted by ",
      paste(
        paste0(
          arrange_strings,
          " [", ifelse(x$arrange_desc, "desc", "asc"), "]"
        ),
        collapse = ", "
      ),
      "\n",
      sep = ""
    )
  }
  cat("See $.data for the source Arrow object\n")
  invisible(x)
}

get_field_names <- function(selected_cols) {
  if (inherits(selected_cols, "arrow_dplyr_query")) {
    selected_cols <- selected_cols$selected_columns
  }
  map_chr(selected_cols, function(x) {
    if (inherits(x, "Expression")) {
      out <- x$field_name
    } else if (inherits(x, "array_expression")) {
      out <- x$args$field_name
    } else {
      out <- NULL
    }
    # If x isn't some kind of field reference, out is NULL,
    # but we always need to return a string
    out %||% ""
  })
}

make_field_refs <- function(field_names, dataset = TRUE) {
  if (dataset) {
    out <- lapply(field_names, Expression$field_ref)
  } else {
    out <- lapply(field_names, function(x) array_expression("array_ref", field_name = x))
  }
  set_names(out, field_names)
}

# These are the names reflecting all select/rename, not what is in Arrow
#' @export
names.arrow_dplyr_query <- function(x) names(x$selected_columns)

#' @export
dim.arrow_dplyr_query <- function(x) {
  cols <- length(names(x))

  if (isTRUE(x$filtered)) {
    rows <- x$.data$num_rows
  } else if (query_on_dataset(x)) {
    warning("Number of rows unknown; returning NA", call. = FALSE)
    # TODO: https://issues.apache.org/jira/browse/ARROW-9697
    rows <- NA_integer_
  } else {
    # Evaluate the filter expression to a BooleanArray and count
    rows <- as.integer(sum(eval_array_expression(x$filtered_rows, x$.data), na.rm = TRUE))
  }
  c(rows, cols)
}

#' @export
as.data.frame.arrow_dplyr_query <- function(x, row.names = NULL, optional = FALSE, ...) {
  collect.arrow_dplyr_query(x, as_data_frame = TRUE, ...)
}

#' @export
head.arrow_dplyr_query <- function(x, n = 6L, ...) {
  if (query_on_dataset(x)) {
    head.Dataset(x, n, ...)
  } else {
    out <- collect.arrow_dplyr_query(x, as_data_frame = FALSE)
    if (inherits(out, "arrow_dplyr_query")) {
      out$.data <- head(out$.data, n)
    } else {
      out <- head(out, n)
    }
    out
  }
}

#' @export
tail.arrow_dplyr_query <- function(x, n = 6L, ...) {
  if (query_on_dataset(x)) {
    tail.Dataset(x, n, ...)
  } else {
    out <- collect.arrow_dplyr_query(x, as_data_frame = FALSE)
    if (inherits(out, "arrow_dplyr_query")) {
      out$.data <- tail(out$.data, n)
    } else {
      out <- tail(out, n)
    }
    out
  }
}

#' @export
`[.arrow_dplyr_query` <- function(x, i, j, ..., drop = FALSE) {
  if (query_on_dataset(x)) {
    `[.Dataset`(x, i, j, ..., drop = FALSE)
  } else {
    stop(
      "[ method not implemented for queries. Call 'collect(x, as_data_frame = FALSE)' first",
      call. = FALSE
    )
  }
}

# The following S3 methods are registered on load if dplyr is present
tbl_vars.arrow_dplyr_query <- function(x) names(x$selected_columns)

select.arrow_dplyr_query <- function(.data, ...) {
  check_select_helpers(enexprs(...))
  column_select(arrow_dplyr_query(.data), !!!enquos(...))
}
select.Dataset <- select.ArrowTabular <- select.arrow_dplyr_query

rename.arrow_dplyr_query <- function(.data, ...) {
  check_select_helpers(enexprs(...))
  column_select(arrow_dplyr_query(.data), !!!enquos(...), .FUN = vars_rename)
}
rename.Dataset <- rename.ArrowTabular <- rename.arrow_dplyr_query

column_select <- function(.data, ..., .FUN = vars_select) {
  # .FUN is either tidyselect::vars_select or tidyselect::vars_rename
  # It operates on the names() of selected_columns, i.e. the column names
  # factoring in any renaming that may already have happened
  out <- .FUN(names(.data), !!!enquos(...))
  # Make sure that the resulting selected columns map back to the original data,
  # as in when there are multiple renaming steps
  .data$selected_columns <- set_names(.data$selected_columns[out], names(out))

  # If we've renamed columns, we need to project that renaming into other
  # query parameters we've collected
  renamed <- out[names(out) != out]
  if (length(renamed)) {
    # Massage group_by
    gbv <- .data$group_by_vars
    renamed_groups <- gbv %in% renamed
    gbv[renamed_groups] <- names(renamed)[match(gbv[renamed_groups], renamed)]
    .data$group_by_vars <- gbv
    # No need to massage filters because those contain references to Arrow objects
  }
  .data
}

relocate.arrow_dplyr_query <- function(.data, ..., .before = NULL, .after = NULL) {
  # The code in this function is adapted from the code in dplyr::relocate.data.frame
  # at https://github.com/tidyverse/dplyr/blob/master/R/relocate.R
  # TODO: revisit this after https://github.com/tidyverse/dplyr/issues/5829
  check_select_helpers(c(enexprs(...), enexpr(.before), enexpr(.after)))

  .data <- arrow_dplyr_query(.data)

  to_move <- eval_select(expr(c(...)), .data$selected_columns)

  .before <- enquo(.before)
  .after <- enquo(.after)
  has_before <- !quo_is_null(.before)
  has_after <- !quo_is_null(.after)

  if (has_before && has_after) {
    abort("Must supply only one of `.before` and `.after`.")
  } else if (has_before) {
    where <- min(unname(eval_select(.before, .data$selected_columns)))
    if (!where %in% to_move) {
      to_move <- c(to_move, where)
    }
  } else if (has_after) {
    where <- max(unname(eval_select(.after, .data$selected_columns)))
    if (!where %in% to_move) {
      to_move <- c(where, to_move)
    }
  } else {
    where <- 1L
    if (!where %in% to_move) {
      to_move <- c(to_move, where)
    }
  }

  lhs <- setdiff(seq2(1, where - 1), to_move)
  rhs <- setdiff(seq2(where + 1, length(.data$selected_columns)), to_move)

  pos <- vec_unique(c(lhs, to_move, rhs))
  new_names <- names(pos)
  .data$selected_columns <- .data$selected_columns[pos]

  if (!is.null(new_names)) {
    names(.data$selected_columns)[new_names != ""] <- new_names[new_names != ""]
  }
  .data
}
relocate.Dataset <- relocate.ArrowTabular <- relocate.arrow_dplyr_query

check_select_helpers <- function(exprs) {
  # Throw an error if unsupported tidyselect selection helpers in `exprs`
  exprs <- lapply(exprs, function(x) if (is_quosure(x)) quo_get_expr(x) else x)
  unsup_select_helpers <- "where"
  funs_in_exprs <- unlist(lapply(exprs, all_funs))
  unsup_funs <- funs_in_exprs[funs_in_exprs %in% unsup_select_helpers]
  if (length(unsup_funs)) {
    stop(
      "Unsupported selection ",
      ngettext(length(unsup_funs), "helper: ", "helpers: "),
      oxford_paste(paste0(unsup_funs, "()"), quote = FALSE),
      call. = FALSE
    )
  }
}

filter.arrow_dplyr_query <- function(.data, ..., .preserve = FALSE) {
  # TODO something with the .preserve argument
  filts <- quos(...)
  if (length(filts) == 0) {
    # Nothing to do
    return(.data)
  }

  .data <- arrow_dplyr_query(.data)
  # tidy-eval the filter expressions inside an Arrow data_mask
  filters <- lapply(filts, arrow_eval, arrow_mask(.data))
  bad_filters <- map_lgl(filters, ~inherits(., "try-error"))
  if (any(bad_filters)) {
    bads <- oxford_paste(map_chr(filts, as_label)[bad_filters], quote = FALSE)
    if (query_on_dataset(.data)) {
      # Abort. We don't want to auto-collect if this is a Dataset because that
      # could blow up, too big.
      stop(
        "Filter expression not supported for Arrow Datasets: ", bads,
        "\nCall collect() first to pull data into R.",
        call. = FALSE
      )
    } else {
      # TODO: only show this in some debug mode?
      warning(
        "Filter expression not implemented in Arrow: ", bads, "; pulling data into R",
        immediate. = TRUE,
        call. = FALSE
      )
      # Set any valid filters first, then collect and then apply the invalid ones in R
      .data <- set_filters(.data, filters[!bad_filters])
      return(dplyr::filter(dplyr::collect(.data), !!!filts[bad_filters]))
    }
  }

  set_filters(.data, filters)
}
filter.Dataset <- filter.ArrowTabular <- filter.arrow_dplyr_query

arrow_eval <- function (expr, mask) {
  # filter(), mutate(), etc. work by evaluating the quoted `exprs` to generate Expressions
  # with references to Arrays (if .data is Table/RecordBatch) or Fields (if
  # .data is a Dataset).

  # This yields an Expression as long as the `exprs` are implemented in Arrow.
  # Otherwise, it returns a try-error
  tryCatch(eval_tidy(expr, mask), error = function(e) {
    # Look for the cases where bad input was given, i.e. this would fail
    # in regular dplyr anyway, and let those raise those as errors;
    # else, for things not supported by Arrow return a "try-error",
    # which we'll handle differently
    msg <- conditionMessage(e)
    patterns <- dplyr_functions$i18ized_error_pattern
    if (is.null(patterns)) {
      patterns <- i18ize_error_messages()
      # Memoize it
      dplyr_functions$i18ized_error_pattern <- patterns
    }
    if (grepl(patterns, msg)) {
      stop(e)
    }
    invisible(structure(msg, class = "try-error", condition = e))
  })
}

i18ize_error_messages <- function() {
  # Figure out what the error messages will be with this LANGUAGE
  # so that we can look for them
  out <- list(
    obj = tryCatch(eval(parse(text = "X_____X")), error = function(e) conditionMessage(e)),
    fun = tryCatch(eval(parse(text = "X_____X()")), error = function(e) conditionMessage(e))
  )
  paste(map(out, ~sub("X_____X", ".*", .)), collapse = "|")
}

# Helper to assemble the functions that go in the NSE data mask
# The only difference between the Dataset and the Table/RecordBatch versions
# is that they use a different wrapping function (FUN) to hold the unevaluated
# expression.
build_function_list <- function(FUN) {
  wrapper <- function(operator) {
    force(operator)
    function(...) FUN(operator, ...)
  }
  all_arrow_funs <- list_compute_functions()

  c(
    # Include mappings from R function name spellings
    lapply(set_names(names(.array_function_map)), wrapper),
    # Plus some special handling where it's not 1:1
    cast = function(x, target_type, safe = TRUE, ...) {
      opts <- cast_options(safe, ...)
      opts$to_type <- as_type(target_type)
      FUN("cast", x, options = opts)
    },
    dictionary_encode = function(x, null_encoding_behavior = c("mask", "encode")) {
      null_encoding_behavior <-
        NullEncodingBehavior[[toupper(match.arg(null_encoding_behavior))]]
      FUN(
        "dictionary_encode",
        x,
        options = list(null_encoding_behavior = null_encoding_behavior)
      )
    },
    # as.factor() is mapped in expression.R
    as.character = function(x) {
      FUN("cast", x, options = cast_options(to_type = string()))
    },
    as.double = function(x) {
      FUN("cast", x, options = cast_options(to_type = float64()))
    },
    as.integer = function(x) {
      FUN(
        "cast",
        x,
        options = cast_options(
          to_type = int32(),
          allow_float_truncate = TRUE,
          allow_decimal_truncate = TRUE
        )
      )
    },
    as.integer64 = function(x) {
      FUN(
        "cast",
        x,
        options = cast_options(
          to_type = int64(),
          allow_float_truncate = TRUE,
          allow_decimal_truncate = TRUE
        )
      )
    },
    as.logical = function(x) {
      FUN("cast", x, options = cast_options(to_type = boolean()))
    },
    as.numeric = function(x) {
      FUN("cast", x, options = cast_options(to_type = float64()))
    },
    nchar = function(x, type = "chars", allowNA = FALSE, keepNA = NA) {
      if (allowNA) {
        stop("allowNA = TRUE not supported for Arrow", call. = FALSE)
      }
      if (is.na(keepNA)) {
        keepNA <- !identical(type, "width")
      }
      if (!keepNA) {
        # TODO: I think there is a fill_null kernel we could use, set null to 2
        stop("keepNA = TRUE not supported for Arrow", call. = FALSE)
      }
      if (identical(type, "bytes")) {
        FUN("binary_length", x)
      } else {
        FUN("utf8_length", x)
      }
    },
    str_trim = function(string, side = c("both", "left", "right")) {
      side <- match.arg(side)
      switch(
        side,
        left = FUN("utf8_ltrim_whitespace", string),
        right = FUN("utf8_rtrim_whitespace", string),
        both = FUN("utf8_trim_whitespace", string)
      )
    },
    grepl = arrow_r_string_match_function(FUN),
    str_detect = arrow_stringr_string_match_function(FUN),
    sub = arrow_r_string_replace_function(FUN, 1L),
    gsub = arrow_r_string_replace_function(FUN, -1L),
    str_replace = arrow_stringr_string_replace_function(FUN, 1L),
    str_replace_all = arrow_stringr_string_replace_function(FUN, -1L),
    between = function(x, left, right) {
      x >= left & x <= right
    },
    # Now also include all available Arrow Compute functions,
    # namespaced as arrow_fun
    set_names(
      lapply(all_arrow_funs, wrapper),
      paste0("arrow_", all_arrow_funs)
    )
  )
}

arrow_r_string_match_function <- function(FUN) {
  function(pattern, x, ignore.case = FALSE, fixed = FALSE) {
    FUN(
      ifelse(fixed && !ignore.case, "match_substring", "match_substring_regex"),
      x,
      options = list(pattern = format_string_pattern(pattern, ignore.case, fixed))
    )
  }
}

arrow_stringr_string_match_function <- function(FUN) {
  function(string, pattern, negate = FALSE) {
    opts <- get_stringr_pattern_options(enexpr(pattern))
    out <- arrow_r_string_match_function(FUN)(
      pattern = opts$pattern,
      x = string,
      ignore.case = opts$ignore_case,
      fixed = opts$fixed
    )
    if (negate) out <- FUN("invert", out)
    out
  }
}

arrow_r_string_replace_function <- function(FUN, max_replacements) {
  function(pattern, replacement, x, ignore.case = FALSE, fixed = FALSE) {
    FUN(
      ifelse(fixed && !ignore.case, "replace_substring", "replace_substring_regex"),
      x,
      options = list(
        pattern = format_string_pattern(pattern, ignore.case, fixed),
        replacement =  format_string_replacement(replacement, ignore.case, fixed),
        max_replacements = max_replacements
      )
    )
  }
}

arrow_stringr_string_replace_function <- function(FUN, max_replacements) {
  function(string, pattern, replacement) {
    opts <- get_stringr_pattern_options(enexpr(pattern))
    arrow_r_string_replace_function(FUN, max_replacements)(
      pattern = opts$pattern,
      replacement = replacement,
      x = string,
      ignore.case = opts$ignore_case,
      fixed = opts$fixed
    )
  }
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

# this function assigns definitions for the stringr pattern modifier functions
# (fixed, regex, etc.) in itself, and uses them to evaluate the quoted
# expression `pattern`
get_stringr_pattern_options <- function(pattern) {
  fixed <- function(pattern, ignore_case = FALSE, ...) {
    check_dots(...)
    list(pattern = pattern, fixed = TRUE, ignore_case = ignore_case)
  }
  regex <- function(pattern, ignore_case = FALSE, ...) {
    check_dots(...)
    list(pattern = pattern, fixed = FALSE, ignore_case = ignore_case)
  }
  coll <- boundary <- function(...) {
    stop(
      "Pattern modifier `",
      match.call()[[1]],
      "()` is not supported in Arrow",
      call. = FALSE
    )
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
      opts <- list(pattern = opts, fixed = TRUE, ignore_case = FALSE)
    }
    opts
  }
  ensure_opts(eval(pattern))
}

# We'll populate these at package load time.
dplyr_functions <- NULL
init_env <- function () {
  dplyr_functions <<- new.env(hash = TRUE)
}
init_env()

# Create a data mask for evaluating a dplyr expression
arrow_mask <- function(.data) {
  if (query_on_dataset(.data)) {
    f_env <- new_environment(dplyr_functions$dataset)
  } else {
    f_env <- new_environment(dplyr_functions$array)
  }

  # Add functions that need to error hard and clear.
  # Some R functions will still try to evaluate on an Expression
  # and return NA with a warning
  fail <- function(...) stop("Not implemented")
  for (f in c("mean")) {
    f_env[[f]] <- fail
  }

  # Add the column references and make the mask
  out <- new_data_mask(
    new_environment(.data$selected_columns, parent = f_env),
    f_env
  )
  # Then insert the data pronoun
  # TODO: figure out what rlang::as_data_pronoun does/why we should use it
  # (because if we do we get `Error: Can't modify the data pronoun` in mutate())
  out$.data <- .data$selected_columns
  out
}

set_filters <- function(.data, expressions) {
  if (length(expressions)) {
    # expressions is a list of Expressions. AND them together and set them on .data
    new_filter <- Reduce("&", expressions)
    if (isTRUE(.data$filtered_rows)) {
      # TRUE is default (i.e. no filter yet), so we don't need to & with it
      .data$filtered_rows <- new_filter
    } else {
      .data$filtered_rows <- .data$filtered_rows & new_filter
    }
  }
  .data
}

collect.arrow_dplyr_query <- function(x, as_data_frame = TRUE, ...) {
  x <- ensure_group_vars(x)
  x <- ensure_arrange_vars(x) # this sets x$temp_columns
  # Pull only the selected rows and cols into R
  if (query_on_dataset(x)) {
    # See dataset.R for Dataset and Scanner(Builder) classes
    tab <- Scanner$create(x)$ToTable()
  } else {
    # This is a Table or RecordBatch

    # Filter and select the data referenced in selected columns
    if (isTRUE(x$filtered_rows)) {
      filter <- TRUE
    } else {
      filter <- eval_array_expression(x$filtered_rows, x$.data)
    }
    # TODO: shortcut if identical(names(x$.data), find_array_refs(c(x$selected_columns, x$temp_columns)))?
    tab <- x$.data[
      filter,
      find_array_refs(c(x$selected_columns, x$temp_columns)),
      keep_na = FALSE
    ]
    # Now evaluate those expressions on the filtered table
    cols <- lapply(c(x$selected_columns, x$temp_columns), eval_array_expression, data = tab)
    if (length(cols) == 0) {
      tab <- tab[, integer(0)]
    } else {
      if (inherits(x$.data, "Table")) {
        tab <- Table$create(!!!cols)
      } else {
        tab <- RecordBatch$create(!!!cols)
      }
    }
  }
  # Arrange rows
  if (length(x$arrange_vars) > 0) {
    tab <- tab[
      tab$SortIndices(names(x$arrange_vars), x$arrange_desc),
      names(x$selected_columns), # this omits x$temp_columns from the result
      drop = FALSE
    ]
  }
  if (as_data_frame) {
    df <- as.data.frame(tab)
    tab$invalidate()
    restore_dplyr_features(df, x)
  } else {
    restore_dplyr_features(tab, x)
  }
}
collect.ArrowTabular <- function(x, as_data_frame = TRUE, ...) {
  if (as_data_frame) {
    as.data.frame(x, ...)
  } else {
    x
  }
}
collect.Dataset <- function(x, ...) dplyr::collect(arrow_dplyr_query(x), ...)

compute.arrow_dplyr_query <- function(x, ...) dplyr::collect(x, as_data_frame = FALSE)
compute.ArrowTabular <- function(x, ...) x
compute.Dataset <- compute.arrow_dplyr_query

ensure_group_vars <- function(x) {
  if (inherits(x, "arrow_dplyr_query")) {
    # Before pulling data from Arrow, make sure all group vars are in the projection
    gv <- set_names(setdiff(dplyr::group_vars(x), names(x)))
    if (length(gv)) {
      # Add them back
      x$selected_columns <- c(
        x$selected_columns,
        make_field_refs(gv, dataset = query_on_dataset(.data))
      )
    }
  }
  x
}

ensure_arrange_vars <- function(x) {
  # The arrange() operation is not performed until later, because:
  # - It must be performed after mutate(), to enable sorting by new columns.
  # - It should be performed after filter() and select(), for efficiency.
  # However, we need users to be able to arrange() by columns and expressions
  # that are *not* returned in the query result. To enable this, we must
  # *temporarily* include these columns and expressions in the projection. We
  # use x$temp_columns to store these. Later, after the arrange() operation has
  # been performed, these are omitted from the result. This differs from the
  # columns in x$group_by_vars which *are* returned in the result.
  x$temp_columns <- x$arrange_vars[!names(x$arrange_vars) %in% names(x$selected_columns)]
  x
}

restore_dplyr_features <- function(df, query) {
  # An arrow_dplyr_query holds some attributes that Arrow doesn't know about
  # After calling collect(), make sure these features are carried over

  grouped <- length(query$group_by_vars) > 0
  renamed <- ncol(df) && !identical(names(df), names(query))
  if (renamed) {
    # In case variables were renamed, apply those names
    names(df) <- names(query)
  }
  if (grouped) {
    # Preserve groupings, if present
    if (is.data.frame(df)) {
      df <- dplyr::grouped_df(
        df,
        dplyr::group_vars(query),
        drop = dplyr::group_by_drop_default(query)
      )
    } else {
      # This is a Table, via compute() or collect(as_data_frame = FALSE)
      df <- arrow_dplyr_query(df)
      df$group_by_vars <- query$group_by_vars
      df$drop_empty_groups <- query$drop_empty_groups
    }
  }
  df
}

pull.arrow_dplyr_query <- function(.data, var = -1) {
  .data <- arrow_dplyr_query(.data)
  var <- vars_pull(names(.data), !!enquo(var))
  .data$selected_columns <- set_names(.data$selected_columns[var], var)
  dplyr::collect(.data)[[1]]
}
pull.Dataset <- pull.ArrowTabular <- pull.arrow_dplyr_query

summarise.arrow_dplyr_query <- function(.data, ...) {
  call <- match.call()
  .data <- arrow_dplyr_query(.data)
  if (query_on_dataset(.data)) {
    not_implemented_for_dataset("summarize()")
  }
  exprs <- quos(...)
  # Only retain the columns we need to do our aggregations
  vars_to_keep <- unique(c(
    unlist(lapply(exprs, all.vars)), # vars referenced in summarise
    dplyr::group_vars(.data)             # vars needed for grouping
  ))
  .data <- dplyr::select(.data, vars_to_keep)
  if (isTRUE(getOption("arrow.summarize", FALSE))) {
    # Try stuff, if successful return()
    out <- try(do_arrow_group_by(.data, ...), silent = TRUE)
    if (inherits(out, "try-error")) {
      return(abandon_ship(call, .data, format(out)))
    } else {
      return(out)
    }
  } else {
    # If unsuccessful or if option not set, do the work in R
    dplyr::summarise(dplyr::collect(.data), ...)
  }
}
summarise.Dataset <- summarise.ArrowTabular <- summarise.arrow_dplyr_query

do_arrow_group_by <- function(.data, ...) {
  exprs <- quos(...)
  mask <- arrow_mask(.data)
  # Add aggregation wrappers to arrow_mask somehow
  # (this is not ideal, would overwrite same-named objects)
  mask$sum <- function(x, na.rm = FALSE) {
    list(
      fun = "sum",
      data = x,
      options = list(na.rm = na.rm)
    )
  }
  results <- list()
  for (i in seq_along(exprs)) {
    # Iterate over the indices and not the names because names may be repeated
    # (which overwrites the previous name)
    new_var <- names(exprs)[i]
    results[[new_var]] <- arrow_eval(exprs[[i]], mask)
    if (inherits(results[[new_var]], "try-error")) {
      msg <- paste('Expression', as_label(exprs[[i]]), 'not supported in Arrow')
      stop(msg, call. = FALSE)
    }
    # Put it in the data mask too?
    #mask[[new_var]] <- mask$.data[[new_var]] <- results[[new_var]]
  }
  # Now, from that, split out the array (expressions) and options
  opts <- lapply(results, function(x) x[c("fun", "options")])
  inputs <- lapply(results, function(x) eval_array_expression(x$data, .data$.data))
  grouping_vars <- lapply(.data$group_by_vars, function(x) eval_array_expression(.data$selected_columns[[x]], .data$.data))
  compute__GroupBy(inputs, grouping_vars, opts)
}

group_by.arrow_dplyr_query <- function(.data,
                                       ...,
                                       .add = FALSE,
                                       add = .add,
                                       .drop = dplyr::group_by_drop_default(.data)) {
  .data <- arrow_dplyr_query(.data)
  # ... can contain expressions (i.e. can add (or rename?) columns)
  # Check for those (they show up as named expressions)
  new_groups <- enquos(...)
  new_groups <- new_groups[nzchar(names(new_groups))]
  if (length(new_groups)) {
    # Add them to the data
    .data <- dplyr::mutate(.data, !!!new_groups)
  }
  if (".add" %in% names(formals(dplyr::group_by))) {
    # dplyr >= 1.0
    gv <- dplyr::group_by_prepare(.data, ..., .add = .add)$group_names
  } else {
    gv <- dplyr::group_by_prepare(.data, ..., add = add)$group_names
  }
  .data$group_by_vars <- gv
  .data$drop_empty_groups <- ifelse(length(gv), .drop, dplyr::group_by_drop_default(.data))
  .data
}
group_by.Dataset <- group_by.ArrowTabular <- group_by.arrow_dplyr_query

groups.arrow_dplyr_query <- function(x) syms(dplyr::group_vars(x))
groups.Dataset <- groups.ArrowTabular <- function(x) NULL

group_vars.arrow_dplyr_query <- function(x) x$group_by_vars
group_vars.Dataset <- group_vars.ArrowTabular <- function(x) NULL

# the logical literal in the two functions below controls the default value of
# the .drop argument to group_by()
group_by_drop_default.arrow_dplyr_query <-
  function(.tbl) .tbl$drop_empty_groups %||% TRUE
group_by_drop_default.Dataset <- group_by_drop_default.ArrowTabular <-
  function(.tbl) TRUE

ungroup.arrow_dplyr_query <- function(x, ...) {
  x$group_by_vars <- character()
  x$drop_empty_groups <- NULL
  x
}
ungroup.Dataset <- ungroup.ArrowTabular <- force

mutate.arrow_dplyr_query <- function(.data,
                                     ...,
                                     .keep = c("all", "used", "unused", "none"),
                                     .before = NULL,
                                     .after = NULL) {
  call <- match.call()
  exprs <- quos(...)

  .keep <- match.arg(.keep)
  .before <- enquo(.before)
  .after <- enquo(.after)

  if (.keep %in% c("all", "unused") && length(exprs) == 0) {
    # Nothing to do
    return(.data)
  }

  .data <- arrow_dplyr_query(.data)

  # Restrict the cases we support for now
  if (length(dplyr::group_vars(.data)) > 0) {
    # mutate() on a grouped dataset does calculations within groups
    # This doesn't matter on scalar ops (arithmetic etc.) but it does
    # for things with aggregations (e.g. subtracting the mean)
    return(abandon_ship(call, .data, 'mutate() on grouped data not supported in Arrow'))
  }

  # Check for unnamed expressions and fix if any
  unnamed <- !nzchar(names(exprs))
  # Deparse and take the first element in case they're long expressions
  names(exprs)[unnamed] <- map_chr(exprs[unnamed], as_label)

  is_dataset <- query_on_dataset(.data)
  mask <- arrow_mask(.data)
  results <- list()
  for (i in seq_along(exprs)) {
    # Iterate over the indices and not the names because names may be repeated
    # (which overwrites the previous name)
    new_var <- names(exprs)[i]
    results[[new_var]] <- arrow_eval(exprs[[i]], mask)
    if (inherits(results[[new_var]], "try-error")) {
      msg <- paste('Expression', as_label(exprs[[i]]), 'not supported in Arrow')
      return(abandon_ship(call, .data, msg))
    } else if (is_dataset &&
               !inherits(results[[new_var]], "Expression") &&
               !is.null(results[[new_var]])) {
      # We need some wrapping to handle literal values
      if (length(results[[new_var]]) != 1) {
        msg <- paste0('In ', new_var, " = ", as_label(exprs[[i]]), ", only values of size one are recycled")
        return(abandon_ship(call, .data, msg))
      }
      results[[new_var]] <- Expression$scalar(results[[new_var]])
    }
    # Put it in the data mask too
    mask[[new_var]] <- mask$.data[[new_var]] <- results[[new_var]]
  }

  old_vars <- names(.data$selected_columns)
  # Note that this is names(exprs) not names(results):
  # if results$new_var is NULL, that means we are supposed to remove it
  new_vars <- names(exprs)

  # Assign the new columns into the .data$selected_columns
  for (new_var in new_vars) {
    .data$selected_columns[[new_var]] <- results[[new_var]]
  }

  # Deduplicate new_vars and remove NULL columns from new_vars
  new_vars <- intersect(new_vars, names(.data$selected_columns))

  # Respect .before and .after
  if (!quo_is_null(.before) || !quo_is_null(.after)) {
    new <- setdiff(new_vars, old_vars)
    .data <- dplyr::relocate(.data, !!new, .before = !!.before, .after = !!.after)
  }

  # Respect .keep
  if (.keep == "none") {
    .data$selected_columns <- .data$selected_columns[new_vars]
  } else if (.keep != "all") {
    # "used" or "unused"
    used_vars <- unlist(lapply(exprs, all.vars), use.names = FALSE)
    if (.keep == "used") {
      .data$selected_columns[setdiff(old_vars, used_vars)] <- NULL
    } else {
      # "unused"
      .data$selected_columns[intersect(old_vars, used_vars)] <- NULL
    }
  }
  # Even if "none", we still keep group vars
  ensure_group_vars(.data)
}
mutate.Dataset <- mutate.ArrowTabular <- mutate.arrow_dplyr_query

transmute.arrow_dplyr_query <- function(.data, ...) dplyr::mutate(.data, ..., .keep = "none")
transmute.Dataset <- transmute.ArrowTabular <- transmute.arrow_dplyr_query

# Helper to handle unsupported dplyr features
# * For Table/RecordBatch, we collect() and then call the dplyr method in R
# * For Dataset, we just error
abandon_ship <- function(call, .data, msg = NULL) {
  dplyr_fun_name <- sub("^(.*?)\\..*", "\\1", as.character(call[[1]]))
  if (query_on_dataset(.data)) {
    if (is.null(msg)) {
      # Default message: function not implemented
      not_implemented_for_dataset(paste0(dplyr_fun_name, "()"))
    } else {
      stop(msg, "\nCall collect() first to pull data into R.", call. = FALSE)
    }
  }

  # else, collect and call dplyr method
  if (!is.null(msg)) {
    warning(msg, "; pulling data into R", immediate. = TRUE, call. = FALSE)
  }
  call$.data <- dplyr::collect(.data)
  call[[1]] <- get(dplyr_fun_name, envir = asNamespace("dplyr"))
  eval.parent(call, 2)
}

arrange.arrow_dplyr_query <- function(.data, ..., .by_group = FALSE) {
  call <- match.call()
  exprs <- quos(...)
  if (.by_group) {
    # when the data is is grouped and .by_group is TRUE, order the result by
    # the grouping columns first
    exprs <- c(quos(!!!dplyr::groups(.data)), exprs)
  }
  if (length(exprs) == 0) {
    # Nothing to do
    return(.data)
  }
  .data <- arrow_dplyr_query(.data)
  # find and remove any dplyr::desc() and tidy-eval
  # the arrange expressions inside an Arrow data_mask
  sorts <- vector("list", length(exprs))
  descs <- logical(0)
  mask <- arrow_mask(.data)
  for (i in seq_along(exprs)) {
    x <- find_and_remove_desc(exprs[[i]])
    exprs[[i]] <- x[["quos"]]
    sorts[[i]] <- arrow_eval(exprs[[i]], mask)
    if (inherits(sorts[[i]], "try-error")) {
      msg <- paste('Expression', as_label(exprs[[i]]), 'not supported in Arrow')
      return(abandon_ship(call, .data, msg))
    }
    names(sorts)[i] <- as_label(exprs[[i]])
    descs[i] <- x[["desc"]]
  }
  .data$arrange_vars <- c(sorts, .data$arrange_vars)
  .data$arrange_desc <- c(descs, .data$arrange_desc)
  .data
}
arrange.Dataset <- arrange.ArrowTabular <- arrange.arrow_dplyr_query

# Helper to handle desc() in arrange()
# * Takes a quosure as input
# * Returns a list with two elements:
#   1. The quosure with any wrapping parentheses and desc() removed
#   2. A logical value indicating whether desc() was found
# * Performs some other validation
find_and_remove_desc <- function(quosure) {
  expr <- quo_get_expr(quosure)
  descending <- FALSE
  if (length(all.vars(expr)) < 1L) {
    stop(
      "Expression in arrange() does not contain any field names: ",
      deparse(expr),
      call. = FALSE
    )
  }
  # Use a while loop to remove any number of nested pairs of enclosing
  # parentheses and any number of nested desc() calls. In the case of multiple
  # nested desc() calls, each one toggles the sort order.
  while (identical(typeof(expr), "language") && is.call(expr)) {
    if (identical(expr[[1]], quote(`(`))) {
      # remove enclosing parentheses
      expr <- expr[[2]]
    } else if (identical(expr[[1]], quote(desc))) {
      # remove desc() and toggle descending
      expr <- expr[[2]]
      descending <- !descending
    } else {
      break
    }
  }
  return(
    list(
      quos = quo_set_expr(quosure, expr),
      desc = descending
    )
  )
}

query_on_dataset <- function(x) inherits(x$.data, "Dataset")

not_implemented_for_dataset <- function(method) {
  stop(
    method, " is not currently implemented for Arrow Datasets. ",
    "Call collect() first to pull data into R.",
    call. = FALSE
  )
}
