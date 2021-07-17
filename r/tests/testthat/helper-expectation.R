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

expect_as_vector <- function(x, y, ignore_attr = FALSE, ...) {
  expect_fun <- if (ignore_attr) {
    expect_equivalent
  } else {
    expect_equal
  }
  expect_fun(as.vector(x), y, ...)
}

expect_data_frame <- function(x, y, ...) {
  expect_equal(as.data.frame(x), y, ...)
}

expect_r6_class <- function(object, class) {
  expect_s3_class(object, class)
  expect_s3_class(object, "R6")
}

expect_equivalent <- function(object, expected, ...) {
  # HACK: dplyr includes an all.equal.tbl_df method that is causing failures.
  # They look spurious, like:
  # `Can't join on 'b' x 'b' because of incompatible types (tbl_df/tbl/data.frame / tbl_df/tbl/data.frame)`
  if (tibble::is_tibble(object)) {
    class(object) <- "data.frame"
  }
  if (tibble::is_tibble(expected)) {
    class(expected) <- "data.frame"
  }
  testthat::expect_equivalent(object, expected, ...)
}

# expect_equal but for DataTypes, so the error prints better
expect_type_equal <- function(object, expected, ...) {
  if (is.Array(object)) {
    object <- object$type
  }
  if (is.Array(expected)) {
    expected <- expected$type
  }
  expect_equal(object, expected, ..., label = object$ToString(), expected.label = expected$ToString())
}

expect_match_arg_error <- function(object, values=c()) {
  expect_error(object, paste0("'arg' .*", paste(dQuote(values), collapse = ", ")))
}

expect_deprecated <- expect_warning

verify_output <- function(...) {
  if (isTRUE(grepl("conda", R.Version()$platform))) {
    skip("On conda")
  }
  testthat::verify_output(...)
}

#' @param expr A dplyr pipeline with `input` as its start
#' @param tbl A tbl/df as reference, will make RB/Table with
#' @param skip_record_batch string skip message, if should skip RB test
#' @param skip_table string skip message, if should skip Table test
#' @param warning string expected warning from the RecordBatch and Table paths,
#'   passed to `expect_warning()`. Special values:
#'     * `NA` (the default) for ensuring no warning message
#'     * `TRUE` is a special case to mean to check for the
#'      "not supported in Arrow; pulling data into R" message.
#' @param ... additional arguments, passed to `expect_equivalent()`
expect_dplyr_equal <- function(expr,
                               tbl,
                               skip_record_batch = NULL,
                               skip_table = NULL,
                               warning = NA,
                               ...) {
  expr <- rlang::enquo(expr)
  expected <- rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(input = tbl)))

  if (isTRUE(warning)) {
    # Special-case the simple warning:
    # TODO: ARROW-13362 pick one of in or by and use it everywhere
    warning <- "not supported (in|by) Arrow; pulling data into R"
  }

  skip_msg <- NULL

  if (is.null(skip_record_batch)) {
    expect_warning(
      via_batch <- rlang::eval_tidy(
        expr,
        rlang::new_data_mask(rlang::env(input = record_batch(tbl)))
      ),
      warning
    )
    expect_equivalent(via_batch, expected, ...)
  } else {
    skip_msg <- c(skip_msg, skip_record_batch)
  }

  if (is.null(skip_table)) {
    expect_warning(
      via_table <- rlang::eval_tidy(
        expr,
        rlang::new_data_mask(rlang::env(input = Table$create(tbl)))
      ),
      warning
    )
    expect_equivalent(via_table, expected, ...)
  } else {
    skip_msg <- c(skip_msg, skip_table)
  }

  if (!is.null(skip_msg)) {
    skip(paste(skip_msg, collapse = "\n"))
  }
}

expect_dplyr_error <- function(expr, # A dplyr pipeline with `input` as its start
                               tbl,  # A tbl/df as reference, will make RB/Table with
                               ...) {
  # ensure we have supplied tbl
  force(tbl)

  expr <- rlang::enquo(expr)
  msg <- tryCatch(
    rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(input = tbl))),
    error = function (e) {
      msg <- conditionMessage(e)

      # The error here is of the form:
      #
      # Problem with `filter()` input `..1`.
      # x object 'b_var' not found
      # ℹ Input `..1` is `chr == b_var`.
      #
      # but what we really care about is the `x` block
      # so (temporarily) let's pull those blocks out when we find them
      pattern <- i18ize_error_messages()

      if (grepl(pattern, msg)) {
        msg <- sub(paste0("^.*(", pattern, ").*$"), "\\1", msg)
      }
      msg
    }
  )
  # make sure msg is a character object (i.e. there has been an error)
  # If it did not error, we would get a data.frame or whatever
  # This expectation will tell us "dplyr on data.frame errored is not TRUE"
  expect_true(identical(typeof(msg), "character"), label = "dplyr on data.frame errored")

  expect_error(
    rlang::eval_tidy(
      expr,
      rlang::new_data_mask(rlang::env(input = record_batch(tbl)))
    ),
    msg,
    ...
  )
  expect_error(
    rlang::eval_tidy(
      expr,
      rlang::new_data_mask(rlang::env(input = Table$create(tbl)))
    ),
    msg,
    ...
  )
}

expect_vector_equal <- function(expr, # A vectorized R expression containing `input` as its input
                               vec,  # A vector as reference, will make Array/ChunkedArray with
                               skip_array = NULL, # Msg, if should skip Array test
                               skip_chunked_array = NULL, # Msg, if should skip ChunkedArray test
                               ignore_attr = FALSE, # ignore attributes?
                               ...) {
  expr <- rlang::enquo(expr)
  expected <- rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(input = vec)))
  skip_msg <- NULL

  if (is.null(skip_array)) {
    via_array <- rlang::eval_tidy(
      expr,
      rlang::new_data_mask(rlang::env(input = Array$create(vec)))
    )
    expect_as_vector(via_array, expected, ignore_attr, ...)
  } else {
    skip_msg <- c(skip_msg, skip_array)
  }

  if (is.null(skip_chunked_array)) {
    # split input vector into two to exercise ChunkedArray with >1 chunk
    split_vector <- split_vector_as_list(vec)

    via_chunked <- rlang::eval_tidy(
      expr,
      rlang::new_data_mask(rlang::env(input = ChunkedArray$create(split_vector[[1]], split_vector[[2]])))
    )
    expect_as_vector(via_chunked, expected, ignore_attr, ...)
  } else {
    skip_msg <- c(skip_msg, skip_chunked_array)
  }

  if (!is.null(skip_msg)) {
    skip(paste(skip_msg, collapse = "\n"))
  }
}

expect_vector_error <- function(expr, # A vectorized R expression containing `input` as its input
                                vec,  # A vector as reference, will make Array/ChunkedArray with
                                skip_array = NULL, # Msg, if should skip Array test
                                skip_chunked_array = NULL, # Msg, if should skip ChunkedArray test
                                ...) {

  expr <- rlang::enquo(expr)

  msg <- tryCatch(
    rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(input = vec))),
    error = function (e) {
      msg <- conditionMessage(e)

      pattern <- i18ize_error_messages()

      if (grepl(pattern, msg)) {
        msg <- sub(paste0("^.*(", pattern, ").*$"), "\\1", msg)
      }
      msg
    }
  )

  expect_true(identical(typeof(msg), "character"), label = "vector errored")

  skip_msg <- NULL

  if (is.null(skip_array)) {

    expect_error(
      rlang::eval_tidy(
        expr,
        rlang::new_data_mask(rlang::env(input = Array$create(vec)))
      ),
      msg,
      ...
    )
  } else {
    skip_msg <- c(skip_msg, skip_array)
  }

  if (is.null(skip_chunked_array)) {
    # split input vector into two to exercise ChunkedArray with >1 chunk
    split_vector <- split_vector_as_list(vec)

    expect_error(
      rlang::eval_tidy(
        expr,
        rlang::new_data_mask(rlang::env(input = ChunkedArray$create(split_vector[[1]], split_vector[[2]])))
      ),
      msg,
      ...
    )
  } else {
    skip_msg <- c(skip_msg, skip_chunked_array)
  }

  if (!is.null(skip_msg)) {
    skip(paste(skip_msg, collapse = "\n"))
  }
}

split_vector_as_list <- function(vec) {
  vec_split <- length(vec) %/% 2
  vec1 <- vec[seq(from = min(1, length(vec) - 1), to = min(length(vec) - 1, vec_split), by = 1)]
  vec2 <- vec[seq(from = min(length(vec), vec_split + 1), to = length(vec), by = 1)]
  list(vec1, vec2)
}
