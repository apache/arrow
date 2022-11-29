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

arrow_eval <- function(expr, mask) {
  # filter(), mutate(), etc. work by evaluating the quoted `exprs` to generate Expressions
  # with references to Arrays (if .data is Table/RecordBatch) or Fields (if
  # .data is a Dataset).

  # This yields an Expression as long as the `exprs` are implemented in Arrow.
  # Otherwise, it returns a try-error
  tryCatch(eval_tidy(expr, mask), error = function(e) {
    # Look for the cases where bad input was given, i.e. this would fail
    # in regular dplyr anyway, and let those raise those as errors;
    # else, for things not supported in Arrow return a "try-error",
    # which we'll handle differently
    msg <- conditionMessage(e)
    if (getOption("arrow.debug", FALSE)) print(msg)
    patterns <- .cache$i18ized_error_pattern
    if (is.null(patterns)) {
      patterns <- i18ize_error_messages()
      # Memoize it
      .cache$i18ized_error_pattern <- patterns
    }
    if (grepl(patterns, msg)) {
      stop(e)
    }

    out <- structure(msg, class = "try-error", condition = e)
    if (grepl("not supported.*Arrow|NotImplemented", msg) || getOption("arrow.debug", FALSE)) {
      # One of ours. Mark it so that consumers can handle it differently
      class(out) <- c("arrow-try-error", class(out))
    }
    invisible(out)
  })
}

handle_arrow_not_supported <- function(err, lab) {
  # Look for informative message from the Arrow function version (see above)
  if (inherits(err, "arrow-try-error")) {
    # Include it if found
    paste0("In ", lab, ", ", as.character(err))
  } else {
    # Otherwise be opaque (the original error is probably not useful)
    paste("Expression", lab, "not supported in Arrow")
  }
}

i18ize_error_messages <- function() {
  # Figure out what the error messages will be with this LANGUAGE
  # so that we can look for them
  out <- list(
    obj = tryCatch(eval(parse(text = "X_____X")), error = function(e) conditionMessage(e)),
    fun = tryCatch(eval(parse(text = "X_____X()")), error = function(e) conditionMessage(e))
  )
  paste(map(out, ~ sub("X_____X", ".*", .)), collapse = "|")
}

# Helper to raise a common error
arrow_not_supported <- function(msg) {
  # TODO: raise a classed error?
  stop(paste(msg, "not supported in Arrow"), call. = FALSE)
}

# Create a data mask for evaluating a dplyr expression
arrow_mask <- function(.data, aggregation = FALSE) {
  f_env <- new_environment(.cache$functions)

  # Add functions that need to error hard and clear.
  # Some R functions will still try to evaluate on an Expression
  # and return NA with a warning
  fail <- function(...) stop("Not implemented")
  for (f in c("mean", "sd")) {
    f_env[[f]] <- fail
  }

  if (aggregation) {
    # This should probably be done with an environment inside an environment
    # but a first attempt at that had scoping problems (ARROW-13499)
    for (f in names(agg_funcs)) {
      f_env[[f]] <- agg_funcs[[f]]
    }
  }

  schema <- .data$.data$schema
  # Assign the schema to the expressions
  walk(.data$selected_columns, ~ (.$schema <- schema))

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

format_expr <- function(x) {
  if (is_quosure(x)) {
    x <- quo_get_expr(x)
  }
  out <- deparse(x)
  if (length(out) > 1) {
    # Add ellipses because we are going to truncate
    out[1] <- paste0(out[1], "...")
  }
  head(out, 1)
}
