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
    if (grepl("not supported.*Arrow", msg) || getOption("arrow.debug", FALSE)) {
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
arrow_mask <- function(.data, aggregation = FALSE, expr = NULL) {

  f_env <- new_environment(.cache$functions)

  unknown <- NULL
  if (!is.null(expr)) {
    # figure out if the expression is unknown
    # TODO update this to check recursively (for nested functions)
    unknown <- setdiff(all_funs(expr), names(f_env))
  }

  if (length(unknown) != 0) {
    # TODO find a different approach as `call_standardise` is now deprecated
    std_expr <- quo_get_expr(rlang::call_standardise(expr))

    fn <- as_function(unknown)

    # get the args and the body of the unknown binding
    fnctn_formals <- formals(fn)
    fnctn_body <- fn_body(fn)

    # get all the function calls inside the body of the unknown binding
    body_calls <- all_funs(fnctn_body[[2]])

    # we can translate if all calls have bindings
    if (all(body_calls %in% names(f_env))) {
      f_env[[unknown]] <- new_function(
        args = fnctn_formals,
        body = translate_to_arrow(fnctn_body[[2]])
      )
    }
  }

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

  # Assign the schema to the expressions
  map(.data$selected_columns, ~ (.$schema <- .data$.data$schema))

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


translate_to_arrow <- function(x) {
  switch_expr(x,
              constant = ,
              symbol = x,
              call = {
                function_name <- as.character(x[[1]])
                children <- as.list(x[-1])
                children_translated <- map(children, translate_to_arrow)
                call2("call_binding", function_name, !!!children_translated)
              }
  )
}

expr_type <- function(x) {
  if (rlang::is_syntactic_literal(x)) {
    "constant"
  } else if (is.symbol(x)) {
    "symbol"
  } else if (is.call(x)) {
    "call"
  } else if (is.pairlist(x)) {
    "pairlist"
  } else {
    typeof(x)
  }
}

switch_expr <- function(x, ...) {
  switch(expr_type(x),
         ...,
         stop("Don't know how to handle type ", typeof(x), call. = FALSE)
  )
}
