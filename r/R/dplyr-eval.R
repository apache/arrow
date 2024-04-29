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

# filter(), mutate(), etc. work by evaluating the quoted `exprs` to generate Expressions
arrow_eval <- function(expr, mask) {
  # Look for R functions referenced in expr that are not in the mask and add
  # them. If they call other functions in the mask, this will let them find them
  # and just work. (If they call things not supported in Arrow, it won't work,
  # but it wouldn't have worked anyway!)
  # Note this is *not* true UDFs.
  add_user_functions_to_mask(expr, mask)

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

add_user_functions_to_mask <- function(expr, mask) {
  # Look for the user's R functions referenced in expr that are not in the mask,
  # see if we can add them to the mask and set their parent env to the mask
  # so that they can reference other functions in the mask.
  if (is_quosure(expr)) {
    # case_when calls arrow_eval() on regular formulas not quosures, which don't
    # have their own environment. But, we've already walked those expressions
    # when calling arrow_eval() on the case_when expression itself, so we don't
    # need to worry about adding them again.
    function_env <- parent.env(parent.env(mask))
    quo_expr <- quo_get_expr(expr)
    funs_in_expr <- all_funs(quo_expr)
    quo_env <- quo_get_env(expr)
    # Enumerate the things we have bindings for, and add anything else that we
    # explicitly want to block from trying to add to the function environment
    known_funcs <- c(ls(function_env, all.names = TRUE), "~", "[", ":")
    unknown <- setdiff(funs_in_expr, known_funcs)
    for (func_name in unknown) {
      if (exists(func_name, quo_env)) {
        user_fun <- get(func_name, quo_env)
        if (!is.null(environment(user_fun)) && !rlang::is_namespace(environment(user_fun))) {
          # Primitives don't have an environment, and we can't trust that
          # functions from packages will work in arrow. (If they could be
          # expressed in arrow, they would be in the mask already.)
          if (getOption("arrow.debug", FALSE)) {
            print(paste("Adding", func_name, "to the function environment"))
          }
          function_env[[func_name]] <- user_fun
          # Also set the enclosing environment to be the function environment.
          # This allows the function to reference other functions in the env.
          # This may have other undesired side effects(?)
          environment(function_env[[func_name]]) <- function_env
        }
      }
    }
  }
  # Don't need to return anything because we assigned into environments,
  # which pass by reference
  invisible()
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

  if (aggregation) {
    # Add the aggregation functions to the environment.
    for (f in names(agg_funcs)) {
      f_env[[f]] <- agg_funcs[[f]]
    }
  } else {
    # Add functions that need to error hard and clear.
    # Some R functions will still try to evaluate on an Expression
    # and return NA with a warning :exploding_head:
    fail <- function(...) stop("Not implemented")
    for (f in c("mean", "sd")) {
      f_env[[f]] <- fail
    }
  }

  # Assign the schema to the expressions
  schema <- .data$.data$schema
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
  # Add the aggregations list to collect any that get pulled out when evaluating
  out$.aggregations <- empty_named_list()
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
