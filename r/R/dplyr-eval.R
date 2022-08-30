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
arrow_mask <- function(.data, aggregation = FALSE, exprs = NULL) {
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

  # Assign the schema to the expressions
  map(.data$selected_columns, ~ (.$schema <- .data$.data$schema))

  if (!is.null(exprs)) {
    for (i in seq_along(exprs)) {
      expr <- exprs[[i]]
      register_user_bindings(expr, f_env)
    }
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

# vector of function names that do not have corresponding bindings, but we
# shouldn't try to translate
translation_exceptions <- c(
  # commmon R or dplyr functions we do not have bindings for
  "c",
  "$",
  "factor",
  "~",
  # "(",
  "across",
  ":",
  "[",
  "{",
  "<-",
  "==",
  "-",
  "regex",
  "fixed",
  "list",
  "%>%",
  # all the types functions
  "int8",
  "int16",
  "int32",
  "int64",
  "uint8",
  "uint16",
  "uint32",
  "uint64",
  "float16",
  "halffloat",
  "float32",
  "float",
  "float64",
  "boolean",
  "bool",
  "utf8",
  "large_utf8",
  "binary",
  "large_binary",
  "fixed_size_binary",
  "string",
  "date32",
  "date64",
  "time32",
  "time64",
  "duration",
  "null",
  "timestamp",
  "decimal",
  "decimal128",
  "decimal256"
)

#' Register user defined bindings
#'
#' The function takes a quosure for the evaluation of which a data mask is
#' required. If there are corresponding bindings (in the functions registry) for
#' all the function calls inside the quosure's expression, it does nothing.
#' If there are any unknown ones, it decomposes and assesses whether there are
#' matching bindings. It does this recursively.
#'
#' It sets a copy of the bindings environment (i.e. the function registry) as
#' the parent environment for the user-defined functions' environments.
#'
#' @param quo quosure for which the data mask is being built
#' @param .env bindings environment to register against
#'
#' @return the function does not return anything, it is used for its side-effects
#' @keywords internal
#' @noRd
register_user_bindings <- function(quo, .env) {
  # figure out which of the calls we do not have bindings for or we shouldn't
  # translate
  unknown_functions_chr <- setdiff(
    all_funs(quo),
    union(
      names(.env),
      translation_exceptions
    )
  )

  if (length(unknown_functions_chr) != 0) {
    # get the actual functions from the quosure's original environment or, if
    # the call contains `::`, get the function from the namespace
    unknown_functions <- purrr::map_if(
      .x = unknown_functions_chr,
      .p = ~ !grepl("::", .x),
      .f = ~ tryCatch(as_function(.x, env = rlang::quo_get_env(quo)), error = function(e) NULL),
      .else = ~ asNamespace(sub(":{+}.*?$", "", .x))[[sub("^.*?:{+}", "", .x)]]
    )

    # set the original quosure environment as the parent environment for the
    # functions
    parent.env(.env) <- rlang::quo_get_env(quo)
    for (i in seq_along(unknown_functions)) {
      unknown_fn_name <- unknown_functions_chr[[i]]
      unknown_fn <- unknown_functions[[i]]
      if (!is.null(unknown_fn) && registrable(unknown_fn, .env)) {
        environment(unknown_fn) <- .env
        function_body <- rlang::fn_body(unknown_fn)
        body_calls <-
          setdiff(
            all_funs(function_body),
            translation_exceptions
          )

        # only register a valid binding if none of the calls in body come back
        # as NULL, otherwise register NULL
        if (purrr::none(mget(body_calls, envir = .env), is.null)) {
          register_binding(
            unknown_fn_name,
            unknown_fn,
            registry = .env,
            update_cache = TRUE
          )
        } else {
          register_binding(
            unknown_fn_name,
            NULL,
            registry = .env,
            update_cache = TRUE
          )
        }
      } else {
        # if there are calls we don't have bindings for, attempt to register
        # them first (in case we can recursively translate a function), if we
        # can't then register them as NULL
        unknown_function_body <-
          tryCatch(
            rlang::fn_body(unknown_fn),
            error = function(e) NULL
          )
        if (!is.null(unknown_function_body)) {
          # make it a quosure before attempting to register again with
          # `register_user_bindings`
          new_quo <- rlang::new_quosure(unknown_function_body[[2]], env = quo_get_env(quo))
          register_user_bindings(new_quo, .env)
        } else {
          register_binding(unknown_fn_name, NULL, registry = .env, update_cache = TRUE)
        }
        # once all unknown calls in the stack have been attempted, make a final
        # attempt with the original quosure
        register_user_bindings(quo, .env)
      }
    }
  }
  invisible()
}

#' Assess if a function is registrable
#'
#' A function is considered registrable when all the function calls in its body
#' have corresponding bindings.
#'
#' @param .fun function to assess
#' @param .env bindings environment to assess against
#'
#' @return (logical) `TRUE` or `FALSE`
#' @keywords internal
#' @noRd
registrable <- function(.fun, .env) {
  if (is.primitive(.fun)) return(FALSE)
  function_body <- rlang::fn_body(.fun)
  # get all the function calls inside the body of the unknown binding
  # the second element is the actual body of a function (the first one are the
  # curly brackets)
  body_calls <- setdiff(
    all_funs(function_body),
    translation_exceptions
  )

  # we can translate if all calls have matching bindings in env
  if (all(body_calls %in% names(.env))) {
    TRUE
  } else {
    FALSE
  }
}
