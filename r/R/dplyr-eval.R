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
  # Otherwise, it raises a classed error, either:
  # * arrow_not_supported: the expression is not supported in Arrow; retry with
  #   regular dplyr may work
  # * validation_error: the expression is known to be not valid, so don't
  #   recommend retrying with regular dplyr
  tryCatch(eval_tidy(expr, mask, env = mask), error = function(e) {
    # Inspect why the expression failed, and add the expr as the `call`
    # for better error messages
    msg <- conditionMessage(e)
    arrow_debug <- getOption("arrow.debug", FALSE)
    if (arrow_debug) print(msg)

    # A few cases:
    # 1. Evaluation raised one of our error classes. Add the expr as the call
    #    and re-raise it.
    if (inherits(e, c("validation_error", "arrow_not_supported"))) {
      e$call <- expr
      stop(e)
    }

    # 2. Error is from assert_that: raise as validation_error
    if (inherits(e, "assertError")) {
      validation_error(msg, call = expr)
    }

    # 3. Check to see if this is a standard R error message (not found etc.).
    #    Retry with dplyr won't help.
    if (grepl(get_standard_error_messages(), msg)) {
      # Raise the original error: it's actually helpful here
      validation_error(msg, call = expr)
    }
    # 3b. Check to see if this is from match.arg. Retry with dplyr won't help.
    if (is.language(e$call) && identical(as.character(e$call[[1]]), "match.arg")) {
      # Raise the original error: it's actually helpful here
      validation_error(msg, call = expr)
    }

    # 4. Check for NotImplemented error raised from Arrow C++ code.
    #    Not sure where exactly we may raise this, but if we see it, it means
    #    that something isn't supported in Arrow. Retry in dplyr may help?
    if (grepl("NotImplemented", msg)) {
      arrow_not_supported(.actual_msg = msg, call = expr)
    }


    # 5. Otherwise, we're not sure why this errored: it's not an error we raised
    #    explicitly. We'll assume it's because the function it calls isn't
    #    supported in arrow, and retry with dplyr may help.
    if (arrow_debug) {
      arrow_not_supported(.actual_msg = msg, call = expr)
    } else {
      # Don't show the original error message unless in debug mode because
      # it's probably not helpful: like, if you've passed an Expression to a
      # regular R function that operates on strings, the way it errors would be
      # more confusing than just saying that the expression is not supported
      # in arrow.
      arrow_not_supported("Expression", call = expr)
    }
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

get_standard_error_messages <- function() {
  if (is.null(.cache$i18ized_error_pattern)) {
    # Memoize it
    .cache$i18ized_error_pattern <- i18ize_error_messages()
  }
  .cache$i18ized_error_pattern
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

#' Helpers to raise classed errors
#'
#' `arrow_not_supported()` and `validation_error()` raise classed errors that
#' allow us to distinguish between things that are not supported in Arrow and
#' things that are just invalid input. Additional wrapping in `arrow_eval()`
#' and `try_arrow_dplyr()` provide more context and suggestions.
#' Importantly, if `arrow_not_supported` is raised, then retrying the same code
#' in regular dplyr in R may work. But if `validation_error` is raised, then we
#' shouldn't recommend retrying with regular dplyr because it will fail there
#' too.
#'
#' Use these in function bindings and in the dplyr methods. Inside of function
#' bindings, you don't need to provide the `call` argument, as it will be
#' automatically filled in with the expression that caused the error in
#' `arrow_eval()`. In dplyr methods, you should provide the `call` argument;
#' `rlang::caller_call()` often is correct, but you may need to experiment to
#' find how far up the call stack you need to look.
#'
#' You may provide additional information in the `body` argument, a named
#' character vector. Use `i` for additional information about the error and `>`
#' to indicate potential solutions or workarounds that don't require pulling the
#' data into R. If you have an `arrow_not_supported()` error with a `>`
#' suggestion, when the error is ultimately raised by `try_error_dplyr()`,
#' `Call collect() first to pull data into R` won't be the only suggestion.
#'
#' You can still use `match.arg()` and `assert_that()` for simple input
#' validation inside of the function bindings. `arrow_eval()` will catch their
#' errors and re-raise them as `validation_error`.
#'
#' @param msg The message to show. `arrow_not_supported()` will append
#' "not supported in Arrow" to this message.
#' @param .actual_msg If you don't want to append "not supported in Arrow" to
#' the message, you can provide the full message here.
#' @param ... Additional arguments to pass to `rlang::abort()`. Useful arguments
#' include `call` to provide the call or expression that caused the error, and
#' `body` to provide additional context about the error.
#' @keywords internal
arrow_not_supported <- function(msg,
                                .actual_msg = paste(msg, "not supported in Arrow"),
                                ...) {
  abort(.actual_msg, class = "arrow_not_supported", use_cli_format = TRUE, ...)
}

#' @rdname arrow_not_supported
validation_error <- function(msg, ...) {
  abort(msg, class = "validation_error", use_cli_format = TRUE, ...)
}

# Wrap the contents of an arrow dplyr verb function in a tryCatch block to
# handle arrow_not_supported errors:
# * If it errors because of arrow_not_supported, abandon ship
# * If it's another error, just stop, retry with regular dplyr won't help
try_arrow_dplyr <- function(expr) {
  parent <- caller_env()
  # Make sure that the call is available in the parent environment
  # so that we can use it in abandon_ship, if needed
  # (but don't error if we're in some weird context where we can't get the call,
  # which could happen if you're code-generating or something?)
  try(
    evalq(call <- match.call(), parent),
    silent = !getOption("arrow.debug", FALSE)
  )

  tryCatch(
    eval(expr, parent),
    arrow_not_supported = function(e) abandon_ship(e, parent)
  )
}

# Helper to handle unsupported dplyr features
# * For Table/RecordBatch, we collect() and then call the dplyr method in R
# * For Dataset, we error and recommend collect()
# Requires that `env` contains `.data`
# The Table/RB path also requires `call` to be in `env` (try_arrow_dplyr adds it)
# and that the function being called also exists in the dplyr namespace.
abandon_ship <- function(err, env) {
  .data <- get(".data", envir = env)
  # If there's no call (see comment in try_arrow_dplyr), we can't eval with
  # dplyr even if the data is in memory already
  call <- try(get("call", envir = env), silent = TRUE)
  if (query_on_dataset(.data) || inherits(call, "try-error")) {
    # Add a note suggesting `collect()` to the error message.
    # If there are other suggestions already there (with the > arrow name),
    # collect() isn't the only suggestion, so message differently
    msg <- ifelse(
      ">" %in% names(err$body),
      "Or, call collect() first to pull data into R.",
      "Call collect() first to pull data into R."
    )
    err$body <- c(err$body, ">" = msg)
    stop(err)
  }

  # Else, warn, collect(), and run in regular dplyr
  rlang::warn(
    message = paste0("In ", format_expr(err$call), ": "),
    body = c("i" = conditionMessage(err), ">" = "Pulling data into R")
  )
  call$.data <- dplyr::collect(.data)
  dplyr_fun_name <- sub("^(.*?)\\..*", "\\1", as.character(call[[1]]))
  call[[1]] <- get(dplyr_fun_name, envir = asNamespace("dplyr"))
  eval(call, env)
}

# Create a data mask for evaluating a dplyr expression
arrow_mask <- function(.data) {
  f_env <- new_environment(.cache$functions)

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
