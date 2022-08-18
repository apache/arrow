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

    # do not label as "unknown-binding" for several exceptions
    exceptions <- c("c", "factor", "~", "(", "across")
    expr_funs <- setdiff(all_funs(expr), exceptions)

    # if any of the calls in expr are to bindings not yet in the function
    # registry, we mark it as unknown-binding-error and attempt translation
    if (!all(expr_funs %in% names(mask$.top_env))) {
      class(out) <- c("unknown-binding-error", class(out))
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

  # so far we only pass an expression to the data mask builder if we want to
  # try to translate it
  if (!is.null(expr)) {
    # figure out which of the calls in expr are unknown (do not have corresponding
    # bindings)
    unknown_functions <- setdiff(all_funs(expr), names(f_env))

    if (length(unknown_functions) != 0) {
      # get the functions from the expr (quosure) original environment
      functions <- map(unknown_functions, as_function, env = rlang::quo_get_env(expr))

      translated_functions <- map(functions, ~ translate_to_arrow(.x, f_env))

      if (purrr::none(translated_functions, is.null)) {
        purrr::walk2(
          .x = unknown_functions,
          .y = translated_functions,
          .f = ~ register_binding(.x, .y, registry = f_env, update_cache = TRUE)
        )
      }
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

#' Translate a function in a given context (environment with bindings)
#'
#' Translates a function (if possible) with the help of existing bindings in a
#' given environment.
#'
#' @param fn function to be translated
#' @param env environment to translate against
#'
#' @return a translated function
#' @keywords internal
#' @noRd
translate_to_arrow <- function(.fun, .env) {
  # get the args and the body of the unknown binding
  function_formals <- formals(.fun)

  if (is.primitive(.fun)) {
    # exit as the function can't be translated, we can only translate closures
    stop("`", as.character(.fun[[1]]), "` is a primitive and cannot be translated")
  }

  # TODO handle errors. `fn_body()` errors when fn is a primitive, `body()` returns
  # NULL so maybe we can work with that
  function_body <- rlang::fn_body(.fun)

  if (translatable(.fun, .env)) {
    translated_function <- rlang::new_function(
      args = function_formals,
      body = translate_to_arrow_rec(function_body[[2]])
    )
  } else {
    # TODO WIP if the function is not directly translatable, make one more try
    # by attempting to translate the unknown calls
    unknown_function <- setdiff(all_funs(function_body[[2]]), names(.env))

    fn <- as_function(unknown_function, env = caller_env())

    # TODO if all the functions inside the body of the unknown function are
    #  translatable then translate and register them
    #  currently we return a NULL for untranslatable functions
    translated_function <- NULL
  }

  ################### - this is the bit I'm struggling with
  # # handle the non-translatable case first as it is more complex
  #
  # if (!translatable(.fun, .env)) {
  #   # unknown_functions <- setdiff(all_funs(function_body[[2]]), names(.env))
  #
  #   # functions <- map(unknown_functions, as_function, env = caller_env())
  #
  #   unknown_function <- setdiff(all_funs(function_body[[2]]), names(.env))
  #
  #   fn <- as_function(unknown_function, env = caller_env())
  #
  #   fn_body <- rlang::fn_body(fn)
  #
  #   if (translatable(fn, .env)) {
  #     translated_fn <- rlang::new_function(
  #       args = formals(fn),
  #       body = translate_to_arrow_rec(fn_body[[2]]),
  #       env = .env
  #     )
  #   } else {
  #     translated_fn <- NULL
  #   }
  #   register_binding(unknown_function, translated_fn, .env, update_cache = TRUE)
  #
  # }
  #
  # translated_function <- rlang::new_function(
  #   args = function_formals,
  #   body = translate_to_arrow_rec(function_body[[2]])
  # )

  ###################

  translated_function
}

#' Translate the body of a function to an Arrow binding
#'
#' Walks the abstract syntax tree of a function recursively and replaces any
#' function call with a `call_binding()` call (e.g. `1 + foo(bar)` becomes
#' `call_binding("+", 1, call_binding("foo", bar))`)
#'
#' @param x the body of a function
#'
#' @return an expression that can be used as a function body
#' @keywords internal
#' @noRd
translate_to_arrow_rec <- function(x) {
  switch_expr(x,
              constant = ,
              symbol = x,
              call = {
                function_name <- as.character(x[[1]])
                # gymnastics to make sure pkg::fun order is respected (as is in
                # the binding name)
                if ("::" %in% function_name && length(function_name) == 3) {
                  function_name <- paste0(function_name[2], function_name[1], function_name[3])
                }
                children <- as.list(x[-1])
                children_translated <- map(children, translate_to_arrow_rec)
                call2("call_binding", function_name, !!!children_translated)
              }
  )
}

#' Assess if a function is directly translatable to an Arrow binding
#'
#' If all calls in the body of the function have corresponding bindings, then
#' a function is deemed as translatable
#'
#' @inheritParams translate_to_arrow
#'
#' @return logical. `TRUE` or `FALSE`
#'
#' @keywords internal
#' @noRd
translatable <- function(.fun, .env) {
  function_body <- rlang::fn_body(.fun)
  # get all the function calls inside the body of the unknown binding
  # the second element is the actual body of a function (the first one are the
  # curly brackets)
  body_calls <- all_funs(function_body[[2]])

  # we can translate if all calls have matching bindings in env
  if (all(body_calls %in% names(.env))) {
    TRUE
  } else {
    FALSE
  }
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

# switch wrapper to make it easier to step through an expression
switch_expr <- function(x, ...) {
  switch(expr_type(x),
         ...,
         stop("Don't know how to handle type ", typeof(x), call. = FALSE)
  )
}
